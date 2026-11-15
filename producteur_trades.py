import asyncio
import json
import os
import signal
import urllib.request
import websockets
from kafka_envoi import KafkaSender
from normalisation import normalize

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "binance.trades")
BINANCE_REST = os.getenv("BINANCE_REST", "https://api.binance.com")
BINANCE_WS = os.getenv("BINANCE_WS", "wss://stream.binance.com:9443")

STREAMS_PER_CONNECTION = 1000
STREAMS_PER_SUBSCRIBE = 200

def active_pairs():
    url = BINANCE_REST + "/api/v3/exchangeInfo?permissions=SPOT&symbolStatus=TRADING"
    with urllib.request.urlopen(url, timeout=15) as response:
        info = json.load(response)
    pairs = {}
    for s in info["symbols"]:
        pairs[s["symbol"]] = {"base": s["baseAsset"], "cotation": s["quoteAsset"]}
    return pairs


async def stream_trades(name, streams, pairs, sender):
    delay = 1
    while True:
        try:
            async with websockets.connect(f"{BINANCE_WS}/stream", max_size=None, ping_interval=20, ping_timeout=20) as ws:
                for i in range(0, len(streams), STREAMS_PER_SUBSCRIBE):
                    batch = streams[i:i + STREAMS_PER_SUBSCRIBE]
                    await ws.send(json.dumps({"method": "SUBSCRIBE", "params": batch, "id": i}))
                    await asyncio.sleep(0.3)
                async for raw in ws:
                    msg = json.loads(raw)
                    if "stream" in msg:
                        data = msg["data"]
                        sender.send(data["s"], normalize(data, pairs[data["s"]]))
                        delay = 1
                    elif msg.get("error"):
                        print(f"[WS {name}] erreur Binance : {msg['error']}")
            print(f"[WS {name}] connexion fermée par Binance")
        except (OSError, asyncio.TimeoutError, websockets.exceptions.WebSocketException) as e:
            print(f"[WS {name}] connexion perdue : {e!r}")

        print(f"[WS {name}] reconnexion dans {delay} s")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)


async def print_stats(sender, interval=10):
    previous = 0
    while True:
        await asyncio.sleep(interval)
        print(f"[stats] {(sender.sent - previous) / interval:.1f} msg/s | total {sender.sent} | "
              f"en attente {len(sender.producer)} | erreurs Kafka {sender.errors}")
        previous = sender.sent


async def run(pairs, sender):
    streams = [f"{p.lower()}@trade" for p in pairs]
    chunks = [streams[i:i + STREAMS_PER_CONNECTION] for i in range(0, len(streams), STREAMS_PER_CONNECTION)]
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, asyncio.current_task().cancel)
    tasks = [stream_trades(f"#{n + 1}", c, pairs, sender) for n, c in enumerate(chunks)]
    await asyncio.gather(print_stats(sender), *tasks)


def main():
    pairs = active_pairs()
    sender = KafkaSender(KAFKA_BOOTSTRAP, TOPIC)
    try:
        asyncio.run(run(pairs, sender))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\nArrêt : envoi des derniers messages")
        sender.close()


if __name__ == "__main__":
    main()
