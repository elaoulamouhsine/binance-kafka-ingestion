import time

def normalize(data: dict, pair: dict) -> dict:
    return {
        "symbol": data["s"],
        "base": pair["base"],
        "cotation": pair["cotation"],
        "prix": float(data["p"]),         
        "quantite": float(data["q"]),
        "heure": data["T"],
        "id": data["t"],
        "heure_reception": int(time.time() * 1000),
    }
