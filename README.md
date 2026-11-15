# Ingestion des transactions Binance

Le producteur récupère en continu les transactions de Binance (WebSocket `@trade`) et les publie
dans le topic Kafka `binance.trades` en format JSON.

| Service | Rôle |
|---|---|
| `kafka` | Kafka (1 broker), joignable en local sur `localhost:9092` |
| `kafka-init` | crée le topic `binance.trades`, puis s'arrête |
| `kafka-ui` | interface web : http://localhost:8080 |
| `producteur` | le programme d'ingestion (Binance → Kafka) |

---

## Démarrer

Prérequis : Docker avec Docker Compose, rien d'autre.

```bash
docker compose up -d --build
```

Au premier lancement, Docker télécharge les images et construit celle du producteur.
Le producteur démarre une fois Kafka prêt et le topic créé.

```bash
docker compose ps                       # kafka "healthy", producteur et kafka-ui "Up", kafka-init "Exited (0)"
docker compose logs -f producteur       # suivre le producteur
```

pour s'arrêter :

```bash
docker compose down           # tout arrêter
docker compose down -v        # tout arrêter et effacer les messages 
```


## Le format des messages

Chaque message publié a pour clé la paire, et pour valeur :

```json
{"symbol":"BTCUSDT","base":"BTC","cotation":"USDT","prix":74000.01,"quantite":7e-05,"heure":1794304803120,"id":1001,"heure_reception":1794304803190}
```

| Champ | Type | Origine Binance | Signification |
|---|---|---|---|
| `symbol` | texte | `s` | paire |
| `base` | texte | `exchangeInfo` | monnaie achetée ou vendue (`BTC` pour BTCUSDT) |
| `cotation` | texte | `exchangeInfo` | monnaie du prix (`USDT` pour BTCUSDT, `TRY` pour BTCTRY) |
| `prix` | décimal | `p` | prix de la transaction, en monnaie de `cotation` |
| `quantite` | décimal | `q` | quantité, en monnaie de `base` (montant = prix × quantite, en `cotation`) |
| `heure` | entier | `T` | heure de la transaction|
| `id` | entier | `t` | identifiant de la transaction  |
| `heure_reception` | entier | - | heure de réception par le producteur |
