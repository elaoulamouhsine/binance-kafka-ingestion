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

## Lancer sur AWS (Learner Lab)

### 1. Créer l'instance EC2

Dans le Learner Lab : **Start Lab**, puis la console AWS → EC2 → Lancer une instance :

Exemple de réglages (vous pouvez les adapter) :

| Réglage | Valeur |
|---|---|
| AMI | Ubuntu Server 24.04 LTS |
| Type | `t3.medium`|
| Paire de clés | `vockey` |
| Groupe de sécurité | SSH |
| Stockage | 20 Go gp3 |

Récupérer la clé : page du Learner Lab → AWS Details → Download PEM, puis sur sa machine :

```bash
mkdir -p ~/.ssh 
mv ~/Downloads/labsuser.pem ~/.ssh/ 
chmod 400 ~/.ssh/labsuser.pem
```

### 2. Se connecter et installer Docker

```bash
ssh -i ~/.ssh/labsuser.pem ubuntu@<IP-publique>
```

Sur l'instance :

```bash
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git
sudo usermod -aG docker ubuntu
exit                                    # se reconnecter pour pouvoir utiliser docker sans sudo
```

### 3. Récupérer le code

```bash
ssh -i ~/.ssh/labsuser.pem ubuntu@<IP-publique> # se reconnecter pour pouvoir utiliser docker sans sudo
git clone https://github.com/elaoulamouhsine/binance-kafka-ingestion.git
cd binance-kafka-ingestion
```

### 4. Configurer l'accès à Binance

Les régions du Learner Lab sont aux États-Unis, d'où Binance refuse `api.binance.com` (erreur 451).
On utilise les adresses réservées aux données de marché, dans un fichier `.env` lu automatiquement
par Docker Compose :

```bash
cat > .env <<'EOF'
BINANCE_REST=https://data-api.binance.vision
BINANCE_WS=wss://data-stream.binance.vision
EOF
```

Vérifier l'accès (doit afficher `200`) :

```bash
curl -s -o /dev/null -w "%{http_code}\n" https://data-api.binance.vision/api/v3/ping
```

### 5. Lancer

```bash
docker compose up -d --build
docker compose ps                       # kafka "healthy", producteur et kafka-ui "Up"
docker compose logs -f producteur       # les lignes [stats] doivent afficher des msg/s
```

### 6. Ouvrir l'interface web de Kafka

L'interface tourne sur l'instance : on y accède par un tunnel SSH, sans ouvrir de port.
Dans un nouveau terminal de sa machine (la commande reste ouverte, c'est normal) :

```bash
ssh -i ~/.ssh/labsuser.pem -N -L 8080:localhost:8080 ubuntu@<IP-publique>
```

Puis ouvrir **http://localhost:8080** → Topics → `binance.trades` → Messages.

### 7. Arrêter

```bash
docker compose down -v                  
```

Puis, dans la console EC2 : Arrêter l'instance
ou la Résilier (plus rien n'est facturé).

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
