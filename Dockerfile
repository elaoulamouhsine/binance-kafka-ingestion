FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Dépendances et code
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY normalisation.py kafka_envoi.py producteur_trades.py ./

# Création d'un user au lieu de root
RUN useradd --create-home ingestion
USER ingestion

CMD ["python", "producteur_trades.py"]
