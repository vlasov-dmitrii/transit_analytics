#!/bin/bash

set -e

echo "=========================================="
echo "BART Analytics - PySpark Upgrade"
echo "=========================================="
echo ""

# Stop containers
echo "[1/6] Stopping containers..."
docker-compose down

# Backup current files
echo "[2/6] Backing up current files..."
mkdir -p backups
cp requirements.txt backups/requirements.txt.bak
cp Dockerfile backups/Dockerfile.bak
cp data_engineering/loading/load_bart.py backups/load_bart.py.bak

# Update files
echo "[3/6] Updating configuration files..."
cp requirements_pyspark.txt requirements.txt
cp Dockerfile_pyspark Dockerfile
cp load_bart_pyspark.py data_engineering/loading/load_bart.py

# Rebuild containers
echo "[4/6] Rebuilding Docker images..."
docker-compose build --no-cache

# Start services
echo "[5/6] Starting services..."
docker-compose up -d

# Wait for database
echo "[6/6] Waiting for database initialization..."
sleep 10

echo ""
echo "=========================================="
echo "PySpark upgrade complete"
echo "=========================================="
echo ""
echo "Services:"
echo "  Dashboard: http://localhost:8501"
echo "  Database: localhost:5432"
echo ""
echo "Monitor logs:"
echo "  docker-compose logs -f ingestion"
echo "  docker-compose logs -f dashboard"
echo ""
