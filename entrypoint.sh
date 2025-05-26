#!/bin/bash
set -e

echo "Starting..."
python scripts/bootstrap.py

echo "Starting server aiohttp..."
python main.py
