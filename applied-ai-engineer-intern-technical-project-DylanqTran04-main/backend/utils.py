import requests, json
from backend.config import OLLAMA_HOST


def ollama_embed(model: str, text: str):
    r = requests.post(f"{OLLAMA_HOST}/api/embeddings", json={"model": model, "prompt": text})
    r.raise_for_status()
    return r.json()["embedding"]


def ollama_generate(model: str, prompt: str):
    r = requests.post(f"{OLLAMA_HOST}/api/generate", json={"model": model, "prompt": prompt, "stream": False})
    r.raise_for_status()
    return r.json()["response"]
