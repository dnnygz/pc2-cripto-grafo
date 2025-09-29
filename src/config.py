COINGECKO_BASE = "https://api.coingecko.com/api/v3"

CATEGORIES = {
    "Artificial Intelligence": ["artificial intelligence", "ai", "ai & big data", "ai-big-data"],
    "Gaming": ["gaming", "gamefi", "play-to-earn"],
    "Real World Assets": ["real-world-assets-rwa", "real world assets", "rwa", "real-world-assets"],
    "Memes": ["meme", "memecoin", "meme token", "memes"],
}

PER_CATEGORY_LIMIT = 250

WALLETS_BY_CHAIN = {
    "ethereum": ["MetaMask", "Trust Wallet"],
    "binance-smart-chain": ["Trust Wallet"],
    "polygon-pos": ["MetaMask", "Trust Wallet"],
    "solana": ["Phantom", "Solflare"],
    "avalanche": ["Core", "MetaMask"],
    "arbitrum-one": ["MetaMask"],
    "optimistic-ethereum": ["MetaMask"],
}

# DETAIL_SLEEP_SECONDS = (1.3, 1.8)
DETAIL_SLEEP_SECONDS = (0.6, 1.0)
# DETAIL_SLEEP_SECONDS = (0.9, 1.3)
MAX_RETRIES = 6

INCLUDE_TICKERS = False