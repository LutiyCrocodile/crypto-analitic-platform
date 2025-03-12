import asyncpg
import aiohttp
import asyncio
from core.config import settings
async def fetch_crypto_list():
    url = "https://api.coingecko.com/api/v3/coins/list"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Error fetching crypto list: {response.status}")
                return []

async def insert_crypto_data(connection, crypto_list):
    values = [
        (crypto["id"].upper(), crypto.get("symbol", "undefind").upper(), crypto.get("name", "undefind"))
        for crypto in crypto_list
    ]
    await connection.executemany(
        """
        INSERT INTO crypto.crypto (crypto, crypto_symbol, crypto_full_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (crypto) 
        DO UPDATE SET
            crypto_symbol = EXCLUDED.crypto_symbol,
            crypto_name = EXCLUDED.crypto_full_name;
        """,
        values
    )

async def main():
    # Подключение к базе данных
    conn = await asyncpg.connect(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_PASSWORD,
        host=settings.DB_HOST,
        port=settings.DB_PORT
    )

    crypto_list = await fetch_crypto_list()

    await insert_crypto_data(conn, crypto_list)

    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())