from cianparser import CianParser
from cian_pipline.utils import process_data
import asyncio

async def main():
    moscow_parser = CianParser(location="Москва")
    

    cian_parser_rent_long = moscow_parser.get_flats(
        deal_type="rent_long",
        rooms=("all"),
        additional_settings={"start_page": 1, "end_page": 50},
    )
    await process_data(cian_parser_rent_long)

if __name__ == "__main__":
    asyncio.run(main())