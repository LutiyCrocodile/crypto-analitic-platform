from typing import List
import re
from cian_pipline.const import field_name
from repository.database import connection
from sqlalchemy import text

async def process_data(cian_parser_data: List[dict]) -> None:
    for response_json in cian_parser_data:
        insert_real_estate_values = {
            "rooms_count": response_json.get("rooms_count"),
            "floor": response_json.get("floor"),
            "floors_count": response_json.get("floors_count"),
            "total_meters": response_json.get("total_meters"),
            "price_per_month": response_json.get("price_per_month"),
            "commissions": response_json.get("commissions"),
            "url": response_json.get("url"),
            "cian_id": get_cian_id(response_json.get("url")),
        }

        for key, value in field_name.items():
            async with connection() as session:
                fetch_query = text(f"SELECT {value} FROM real_estate.{key} WHERE {key} = :value")
                result = await session.execute(fetch_query, {"value": str(response_json.get(key))})
                existing_record = result.fetchone()
                
                if not existing_record:  
                    insert_query = text(f"INSERT INTO real_estate.{key} ({key}) VALUES (:value) RETURNING {value}")
                    inserted_value = await session.execute(insert_query, {"value": str(response_json.get(key))})
                    await session.commit()  
                    insert_real_estate_values[value] = inserted_value.scalar()
                else:  
                    insert_real_estate_values[value] = existing_record[0]  

        async with connection() as session:
            try:
                insert_query = text("""
                    INSERT INTO real_estate.apart 
                    (rooms_count, floor, floors_count, total_meters, price_per_month, commissions, url, cian_id, 
                    author_id, author_type_id, location_id, deal_type_id, accommodation_type_id, district_id, 
                    house_number_id, underground_id, street_id) 
                    VALUES 
                    (:rooms_count, :floor, :floors_count, :total_meters, :price_per_month, :commissions, :url, :cian_id, 
                    :author_id, :author_type_id, :location_id, :deal_type_id, :accommodation_type_id, :district_id, 
                    :house_number_id, :underground_id, :street_id)
                    ON CONFLICT (cian_id) DO UPDATE 
                    SET rooms_count = EXCLUDED.rooms_count,
                        floor = EXCLUDED.floor,
                        floors_count = EXCLUDED.floors_count,
                        total_meters = EXCLUDED.total_meters,
                        price_per_month = EXCLUDED.price_per_month,
                        commissions = EXCLUDED.commissions,
                        url = EXCLUDED.url,
                        author_id = EXCLUDED.author_id,
                        author_type_id = EXCLUDED.author_type_id,
                        location_id = EXCLUDED.location_id,
                        deal_type_id = EXCLUDED.deal_type_id,
                        accommodation_type_id = EXCLUDED.accommodation_type_id,
                        district_id = EXCLUDED.district_id,
                        house_number_id = EXCLUDED.house_number_id,
                        underground_id = EXCLUDED.underground_id,
                        street_id = EXCLUDED.street_id
                """)
                await session.execute(insert_query, insert_real_estate_values)
                await session.commit()  
            except Exception as e: 
                print(f"Error inserting data: {e}")

def get_cian_id(url: str) -> int:
    match = re.search(r"/flat/(\d+)/", url)
    if match:
        return int(match.group(1))
    return None