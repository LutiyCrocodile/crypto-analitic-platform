from sqlalchemy import text
from cianparser import CianParser
from dags.repository.database import connection


def get_pars(deal_type: str, rooms: int, additional_settings: dict) -> list[dict]:
    moscow_parser = CianParser(location="Москва")
    data = moscow_parser.get_flats(
        deal_type=deal_type,
        rooms=rooms,
        with_saving_csv=False,
        additional_settings=additional_settings,
    )
    return data


async def pars_filtred():
    rooms = (1, 2, 3, 4, 5, "studio")
    returning_flats = []

    # какие данные берем из бд
    selected_values = {
        "deal_type": "deal_type",
        "underground": "underground",
    }

    # конект с бд
    for key, value in selected_values.items():
        if key == "deal_type":
            async with connection() as session:
                fetch_query = text(
                    f"SELECT {value} FROM real_estate.{key}",
                )
                result = await session.execute(fetch_query)
                deal_type_list = result.scalars().all()
        elif key == "underground":
            async with connection() as session:
                fetch_query = text(
                    f"SELECT {value} FROM real_estate.{key}",
                )
                result = await session.execute(fetch_query)
                underground_list = result.scalars().all()

    # проходимся по данным из бд
    for room in rooms:
        for deal_type in deal_type_list:
            if deal_type == "rent":
                for underground in underground_list:
                    additional_settings = {
                        "start_page": 1,
                        "end_page": 10,
                        "metro": "Московский",
                        "metro_station": underground,
                    }
                    returning_flats.append(
                        get_pars(
                            rooms=room,
                            deal_type="rent_long",
                            additional_settings=additional_settings,
                        )
                    )
            else:
                for underground in underground_list:
                    additional_settings = {
                        "start_page": 1,
                        "end_page": 1,
                        "metro": "Московский",
                        "metro_station": underground,
                    }
                    returning_flats.append(
                        get_pars(
                            rooms=room,
                            deal_type=deal_type,
                            additional_settings=additional_settings,
                        )
                    )
    # вывод
    return returning_flats
