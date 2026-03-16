import unittest
from dataclasses import dataclass, field
from typing import List, Optional

import marshmallow.validate

import marshmallow_dataclass


@dataclass
class Building:
    # field metadata is used to instantiate the marshmallow field
    height: float = field(metadata={"validate": marshmallow.validate.Range(min=0)})
    name: str = field(default="anonymous")


@dataclass
class City:
    name: Optional[str]
    buildings: List[Building] = field(default_factory=list)


CitySchema = marshmallow_dataclass.class_schema(City)()


class TestCityBuildingExample(unittest.TestCase):
    def test_load(self):
        city = CitySchema.load(
            {"name": "Paris", "buildings": [{"name": "Eiffel Tower", "height": 324}]}
        )
        self.assertEqual(
            city,
            City(name="Paris", buildings=[Building(height=324.0, name="Eiffel Tower")]),
        )

    def test_back_and_forth(self):
        original_dict = {
            "name": "Paris",
            "buildings": [{"name": "Eiffel Tower", "height": 324}],
        }
        city_dict = CitySchema.dump(CitySchema.load(original_dict))
        self.assertEqual(city_dict, original_dict)


if __name__ == "__main__":
    unittest.main()
