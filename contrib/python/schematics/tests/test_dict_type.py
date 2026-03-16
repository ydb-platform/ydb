from schematics.models import Model
from schematics.types import IntType, StringType
from schematics.types.serializable import serializable
from schematics.types.compound import ModelType, DictType

try:
    long
except NameError:
    long = int

def test_basic_type():
    class PlayerInfo(Model):
        categories = DictType(StringType)

    info = PlayerInfo(dict(categories={
        "math": "math",
        "batman": "batman",
    }))

    assert info.categories["math"] == "math"

    d = info.serialize()
    assert d == {
        "categories": {
            "math": "math",
            "batman": "batman",
        }
    }


def test_dict_type_with_model_type():
    class CategoryStats(Model):
        category_slug = StringType()
        total_wins = IntType()

    class PlayerInfo(Model):
        categories = DictType(ModelType(CategoryStats))
        # TODO: Maybe it would be cleaner to have
        #       DictType(CategoryStats) and implicitly convert to ModelType(CategoryStats)

    info = PlayerInfo(dict(categories={
        "math": {
            "category_slug": "math",
            "total_wins": 1
        },
        "batman": {
            "category_slug": "batman",
            "total_wins": 3
        }
    }))

    math_stats = CategoryStats({"category_slug": "math", "total_wins": 1})
    assert info.categories["math"] == math_stats

    d = info.serialize()
    assert d == {
        "categories": {
            "math": {
                "category_slug": "math",
                "total_wins": 1
            },
            "batman": {
                "category_slug": "batman",
                "total_wins": 3
            }
        }
    }


def test_dict_type_with_model_type_init_with_instance():
    class ExperienceLevel(Model):
        level = IntType()

    class CategoryStats(Model):
        category_slug = StringType()
        total_wins = IntType()

        @serializable(type=ModelType(ExperienceLevel))
        def xp_level(self):
            return ExperienceLevel(dict(level=self.total_wins))

    class PlayerInfo(Model):
        id = IntType()
        categories = DictType(ModelType(CategoryStats))
        # TODO: Maybe it would be cleaner to have
        #       DictType(CategoryStats) and implicitly convert to ModelType(CategoryStats)

    math_stats = CategoryStats({
        "category_slug": "math",
        "total_wins": 1
    })

    info = PlayerInfo(dict(id=1, categories={
        "math": math_stats,
    }))

    assert info.categories["math"] == math_stats

    d = info.serialize()

    assert d == {
        "id": 1,
        "categories": {
            "math": {
                "category_slug": "math",
                "total_wins": 1,
                "xp_level": {
                    "level": 1
                }
            },
        }
    }


def test_with_empty():
    class CategoryStatsInfo(Model):
        slug = StringType()

    class PlayerInfo(Model):
        categories = DictType(
            ModelType(CategoryStatsInfo),
            default=lambda: {},
            serialize_when_none=True,
        )

    info = PlayerInfo()

    assert info.categories == {}

    d = info.serialize()
    assert d == {
        "categories": {},
    }


def test_key_type():
    def player_id(value):
        return long(value)

    class CategoryStatsInfo(Model):
        slug = StringType()

    class PlayerInfo(Model):
        categories = DictType(ModelType(CategoryStatsInfo), coerce_key=player_id)

    stats = CategoryStatsInfo({
        "slug": "math",
    })

    info = PlayerInfo({
        "categories": {
            1: {"slug": "math"}
        },
    })

    assert info.categories == {1: stats}

    d = info.serialize()
    assert d == {
        "categories": {1: {"slug": "math"}}
    }
