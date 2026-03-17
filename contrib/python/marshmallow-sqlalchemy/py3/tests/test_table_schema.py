import pytest
from marshmallow_sqlalchemy import TableSchema


@pytest.fixture
def school(models, session):
    table = models.School.__table__
    insert = table.insert().values(name="Univ. of Whales")
    with session.connection() as conn:
        conn.execute(insert)
        select = table.select().limit(1)
        return conn.execute(select).fetchone()


class TestTableSchema:
    def test_dump_row(self, models, school):
        class SchoolSchema(TableSchema):
            class Meta:
                table = models.School.__table__

        schema = SchoolSchema()
        data = schema.dump(school)
        assert data == {"name": "Univ. of Whales", "school_id": 1}

    def test_exclude(self, models, school):
        class SchoolSchema(TableSchema):
            class Meta:
                table = models.School.__table__
                exclude = ("name",)

        schema = SchoolSchema()
        data = schema.dump(school)
        assert "name" not in data
