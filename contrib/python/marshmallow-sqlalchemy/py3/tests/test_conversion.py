import uuid
import datetime as dt
import decimal

import sqlalchemy as sa
import pytest
from sqlalchemy.dialects import postgresql, mysql
from sqlalchemy.orm import column_property
from marshmallow import Schema, fields, validate

from marshmallow_sqlalchemy import (
    fields_for_model,
    ModelConverter,
    property2field,
    column2field,
    field_for,
    ModelConversionError,
)
from marshmallow_sqlalchemy.fields import Related, RelatedList


def contains_validator(field, v_type):
    for v in field.validators:
        if isinstance(v, v_type):
            return v
    return False


class TestModelFieldConversion:
    def test_fields_for_model_types(self, models):
        fields_ = fields_for_model(models.Student, include_fk=True)
        assert type(fields_["id"]) is fields.Int
        assert type(fields_["full_name"]) is fields.Str
        assert type(fields_["dob"]) is fields.Date
        assert type(fields_["current_school_id"]) is fields.Int
        assert type(fields_["date_created"]) is fields.DateTime

    def test_fields_for_model_handles_exclude(self, models):
        fields_ = fields_for_model(models.Student, exclude=("dob",))
        assert type(fields_["id"]) is fields.Int
        assert type(fields_["full_name"]) is fields.Str
        assert fields_["dob"] is None

    def test_fields_for_model_handles_custom_types(self, models):
        fields_ = fields_for_model(models.Course, include_fk=True)
        assert type(fields_["grade"]) is fields.Int
        assert type(fields_["transcription"]) is fields.Str

    def test_fields_for_model_saves_doc(self, models):
        fields_ = fields_for_model(models.Student, include_fk=True)
        assert (
            fields_["date_created"].metadata["description"]
            == "date the student was created"
        )

    def test_length_validator_set(self, models):
        fields_ = fields_for_model(models.Student)
        validator = contains_validator(fields_["full_name"], validate.Length)
        assert validator
        assert validator.max == 255

    def test_none_length_validator_not_set(self, models):
        fields_ = fields_for_model(models.Course)
        assert not contains_validator(fields_["transcription"], validate.Length)

    def test_sets_allow_none_for_nullable_fields(self, models):
        fields_ = fields_for_model(models.Student)
        assert fields_["dob"].allow_none is True

    def test_sets_enum_choices(self, models):
        fields_ = fields_for_model(models.Course)
        validator = contains_validator(fields_["level"], validate.OneOf)
        assert validator
        assert list(validator.choices) == ["Primary", "Secondary"]

    def test_many_to_many_relationship(self, models):
        student_fields = fields_for_model(models.Student, include_relationships=True)
        assert type(student_fields["courses"]) is RelatedList

        course_fields = fields_for_model(models.Course, include_relationships=True)
        assert type(course_fields["students"]) is RelatedList

    def test_many_to_one_relationship(self, models):
        student_fields = fields_for_model(models.Student, include_relationships=True)
        assert type(student_fields["current_school"]) is Related

        school_fields = fields_for_model(models.School, include_relationships=True)
        assert type(school_fields["students"]) is RelatedList

    def test_include_fk(self, models):
        student_fields = fields_for_model(models.Student, include_fk=False)
        assert "current_school_id" not in student_fields

        student_fields2 = fields_for_model(models.Student, include_fk=True)
        assert "current_school_id" in student_fields2

    def test_overridden_with_fk(self, models):
        graded_paper_fields = fields_for_model(models.GradedPaper, include_fk=False)
        assert "id" in graded_paper_fields

    def test_info_overrides(self, models):
        class TestModel(models.Course):
            test = sa.Column(
                sa.Text,
                nullable=True,
                info=dict(
                    marshmallow=dict(
                        validate=[validate.Length(max=1000)], required=True
                    )
                ),
            )

        fields_ = fields_for_model(TestModel)
        field = fields_["test"]
        validator = contains_validator(field, validate.Length)
        assert validator.max == 1000
        assert field.required

    def test_rename_key(self, models):
        class RenameConverter(ModelConverter):
            def _get_field_name(self, prop):
                if prop.key == "name":
                    return "title"
                return prop.key

        converter = RenameConverter()
        fields = converter.fields_for_model(models.Paper)
        assert "title" in fields
        assert "name" not in fields


def make_property(*column_args, **column_kwargs):
    return column_property(sa.Column(*column_args, **column_kwargs))


class TestPropertyFieldConversion:
    @pytest.fixture()
    def converter(self):
        return ModelConverter()

    def test_convert_custom_type_mapping_on_schema(self):
        class MyDateTimeField(fields.DateTime):
            pass

        class MySchema(Schema):
            TYPE_MAPPING = Schema.TYPE_MAPPING.copy()
            TYPE_MAPPING.update({dt.datetime: MyDateTimeField})

        converter = ModelConverter(schema_cls=MySchema)
        prop = make_property(sa.DateTime())
        field = converter.property2field(prop)
        assert type(field) == MyDateTimeField

    @pytest.mark.parametrize(
        ("sa_type", "field_type"),
        (
            (sa.String, fields.Str),
            (sa.Unicode, fields.Str),
            (sa.LargeBinary, fields.Str),
            (sa.Text, fields.Str),
            (sa.Date, fields.Date),
            (sa.DateTime, fields.DateTime),
            (sa.Boolean, fields.Bool),
            (sa.Boolean, fields.Bool),
            (sa.Float, fields.Float),
            (sa.SmallInteger, fields.Int),
            (sa.Interval, fields.TimeDelta),
            (postgresql.UUID, fields.UUID),
            (postgresql.MACADDR, fields.Str),
            (postgresql.INET, fields.Str),
            (postgresql.BIT, fields.Integer),
            (postgresql.OID, fields.Integer),
            (postgresql.CIDR, fields.String),
            (postgresql.DATE, fields.Date),
            (postgresql.TIME, fields.Time),
            (mysql.INTEGER, fields.Integer),
            (mysql.DATETIME, fields.DateTime),
        ),
    )
    def test_convert_types(self, converter, sa_type, field_type):
        prop = make_property(sa_type())
        field = converter.property2field(prop)
        assert type(field) == field_type

    def test_convert_Numeric(self, converter):
        prop = make_property(sa.Numeric(scale=2))
        field = converter.property2field(prop)
        assert type(field) == fields.Decimal
        assert field.places == decimal.Decimal((0, (1,), -2))

    def test_convert_ARRAY_String(self, converter):
        prop = make_property(postgresql.ARRAY(sa.String()))
        field = converter.property2field(prop)
        assert type(field) == fields.List
        inner_field = getattr(field, "inner", getattr(field, "container", None))
        assert type(inner_field) == fields.Str

    def test_convert_ARRAY_Integer(self, converter):
        prop = make_property(postgresql.ARRAY(sa.Integer))
        field = converter.property2field(prop)
        assert type(field) == fields.List
        inner_field = getattr(field, "inner", getattr(field, "container", None))
        assert type(inner_field) == fields.Int

    def test_convert_TSVECTOR(self, converter):
        prop = make_property(postgresql.TSVECTOR)
        with pytest.raises(ModelConversionError):
            converter.property2field(prop)

    def test_convert_default(self, converter):
        prop = make_property(sa.String, default="ack")
        field = converter.property2field(prop)
        assert field.required is False

    def test_convert_server_default(self, converter):
        prop = make_property(sa.String, server_default=sa.text("sysdate"))
        field = converter.property2field(prop)
        assert field.required is False

    def test_convert_autoincrement(self, models, converter):
        prop = models.Course.__mapper__.get_property("id")
        field = converter.property2field(prop)
        assert field.required is False


class TestPropToFieldClass:
    def test_property2field(self):
        prop = make_property(sa.Integer())
        field = property2field(prop, instance=True)

        assert type(field) == fields.Int

        field_cls = property2field(prop, instance=False)
        assert field_cls == fields.Int

    def test_can_pass_extra_kwargs(self):
        prop = make_property(sa.String())
        field = property2field(prop, instance=True, description="just a string")
        assert field.metadata["description"] == "just a string"


class TestColumnToFieldClass:
    def test_column2field(self):
        column = sa.Column(sa.String(255))
        field = column2field(column, instance=True)

        assert type(field) == fields.String

        field_cls = column2field(column, instance=False)
        assert field_cls == fields.String

    def test_can_pass_extra_kwargs(self):
        column = sa.Column(sa.String(255))
        field = column2field(column, instance=True, description="just a string")
        assert field.metadata["description"] == "just a string"

    def test_uuid_column2field(self):
        class UUIDType(sa.types.TypeDecorator):
            python_type = uuid.UUID
            impl = sa.BINARY(16)

        column = sa.Column(UUIDType)
        assert issubclass(column.type.python_type, uuid.UUID)  # Test against test check
        assert hasattr(column.type, "length")  # Test against test check
        assert column.type.length == 16  # Test against test
        field = column2field(column, instance=True)

        uuid_val = uuid.uuid4()
        assert field.deserialize(str(uuid_val)) == uuid_val


class TestFieldFor:
    def test_field_for(self, models, session):
        field = field_for(models.Student, "full_name")
        assert type(field) == fields.Str

        field = field_for(models.Student, "current_school", session=session)
        assert type(field) == Related

        field = field_for(models.Student, "full_name", field_class=fields.Date)
        assert type(field) == fields.Date

    def test_field_for_can_override_validators(self, models, session):
        field = field_for(
            models.Student, "full_name", validate=[validate.Length(max=20)]
        )
        assert len(field.validators) == 1
        assert field.validators[0].max == 20

        field = field_for(models.Student, "full_name", validate=[])
        assert field.validators == []


def _repr_validator_list(validators):
    return sorted([repr(validator) for validator in validators])


@pytest.mark.parametrize(
    "defaults,new,expected",
    [
        ([validate.Length()], [], [validate.Length()]),
        (
            [validate.Range(max=100), validate.Length(min=3)],
            [validate.Range(max=1000)],
            [validate.Range(max=1000), validate.Length(min=3)],
        ),
        (
            [validate.Range(max=1000)],
            [validate.Length(min=3)],
            [validate.Range(max=1000), validate.Length(min=3)],
        ),
        ([], [validate.Length(min=3)], [validate.Length(min=3)]),
    ],
)
def test_merge_validators(defaults, new, expected):
    converter = ModelConverter()
    validators = converter._merge_validators(defaults, new)
    assert _repr_validator_list(validators) == _repr_validator_list(expected)
