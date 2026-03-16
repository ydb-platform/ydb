import pytest
from pytest_lazy_fixtures import lf

from marshmallow import validate, ValidationError, Schema
import marshmallow
import sqlalchemy as sa

from marshmallow_sqlalchemy import SQLAlchemySchema, SQLAlchemyAutoSchema, auto_field
from marshmallow_sqlalchemy.exceptions import IncorrectSchemaTypeError
from marshmallow_sqlalchemy.fields import Related


# -----------------------------------------------------------------------------


@pytest.fixture
def teacher(models, session):
    school = models.School(id=42, name="Univ. Of Whales")
    teacher_ = models.Teacher(
        id=24, full_name="Teachy McTeachFace", current_school=school
    )
    session.add(teacher_)
    session.flush()
    return teacher_


@pytest.fixture
def school(models, session):
    school = models.School(id=42, name="Univ. Of Whales")
    students = [
        models.Student(id=35, full_name="Bob Smith", current_school=school),
        models.Student(id=53, full_name="John Johnson", current_school=school),
    ]

    session.add_all(students)
    session.flush()
    return school


class EntityMixin:
    id = auto_field(dump_only=True)


# Auto schemas with default options


@pytest.fixture
def sqla_auto_model_schema(models, request):
    class TeacherSchema(SQLAlchemyAutoSchema):
        class Meta:
            model = models.Teacher

        full_name = auto_field(validate=validate.Length(max=20))

    return TeacherSchema()


@pytest.fixture
def sqla_auto_table_schema(models, request):
    class TeacherSchema(SQLAlchemyAutoSchema):
        class Meta:
            table = models.Teacher.__table__

        full_name = auto_field(validate=validate.Length(max=20))

    return TeacherSchema()


# Schemas with relationships


@pytest.fixture
def sqla_schema_with_relationships(models, request):
    class TeacherSchema(EntityMixin, SQLAlchemySchema):
        class Meta:
            model = models.Teacher

        full_name = auto_field(validate=validate.Length(max=20))
        current_school = auto_field()
        substitute = auto_field()

    return TeacherSchema()


@pytest.fixture
def sqla_auto_model_schema_with_relationships(models, request):
    class TeacherSchema(SQLAlchemyAutoSchema):
        class Meta:
            model = models.Teacher
            include_relationships = True

        full_name = auto_field(validate=validate.Length(max=20))

    return TeacherSchema()


# Schemas with foreign keys


@pytest.fixture
def sqla_schema_with_fks(models, request):
    class TeacherSchema(EntityMixin, SQLAlchemySchema):
        class Meta:
            model = models.Teacher

        full_name = auto_field(validate=validate.Length(max=20))
        current_school_id = auto_field()

    return TeacherSchema()


@pytest.fixture
def sqla_auto_model_schema_with_fks(models, request):
    class TeacherSchema(SQLAlchemyAutoSchema):
        class Meta:
            model = models.Teacher
            include_fk = True
            include_relationships = False

        full_name = auto_field(validate=validate.Length(max=20))

    return TeacherSchema()


# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "schema",
    (
        lf("sqla_schema_with_relationships"),
        lf("sqla_auto_model_schema_with_relationships"),
    ),
)
def test_dump_with_relationships(teacher, schema):
    assert schema.dump(teacher) == {
        "id": teacher.id,
        "full_name": teacher.full_name,
        "current_school": 42,
        "substitute": None,
    }


@pytest.mark.parametrize(
    "schema",
    (
        lf("sqla_schema_with_fks"),
        lf("sqla_auto_model_schema_with_fks"),
    ),
)
def test_dump_with_foreign_keys(teacher, schema):
    assert schema.dump(teacher) == {
        "id": teacher.id,
        "full_name": teacher.full_name,
        "current_school_id": 42,
    }


def test_table_schema_dump(teacher, sqla_auto_table_schema):
    assert sqla_auto_table_schema.dump(teacher) == {
        "id": teacher.id,
        "full_name": teacher.full_name,
    }


@pytest.mark.parametrize(
    "schema",
    (
        lf("sqla_schema_with_relationships"),
        lf("sqla_schema_with_fks"),
        lf("sqla_auto_model_schema"),
        lf("sqla_auto_table_schema"),
    ),
)
def test_load(schema):
    assert schema.load({"full_name": "Teachy T"}) == {"full_name": "Teachy T"}


class TestLoadInstancePerSchemaInstance:
    @pytest.fixture
    def schema_no_load_instance(self, models, session):
        class TeacherSchema(SQLAlchemySchema):
            class Meta:
                model = models.Teacher
                sqla_session = session
                # load_instance = False is the default

            full_name = auto_field(validate=validate.Length(max=20))
            current_school = auto_field()
            substitute = auto_field()

        return TeacherSchema

    @pytest.fixture
    def schema_with_load_instance(self, schema_no_load_instance):
        class TeacherSchema(schema_no_load_instance):
            class Meta(schema_no_load_instance.Meta):
                load_instance = True

        return TeacherSchema

    @pytest.fixture
    def auto_schema_no_load_instance(self, models, session):
        class TeacherSchema(SQLAlchemyAutoSchema):
            class Meta:
                model = models.Teacher
                sqla_session = session
                # load_instance = False is the default

        return TeacherSchema

    @pytest.fixture
    def auto_schema_with_load_instance(self, auto_schema_no_load_instance):
        class TeacherSchema(auto_schema_no_load_instance):
            class Meta(auto_schema_no_load_instance.Meta):
                load_instance = True

        return TeacherSchema

    @pytest.mark.parametrize(
        "Schema",
        (
            lf("schema_no_load_instance"),
            lf("schema_with_load_instance"),
            lf("auto_schema_no_load_instance"),
            lf("auto_schema_with_load_instance"),
        ),
    )
    def test_toggle_load_instance_per_schema(self, models, Schema):
        tname = "Teachy T"
        source = {"full_name": tname}

        # No per-instance override
        load_instance_default = Schema()
        result = load_instance_default.load(source)
        default = load_instance_default.opts.load_instance

        default_type = models.Teacher if default else dict
        assert isinstance(result, default_type)

        # Override the default
        override = Schema(load_instance=not default)
        result = override.load(source)

        override_type = dict if default else models.Teacher
        assert isinstance(result, override_type)


@pytest.mark.parametrize(
    "schema",
    (
        lf("sqla_schema_with_relationships"),
        lf("sqla_schema_with_fks"),
        lf("sqla_auto_model_schema"),
        lf("sqla_auto_table_schema"),
    ),
)
def test_load_validation_errors(schema):
    with pytest.raises(ValidationError):
        schema.load({"full_name": "x" * 21})


def test_auto_field_on_plain_schema_raises_error():
    class BadSchema(Schema):
        name = auto_field()

    with pytest.raises(IncorrectSchemaTypeError):
        BadSchema()


def test_cannot_set_both_model_and_table(models):
    with pytest.raises(ValueError, match="Cannot set both"):

        class BadWidgetSchema(SQLAlchemySchema):
            class Meta:
                model = models.Teacher
                table = models.Teacher


def test_passing_model_to_auto_field(models, teacher):
    class TeacherSchema(SQLAlchemySchema):
        current_school_id = auto_field(model=models.Teacher)

    schema = TeacherSchema()
    assert schema.dump(teacher) == {"current_school_id": teacher.current_school_id}


def test_passing_table_to_auto_field(models, teacher):
    class TeacherSchema(SQLAlchemySchema):
        current_school_id = auto_field(table=models.Teacher.__table__)

    schema = TeacherSchema()
    assert schema.dump(teacher) == {"current_school_id": teacher.current_school_id}


# https://github.com/marshmallow-code/marshmallow-sqlalchemy/issues/190
def test_auto_schema_skips_synonyms(models):
    class TeacherSchema(SQLAlchemyAutoSchema):
        class Meta:
            model = models.Teacher
            include_fk = True

    schema = TeacherSchema()
    assert "current_school_id" in schema.fields
    assert "curr_school_id" not in schema.fields


def test_auto_field_works_with_synonym(models):
    class TeacherSchema(SQLAlchemyAutoSchema):
        class Meta:
            model = models.Teacher
            include_fk = True

        curr_school_id = auto_field()

    schema = TeacherSchema()
    assert "current_school_id" in schema.fields
    assert "curr_school_id" in schema.fields


# Regresion test https://github.com/marshmallow-code/marshmallow-sqlalchemy/issues/306
def test_auto_field_works_with_ordered_flag(models):
    class StudentSchema(SQLAlchemyAutoSchema):
        class Meta:
            model = models.Student
            ordered = True

        full_name = auto_field()

    schema = StudentSchema()
    # Declared fields precede auto-generated fields
    assert tuple(schema.fields.keys()) == ("full_name", "id", "dob", "date_created")


class TestAliasing:
    @pytest.fixture
    def aliased_schema(self, models):
        class TeacherSchema(SQLAlchemySchema):
            class Meta:
                model = models.Teacher

            # Generate field from "full_name", pull from "full_name" attribute, output to "name"
            name = auto_field("full_name")

        return TeacherSchema()

    @pytest.fixture
    def aliased_auto_schema(self, models):
        class TeacherSchema(SQLAlchemyAutoSchema):
            class Meta:
                model = models.Teacher
                exclude = ("full_name",)

            # Generate field from "full_name", pull from "full_name" attribute, output to "name"
            name = auto_field("full_name")

        return TeacherSchema()

    @pytest.fixture
    def aliased_attribute_schema(self, models):
        class TeacherSchema(SQLAlchemySchema):
            class Meta:
                model = models.Teacher

            # Generate field from "full_name", pull from "fname" attribute, output to "name"
            name = auto_field("full_name", attribute="fname")

        return TeacherSchema()

    @pytest.mark.parametrize(
        "schema",
        (
            lf("aliased_schema"),
            lf("aliased_auto_schema"),
        ),
    )
    def test_passing_column_name(self, schema, teacher):
        assert schema.fields["name"].attribute == "full_name"
        dumped = schema.dump(teacher)
        assert dumped["name"] == teacher.full_name

    def test_passing_column_name_and_attribute(self, teacher, aliased_attribute_schema):
        assert aliased_attribute_schema.fields["name"].attribute == "fname"
        dumped = aliased_attribute_schema.dump(teacher)
        assert dumped["name"] == teacher.fname


class TestModelInstanceDeserialization:
    @pytest.fixture
    def sqla_schema_class(self, models, session):
        class TeacherSchema(SQLAlchemySchema):
            class Meta:
                model = models.Teacher
                load_instance = True
                sqla_session = session

            full_name = auto_field(validate=validate.Length(max=20))
            current_school = auto_field()
            substitute = auto_field()

        return TeacherSchema

    @pytest.fixture
    def sqla_auto_schema_class(self, models, session):
        class TeacherSchema(SQLAlchemyAutoSchema):
            class Meta:
                model = models.Teacher
                include_relationships = True
                load_instance = True
                sqla_session = session

        return TeacherSchema

    @pytest.mark.parametrize(
        "SchemaClass",
        (
            lf("sqla_schema_class"),
            lf("sqla_auto_schema_class"),
        ),
    )
    def test_load(self, teacher, SchemaClass, models):
        schema = SchemaClass(unknown=marshmallow.INCLUDE)
        dump_data = schema.dump(teacher)
        load_data = schema.load(dump_data)

        assert isinstance(load_data, models.Teacher)

    def test_load_transient(self, models, teacher):
        class TeacherSchema(SQLAlchemyAutoSchema):
            class Meta:
                model = models.Teacher
                load_instance = True
                transient = True

        schema = TeacherSchema()
        dump_data = schema.dump(teacher)
        load_data = schema.load(dump_data)
        assert isinstance(load_data, models.Teacher)
        state = sa.inspect(load_data)
        assert state.transient


def test_related_when_model_attribute_name_distinct_from_column_name(
    models,
    session,
    teacher,
):
    class TeacherSchema(SQLAlchemyAutoSchema):
        class Meta:
            model = models.Teacher
            load_instance = True
            sqla_session = session

        current_school = Related(["id", "name"])

    dump_data = TeacherSchema().dump(teacher)
    assert "school_id" not in dump_data["current_school"]
    assert dump_data["current_school"]["id"] == teacher.current_school.id
    new_teacher = TeacherSchema().load(dump_data, transient=True)
    assert new_teacher.current_school.id == teacher.current_school.id
    assert TeacherSchema().load(dump_data) is teacher


# https://github.com/marshmallow-code/marshmallow-sqlalchemy/issues/338
def test_auto_field_works_with_assoc_proxy(models):
    class StudentSchema(SQLAlchemySchema):
        class Meta:
            model = models.Student

        possible_teachers = auto_field()

    schema = StudentSchema()
    assert "possible_teachers" in schema.fields


def test_dump_and_load_with_assoc_proxy_multiplicity(models, session, school):
    class SchoolSchema(SQLAlchemySchema):
        class Meta:
            model = models.School
            load_instance = True
            sqla_session = session

        student_ids = auto_field()

    schema = SchoolSchema()
    assert "student_ids" in schema.fields
    dump_data = schema.dump(school)
    assert "student_ids" in dump_data
    assert dump_data["student_ids"] == list(school.student_ids)
    new_school = schema.load(dump_data, transient=True)
    assert list(new_school.student_ids) == list(school.student_ids)


def test_dump_and_load_with_assoc_proxy_multiplicity_dump_only_kwargs(
    models, session, school
):
    class SchoolSchema(SQLAlchemySchema):
        class Meta:
            model = models.School
            load_instance = True
            sqla_session = session

        student_ids = auto_field(dump_only=True, data_key="student_identifiers")

    schema = SchoolSchema()
    assert "student_ids" in schema.fields
    assert schema.fields["student_ids"] not in schema.load_fields.values()
    assert schema.fields["student_ids"] in schema.dump_fields.values()

    dump_data = schema.dump(school)
    assert "student_ids" not in dump_data
    assert "student_identifiers" in dump_data
    assert dump_data["student_identifiers"] == list(school.student_ids)

    with pytest.raises(ValidationError):
        schema.load(dump_data, transient=True)


def test_dump_and_load_with_assoc_proxy_multiplicity_load_only_only_kwargs(
    models, session, school
):
    class SchoolSchema(SQLAlchemySchema):
        class Meta:
            model = models.School
            load_instance = True
            sqla_session = session

        student_ids = auto_field(load_only=True, data_key="student_identifiers")

    schema = SchoolSchema()

    assert "student_ids" in schema.fields
    assert schema.fields["student_ids"] not in schema.dump_fields.values()
    assert schema.fields["student_ids"] in schema.load_fields.values()

    dump_data = schema.dump(school)
    assert "student_identifers" not in dump_data

    new_school = schema.load(
        {"student_identifiers": list(school.student_ids)}, transient=True
    )
    assert list(new_school.student_ids) == list(school.student_ids)
