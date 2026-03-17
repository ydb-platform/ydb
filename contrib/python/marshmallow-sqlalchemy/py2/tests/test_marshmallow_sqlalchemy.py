# -*- coding: utf-8 -*-
from __future__ import absolute_import
import uuid
import datetime as dt
import decimal
from collections import OrderedDict

import sqlalchemy as sa
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref, column_property
from sqlalchemy.dialects import postgresql

import marshmallow
from marshmallow import Schema, fields, validate, post_load, ValidationError

import pytest
from marshmallow_sqlalchemy import (
    fields_for_model,
    TableSchema,
    ModelSchema,
    ModelConverter,
    property2field,
    column2field,
    field_for,
    ModelConversionError,
)
from marshmallow_sqlalchemy.fields import Related, RelatedList, Nested

MARSHMALLOW_VERSION_INFO = tuple(
    [int(part) for part in marshmallow.__version__.split(".") if part.isdigit()]
)


def unpack(return_value):
    return return_value.data if MARSHMALLOW_VERSION_INFO[0] < 3 else return_value


def contains_validator(field, v_type):
    for v in field.validators:
        if isinstance(v, v_type):
            return v
    return False


class AnotherInteger(sa.Integer):
    """Use me to test if MRO works like we want"""

    pass


class AnotherText(sa.types.TypeDecorator):
    """Use me to test if MRO and `impl` virtual type works like we want"""

    impl = sa.UnicodeText


@pytest.fixture()
def Base():
    return declarative_base()


@pytest.fixture()
def engine():
    return sa.create_engine("sqlite:///:memory:", echo=False)


@pytest.fixture()
def session(Base, models, engine):
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(bind=engine)
    return Session()


@pytest.fixture()
def models(Base):

    # models adapted from https://github.com/wtforms/wtforms-sqlalchemy/blob/master/tests/tests.py
    student_course = sa.Table(
        "student_course",
        Base.metadata,
        sa.Column("student_id", sa.Integer, sa.ForeignKey("student.id")),
        sa.Column("course_id", sa.Integer, sa.ForeignKey("course.id")),
    )

    class Course(Base):
        __tablename__ = "course"
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String(255), nullable=False)
        # These are for better model form testing
        cost = sa.Column(sa.Numeric(5, 2), nullable=False)
        description = sa.Column(
            sa.Text,
            nullable=True,
            info=dict(
                marshmallow=dict(validate=[validate.Length(max=1000)], required=True)
            ),
        )
        level = sa.Column(sa.Enum("Primary", "Secondary"))
        has_prereqs = sa.Column(sa.Boolean, nullable=False)
        started = sa.Column(sa.DateTime, nullable=False)
        grade = sa.Column(AnotherInteger, nullable=False)
        transcription = sa.Column(AnotherText, nullable=False)

        @property
        def url(self):
            return "/courses/{}".format(self.id)

    class School(Base):
        __tablename__ = "school"
        id = sa.Column("school_id", sa.Integer, primary_key=True)
        name = sa.Column(sa.String(255), nullable=False)

        @property
        def url(self):
            return "/schools/{}".format(self.id)

    class Student(Base):
        __tablename__ = "student"
        id = sa.Column(sa.Integer, primary_key=True)
        full_name = sa.Column(sa.String(255), nullable=False, unique=True)
        dob = sa.Column(sa.Date(), nullable=True)
        date_created = sa.Column(
            sa.DateTime, default=dt.datetime.utcnow, doc="date the student was created"
        )

        current_school_id = sa.Column(
            sa.Integer, sa.ForeignKey(School.id), nullable=False
        )
        current_school = relationship(School, backref=backref("students"))
        possible_teachers = association_proxy("current_school", "teachers")

        courses = relationship(
            "Course",
            secondary=student_course,
            backref=backref("students", lazy="dynamic"),
        )

        @property
        def url(self):
            return "/students/{}".format(self.id)

    class Teacher(Base):
        __tablename__ = "teacher"
        id = sa.Column(sa.Integer, primary_key=True)

        full_name = sa.Column(
            sa.String(255), nullable=False, unique=True, default="Mr. Noname"
        )

        current_school_id = sa.Column(
            sa.Integer, sa.ForeignKey(School.id), nullable=True
        )
        current_school = relationship(School, backref=backref("teachers"))

        substitute = relationship("SubstituteTeacher", uselist=False, backref="teacher")

    class SubstituteTeacher(Base):
        __tablename__ = "substituteteacher"
        id = sa.Column(sa.Integer, sa.ForeignKey("teacher.id"), primary_key=True)

    class Paper(Base):
        __tablename__ = "paper"

        satype = sa.Column(sa.String(50))
        __mapper_args__ = {"polymorphic_identity": "paper", "polymorphic_on": satype}

        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String, nullable=False, unique=True)

    class GradedPaper(Paper):
        __tablename__ = "gradedpaper"

        __mapper_args__ = {"polymorphic_identity": "gradedpaper"}

        id = sa.Column(sa.Integer, sa.ForeignKey("paper.id"), primary_key=True)

        marks_available = sa.Column(sa.Integer)

    class Seminar(Base):
        __tablename__ = "seminar"

        title = sa.Column(sa.String, primary_key=True)
        semester = sa.Column(sa.String, primary_key=True)

        label = column_property(title + ": " + semester)

    lecturekeywords_table = sa.Table(
        "lecturekeywords",
        Base.metadata,
        sa.Column("keyword_id", sa.Integer, sa.ForeignKey("keyword.id")),
        sa.Column("lecture_id", sa.Integer, sa.ForeignKey("lecture.id")),
    )

    class Keyword(Base):
        __tablename__ = "keyword"

        id = sa.Column(sa.Integer, primary_key=True)
        keyword = sa.Column(sa.String)

    class Lecture(Base):
        __tablename__ = "lecture"
        __table_args__ = (
            sa.ForeignKeyConstraint(
                ["seminar_title", "seminar_semester"],
                ["seminar.title", "seminar.semester"],
            ),
        )

        id = sa.Column(sa.Integer, primary_key=True)
        topic = sa.Column(sa.String)
        seminar_title = sa.Column(sa.String, sa.ForeignKey(Seminar.title))
        seminar_semester = sa.Column(sa.String, sa.ForeignKey(Seminar.semester))
        seminar = relationship(
            Seminar, foreign_keys=[seminar_title, seminar_semester], backref="lectures"
        )
        kw = relationship("Keyword", secondary=lecturekeywords_table)
        keywords = association_proxy(
            "kw", "keyword", creator=lambda kw: Keyword(keyword=kw)
        )

    # So that we can access models with dot-notation, e.g. models.Course
    class _models(object):
        def __init__(self):
            self.Course = Course
            self.School = School
            self.Student = Student
            self.Teacher = Teacher
            self.SubstituteTeacher = SubstituteTeacher
            self.Paper = Paper
            self.GradedPaper = GradedPaper
            self.Seminar = Seminar
            self.Lecture = Lecture
            self.Keyword = Keyword

    return _models()


class MyDateField(fields.Date):
    pass


@pytest.fixture()
def schemas(models, session):
    class CourseSchema(ModelSchema):
        class Meta:
            model = models.Course
            sqla_session = session
            strict = True  # for testing marshmallow 2

    class SchoolSchema(ModelSchema):
        class Meta:
            model = models.School
            sqla_session = session
            strict = True  # for testing marshmallow 2

    class StudentSchema(ModelSchema):
        class Meta:
            model = models.Student
            sqla_session = session
            strict = True  # for testing marshmallow 2

    class StudentSchemaWithCustomTypeMapping(ModelSchema):
        TYPE_MAPPING = Schema.TYPE_MAPPING.copy()
        TYPE_MAPPING.update({dt.date: MyDateField})

        class Meta:
            model = models.Student
            sqla_session = session
            strict = True  # for testing marshmallow 2

    class TeacherSchema(ModelSchema):
        class Meta:
            model = models.Teacher
            sqla_session = session
            strict = True  # for testing marshmallow 2

    class SubstituteTeacherSchema(ModelSchema):
        class Meta:
            model = models.SubstituteTeacher
            strict = True  # for testing marshmallow 2

    class PaperSchema(ModelSchema):
        class Meta:
            model = models.Paper
            sqla_session = session
            strict = True  # for testing marshmallow 2

    class GradedPaperSchema(ModelSchema):
        class Meta:
            model = models.GradedPaper
            sqla_session = session
            strict = True  # for testing marshmallow 2

    class HyperlinkStudentSchema(ModelSchema):
        class Meta:
            model = models.Student
            sqla_session = session
            strict = True  # for testing marshmallow 2

    class SeminarSchema(ModelSchema):
        class Meta:
            model = models.Seminar
            sqla_session = session
            strict = True  # for testing marshmallow 2

        label = fields.Str()

    class LectureSchema(ModelSchema):
        class Meta:
            model = models.Lecture
            sqla_session = session
            strict = True  # for testing marshmallow 2

    # Again, so we can use dot-notation
    class _schemas(object):
        def __init__(self):
            self.CourseSchema = CourseSchema
            self.SchoolSchema = SchoolSchema
            self.StudentSchema = StudentSchema
            self.StudentSchemaWithCustomTypeMapping = StudentSchemaWithCustomTypeMapping
            self.TeacherSchema = TeacherSchema
            self.SubstituteTeacherSchema = SubstituteTeacherSchema
            self.PaperSchema = PaperSchema
            self.GradedPaperSchema = GradedPaperSchema
            self.HyperlinkStudentSchema = HyperlinkStudentSchema
            self.SeminarSchema = SeminarSchema
            self.LectureSchema = LectureSchema

    return _schemas()


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

    def test_sets_allow_none_for_nullable_fields(self, models):
        fields_ = fields_for_model(models.Student)
        assert fields_["dob"].allow_none is True

    def test_sets_enum_choices(self, models):
        fields_ = fields_for_model(models.Course)
        validator = contains_validator(fields_["level"], validate.OneOf)
        assert validator
        assert list(validator.choices) == ["Primary", "Secondary"]

    def test_many_to_many_relationship(self, models):
        student_fields = fields_for_model(models.Student)
        assert type(student_fields["courses"]) is RelatedList

        course_fields = fields_for_model(models.Course)
        assert type(course_fields["students"]) is RelatedList

    def test_many_to_one_relationship(self, models):
        student_fields = fields_for_model(models.Student)
        assert type(student_fields["current_school"]) is Related

        school_fields = fields_for_model(models.School)
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
        fields_ = fields_for_model(models.Course)
        field = fields_["description"]
        validator = contains_validator(field, validate.Length)
        assert validator.max == 1000
        assert field.required


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
            (sa.Binary, fields.Str),
            (sa.LargeBinary, fields.Str),
            (sa.Text, fields.Str),
            (sa.Date, fields.Date),
            (sa.DateTime, fields.DateTime),
            (sa.Boolean, fields.Bool),
            (sa.Boolean, fields.Bool),
            (sa.Float, fields.Float),
            (sa.SmallInteger, fields.Int),
            (postgresql.UUID, fields.UUID),
            (postgresql.MACADDR, fields.Str),
            (postgresql.INET, fields.Str),
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


class TestTableSchema:
    @pytest.fixture
    def school(self, models, session):
        table = models.School.__table__
        insert = table.insert().values(name="Univ. of Whales")
        with session.connection() as conn:
            conn.execute(insert)
            select = table.select().limit(1)
            return conn.execute(select).fetchone()

    def test_dump_row(self, models, school):
        class SchoolSchema(TableSchema):
            class Meta:
                table = models.School.__table__

        schema = SchoolSchema()
        data = unpack(schema.dump(school))
        assert data == {"name": "Univ. of Whales", "school_id": 1}

    def test_exclude(self, models, school):
        class SchoolSchema(TableSchema):
            class Meta:
                table = models.School.__table__
                exclude = ("name",)

        schema = SchoolSchema()
        data = unpack(schema.dump(school))
        assert "name" not in data


class TestModelSchema:
    @pytest.fixture()
    def school(self, models, session):
        school_ = models.School(name="Univ. Of Whales")
        session.add(school_)
        session.flush()
        return school_

    @pytest.fixture()
    def school_with_teachers(self, models, session):
        school_with_teachers_ = models.School(name="School of Hard Knocks")
        school_with_teachers_.teachers = [
            models.Teacher(full_name="Teachy McTeachFace"),
            models.Teacher(full_name="Another Teacher"),
        ]
        session.add(school_with_teachers_)
        session.flush()
        return school_with_teachers_

    @pytest.fixture()
    def student(self, models, school, session):
        student_ = models.Student(full_name="Monty Python", current_school=school)
        session.add(student_)
        session.flush()
        return student_

    @pytest.fixture()
    def student_with_teachers(self, models, school_with_teachers, session):
        student_ = models.Student(
            full_name="Monty Python", current_school=school_with_teachers
        )
        session.add(student_)
        session.flush()
        return student_

    @pytest.fixture()
    def teacher(self, models, school, session):
        teacher_ = models.Teacher(full_name="The Substitute Teacher")
        session.add(teacher_)
        session.flush()
        return teacher_

    @pytest.fixture()
    def subteacher(self, models, school, teacher, session):
        sub_ = models.SubstituteTeacher(teacher=teacher)
        session.add(sub_)
        session.flush()
        return sub_

    @pytest.fixture()
    def seminar(self, models, session):
        seminar_ = models.Seminar(title="physics", semester="spring")
        session.add(seminar_)
        session.flush()
        return seminar_

    @pytest.fixture()
    def lecture(self, models, session, seminar):
        lecture_ = models.Lecture(topic="force", seminar=seminar)
        lecture_.keywords.extend(["Newton's Laws", "Friction"])
        session.add(lecture_)
        session.flush()
        return lecture_

    def test_model_schema_with_custom_type_mapping(self, schemas):
        schema = schemas.StudentSchemaWithCustomTypeMapping()
        assert type(schema.fields["dob"]) is MyDateField

    def test_model_schema_field_inheritance(self, schemas):
        class CourseSchemaSub(schemas.CourseSchema):
            additional = fields.Int()

        parent_schema = schemas.CourseSchema()
        child_schema = CourseSchemaSub()

        parent_schema_fields = set(parent_schema.declared_fields)
        child_schema_fields = set(child_schema.declared_fields)

        assert parent_schema_fields.issubset(child_schema_fields)
        assert "additional" in child_schema_fields

    def test_model_schema_class_meta_inheritance(self, models, session):
        class BaseCourseSchema(ModelSchema):
            class Meta:
                model = models.Course
                sqla_session = session

        class CourseSchema(BaseCourseSchema):
            pass

        schema = CourseSchema()
        field_names = schema.declared_fields
        assert "id" in field_names
        assert "name" in field_names
        assert "cost" in field_names

    def test_model_schema_ordered(self, models):
        class SchoolSchema(ModelSchema):
            class Meta:
                model = models.School
                ordered = True

        schema = SchoolSchema()
        assert isinstance(schema.fields, OrderedDict)
        fields = [prop.key for prop in models.School.__mapper__.iterate_properties]
        assert list(schema.fields.keys()) == fields

    def test_model_schema_dumping(self, schemas, student):
        schema = schemas.StudentSchema()
        data = unpack(schema.dump(student))
        # fk excluded by default
        assert "current_school_id" not in data
        # related field dumps to pk
        assert data["current_school"] == student.current_school.id

    def test_model_schema_loading(self, models, schemas, student, session):
        schema_kwargs = (
            {"unknown": marshmallow.INCLUDE} if MARSHMALLOW_VERSION_INFO[0] >= 3 else {}
        )
        schema = schemas.StudentSchema(**schema_kwargs)
        dump_data = unpack(schema.dump(student))
        load_data = unpack(schema.load(dump_data))

        assert load_data is student
        assert load_data.current_school == student.current_school

    def test_model_schema_loading_invalid_related_type(
        self, models, schemas, student, session
    ):
        schema = schemas.StudentSchema()
        dump_data = unpack(schema.dump(student))
        dump_data["current_school"] = [1]
        with pytest.raises(
            ValidationError, match="Could not deserialize related value"
        ):
            schema.load(dump_data)

    def test_model_schema_loading_missing_field(
        self, models, schemas, student, session
    ):
        schema = schemas.StudentSchema()
        dump_data = unpack(schema.dump(student))
        dump_data.pop("full_name")
        with pytest.raises(ValidationError) as excinfo:
            schema.load(dump_data)
        err = excinfo.value
        assert err.messages["full_name"] == ["Missing data for required field."]

    def test_model_schema_loading_custom_instance(
        self, models, schemas, student, session
    ):
        schema = schemas.StudentSchema(instance=student)
        dump_data = unpack(schema.dump(student))
        dump_data["full_name"] = "Terry Gilliam"
        load_data = unpack(schema.load(dump_data))

        assert load_data is student
        assert load_data.current_school == student.current_school

    # Regression test for https://github.com/marshmallow-code/marshmallow-sqlalchemy/issues/78
    def test_model_schema_loading_resets_instance(self, models, schemas, student):
        schema = schemas.StudentSchema()
        data1 = unpack(schema.load({"full_name": "new name"}, instance=student))
        assert data1.id == student.id
        assert data1.full_name == student.full_name

        data2 = unpack(schema.load({"full_name": "new name2"}))
        assert isinstance(data2, models.Student)
        # loaded data is different from first instance (student)
        assert data2 != student
        assert data2.full_name == "new name2"

    def test_model_schema_loading_no_instance_or_pk(
        self, models, schemas, student, session
    ):
        schema = schemas.StudentSchema()
        dump_data = {"full_name": "Terry Gilliam"}
        load_data = unpack(schema.load(dump_data))

        assert load_data is not student

    def test_model_schema_compound_key(self, schemas, seminar):
        schema = schemas.SeminarSchema()
        dump_data = unpack(schema.dump(seminar))
        load_data = unpack(schema.load(dump_data))

        assert load_data is seminar

    def test_model_schema_compound_key_relationship(self, schemas, lecture):
        schema = schemas.LectureSchema()
        dump_data = unpack(schema.dump(lecture))
        assert dump_data["seminar"] == {
            "title": lecture.seminar_title,
            "semester": lecture.seminar_semester,
        }
        load_data = unpack(schema.load(dump_data))

        assert load_data is lecture

    def test_model_schema_compound_key_relationship_invalid_key(self, schemas, lecture):
        schema = schemas.LectureSchema()
        dump_data = unpack(schema.dump(lecture))
        dump_data["seminar"] = "scalar"
        with pytest.raises(ValidationError) as excinfo:
            schema.load(dump_data)
        err = excinfo.value
        assert "seminar" in err.messages

    def test_model_schema_loading_passing_session_to_load(
        self, models, schemas, student, session
    ):
        class StudentSchemaNoSession(ModelSchema):
            class Meta:
                model = models.Student

        schema = StudentSchemaNoSession()
        dump_data = unpack(schema.dump(student))
        load_data = unpack(schema.load(dump_data, session=session))
        assert type(load_data) == models.Student
        assert load_data.current_school == student.current_school

    def test_model_schema_validation_passing_session_to_validate(
        self, models, schemas, student, session
    ):
        class StudentSchemaNoSession(ModelSchema):
            class Meta:
                model = models.Student

        schema = StudentSchemaNoSession()
        dump_data = unpack(schema.dump(student))
        assert type(schema.validate(dump_data, session=session)) is dict

    def test_model_schema_loading_passing_session_to_constructor(
        self, models, schemas, student, session
    ):
        class StudentSchemaNoSession(ModelSchema):
            class Meta:
                model = models.Student

        schema = StudentSchemaNoSession(session=session)
        dump_data = unpack(schema.dump(student))
        load_data = unpack(schema.load(dump_data))
        assert type(load_data) == models.Student
        assert load_data.current_school == student.current_school

    def test_model_schema_validation_passing_session_to_constructor(
        self, models, schemas, student, session
    ):
        class StudentSchemaNoSession(ModelSchema):
            class Meta:
                model = models.Student

        schema = StudentSchemaNoSession(session=session)
        dump_data = unpack(schema.dump(student))
        assert type(schema.validate(dump_data)) is dict

    def test_model_schema_loading_and_validation_with_no_session_raises_error(
        self, models, schemas, student, session
    ):
        class StudentSchemaNoSession(ModelSchema):
            class Meta:
                model = models.Student

        schema = StudentSchemaNoSession()
        dump_data = unpack(schema.dump(student))
        with pytest.raises(ValueError) as excinfo:
            schema.load(dump_data)
        assert excinfo.value.args[0] == "Deserialization requires a session"

        with pytest.raises(ValueError) as excinfo:
            schema.validate(dump_data)
        assert excinfo.value.args[0] == "Validation requires a session"

    def test_model_schema_custom_related_column(
        self, models, schemas, student, session
    ):
        class StudentSchema(ModelSchema):
            class Meta:
                model = models.Student
                sqla_session = session

            current_school = Related(column="name")

        schema = StudentSchema()
        dump_data = unpack(schema.dump(student))
        load_data = unpack(schema.load(dump_data))

        assert type(load_data) == models.Student
        assert load_data.current_school == student.current_school

    def test_model_schema_with_attribute(
        self, models, schemas, school, student, session
    ):
        class StudentSchema(Schema):
            id = fields.Int()

        class SchoolSchema(ModelSchema):
            class Meta:
                model = models.School
                sqla_session = session

            students = RelatedList(Related(attribute="students"), attribute="students")

        class SchoolSchema2(ModelSchema):
            class Meta:
                model = models.School
                sqla_session = session

            students = field_for(models.School, "students", attribute="students")

        class SchoolSchema3(ModelSchema):
            class Meta:
                model = models.School
                sqla_session = session

            students = field_for(
                models.School, "students", attribute="students", column="full_name"
            )

        schema = SchoolSchema()
        schema2 = SchoolSchema2()
        schema3 = SchoolSchema3()

        dump_data = unpack(schema.dump(school))
        dump_data2 = unpack(schema2.dump(school))
        dump_data3 = unpack(schema3.dump(school))
        load_data = unpack(schema.load(dump_data))

        assert dump_data["students"] == dump_data["students"]
        assert dump_data["students"] == dump_data2["students"]
        assert [i.full_name for i in school.students] == dump_data3["students"]

        assert type(load_data) == models.School
        assert load_data.students == school.students

    def test_dump_many_to_one_relationship(self, models, schemas, school, student):
        schema = schemas.SchoolSchema()
        dump_data = unpack(schema.dump(school))

        assert dump_data["students"] == [student.id]

    def test_load_many_to_one_relationship(self, models, schemas, school, student):
        schema = schemas.SchoolSchema()
        dump_data = unpack(schema.dump(school))
        load_data = unpack(schema.load(dump_data))
        assert type(load_data.students[0]) is models.Student
        assert load_data.students[0] == student

    def test_fields_option(self, student, models, session):
        class StudentSchema(ModelSchema):
            class Meta:
                model = models.Student
                sqla_session = session
                fields = ("full_name", "date_created")

        session.commit()
        schema = StudentSchema()
        data = unpack(schema.dump(student))

        assert "full_name" in data
        assert "date_created" in data
        assert "dob" not in data
        assert len(data.keys()) == 2

    def test_exclude_option(self, student, models, session):
        class StudentSchema(ModelSchema):
            class Meta:
                model = models.Student
                sqla_session = session
                exclude = ("date_created",)

        session.commit()
        schema = StudentSchema()
        data = unpack(schema.dump(student))

        assert "full_name" in data
        assert "date_created" not in data

    def test_include_fk_option(self, models, schemas):
        class StudentSchema(ModelSchema):
            class Meta:
                model = models.Student
                include_fk = True

        assert "current_school_id" in StudentSchema().fields
        assert "current_school_id" not in schemas.StudentSchema().fields

    def test_additional_option(self, student, models, session):
        class StudentSchema(ModelSchema):
            uppername = fields.Function(lambda x: x.full_name.upper())

            class Meta:
                model = models.Student
                sqla_session = session
                additional = ("date_created",)

        session.commit()
        schema = StudentSchema()
        data = unpack(schema.dump(student))
        assert "full_name" in data
        assert "uppername" in data
        assert data["uppername"] == student.full_name.upper()

    def test_field_override(self, student, models, session):
        class MyString(fields.Str):
            def _serialize(self, val, attr, obj):
                return val.upper()

        class StudentSchema(ModelSchema):
            full_name = MyString()

            class Meta:
                model = models.Student
                sqla_session = session

        session.commit()
        schema = StudentSchema()
        data = unpack(schema.dump(student))
        assert "full_name" in data
        assert data["full_name"] == student.full_name.upper()

    def test_a_teacher_who_is_a_substitute(
        self, models, schemas, teacher, subteacher, session
    ):
        session.commit()
        schema = schemas.TeacherSchema()
        data = unpack(schema.dump(teacher))
        load_data = unpack(schema.load(data))

        assert type(load_data) is models.Teacher
        assert "substitute" in data
        assert data["substitute"] == subteacher.id

    def test_dump_only_relationship(self, models, session, school, student):
        class SchoolSchema2(ModelSchema):
            class Meta:
                model = models.School

            students = field_for(models.School, "students", dump_only=True)

            # override for easier testing
            @post_load
            def make_instance(self, data, **kwargs):
                return data

        sch = SchoolSchema2()
        students_field = sch.fields["students"]

        assert students_field.dump_only is True
        dump_data = unpack(sch.dump(school))
        if MARSHMALLOW_VERSION_INFO[0] < 3:
            load_data = unpack(sch.load(dump_data, session=session))
            assert "students" not in load_data
        else:
            with pytest.raises(ValidationError) as excinfo:
                sch.load(dump_data, session=session)
            err = excinfo.value
            assert err.messages == {"students": ["Unknown field."]}

    def test_transient_schema(self, models, school):
        class SchoolSchemaTransient(ModelSchema):
            class Meta:
                model = models.School
                transient = True

        sch = SchoolSchemaTransient()
        dump_data = unpack(sch.dump(school))
        load_data = unpack(sch.load(dump_data))
        assert isinstance(load_data, models.School)
        state = sa.inspect(load_data)
        assert state.transient

    def test_transient_load(self, models, session, school):
        class SchoolSchemaTransient(ModelSchema):
            class Meta:
                model = models.School
                sqla_session = session

        sch = SchoolSchemaTransient()
        dump_data = unpack(sch.dump(school))
        load_data = unpack(sch.load(dump_data, transient=True))
        assert isinstance(load_data, models.School)
        state = sa.inspect(load_data)
        assert state.transient

    @pytest.mark.skipif(
        MARSHMALLOW_VERSION_INFO[0] < 3, reason="`unknown` was added in marshmallow 3"
    )
    def test_transient_load_with_unknown_include(self, models, session, school):
        class SchoolSchemaTransient(ModelSchema):
            class Meta:
                model = models.School
                sqla_session = session
                unknown = marshmallow.INCLUDE

        sch = SchoolSchemaTransient()
        dump_data = unpack(sch.dump(school))
        dump_data["foo"] = "bar"
        load_data = unpack(sch.load(dump_data, transient=True))

        assert isinstance(load_data, models.School)
        state = sa.inspect(load_data)
        assert state.transient

    def test_transient_schema_with_relationship(
        self, models, student_with_teachers, session
    ):
        class StudentSchemaTransient(ModelSchema):
            class Meta:
                model = models.Student
                transient = True

        sch = StudentSchemaTransient()
        dump_data = unpack(sch.dump(student_with_teachers))
        load_data = unpack(sch.load(dump_data))
        assert isinstance(load_data, models.Student)
        state = sa.inspect(load_data)
        assert state.transient

        school = load_data.current_school
        assert isinstance(school, models.School)
        school_state = sa.inspect(school)
        assert school_state.transient

    def test_transient_schema_with_association(self, models, student_with_teachers):
        class StudentSchemaTransient(ModelSchema):
            class Meta:
                model = models.Student
                transient = True

            possible_teachers = field_for(
                models.School, "teachers", attribute="possible_teachers"
            )

        sch = StudentSchemaTransient()
        dump_data = unpack(sch.dump(student_with_teachers))
        load_data = unpack(sch.load(dump_data))
        assert isinstance(load_data, models.Student)
        state = sa.inspect(load_data)
        assert state.transient

        school = load_data.current_school
        assert isinstance(school, models.School)
        school_state = sa.inspect(school)
        assert school_state.transient

        possible_teachers = load_data.possible_teachers
        assert possible_teachers
        assert school.teachers == load_data.possible_teachers
        assert sa.inspect(possible_teachers[0]).transient

    def test_transient_schema_with_implicit_association(self, models, lecture):
        """Test for transient implicit creation of associated objects."""

        class LectureSchemaTransient(ModelSchema):
            class Meta:
                model = models.Lecture
                transient = True

            keywords = field_for(
                models.Keyword,
                "keyword",
                attribute="keywords",
                field_class=fields.List,
                cls_or_instance=fields.Str,
            )

        sch = LectureSchemaTransient()
        dump_data = unpack(sch.dump(lecture))
        del dump_data["kw"]
        load_data = unpack(sch.load(dump_data))
        assert isinstance(load_data, models.Lecture)
        state = sa.inspect(load_data)
        assert state.transient

        kw_objects = load_data.kw
        assert kw_objects
        for keyword in kw_objects:
            assert isinstance(keyword, models.Keyword)
            assert sa.inspect(keyword).transient

        keywords = {kw.keyword for kw in kw_objects}
        assert keywords == set(load_data.keywords)

    def test_exclude(self, models, school):
        class SchoolSchema(ModelSchema):
            class Meta:
                model = models.School
                exclude = ("name",)

        schema = SchoolSchema()
        data = unpack(schema.dump(school))
        assert "name" not in data


class TestNullForeignKey:
    @pytest.fixture()
    def school(self, models, session):
        school_ = models.School(name="The Teacherless School")
        session.add(school_)
        session.flush()
        return school_

    @pytest.fixture()
    def teacher(self, models, school, session):
        teacher_ = models.Teacher(full_name="The Schoolless Teacher")
        session.add(teacher_)
        session.flush()
        return teacher_

    def test_a_teacher_with_no_school(self, models, schemas, teacher, session):
        session.commit()
        schema = schemas.TeacherSchema()
        dump_data = unpack(schema.dump(teacher))
        load_data = unpack(schema.load(dump_data))

        assert type(load_data) == models.Teacher
        assert load_data.current_school is None

    def test_a_teacher_who_is_not_a_substitute(self, models, schemas, teacher, session):
        session.commit()
        schema = schemas.TeacherSchema()
        data = unpack(schema.dump(teacher))
        load_data = unpack(schema.load(data))

        assert type(load_data) is models.Teacher
        assert "substitute" in data
        assert data["substitute"] is None


class TestDeserializeObjectThatDNE:
    def test_deserialization_of_seminar_with_many_lectures_that_DNE(
        self, models, schemas, session
    ):
        seminar_schema = schemas.SeminarSchema()
        seminar_dict = {
            "title": "Novice Training",
            "semester": "First",
            "lectures": [
                {
                    "topic": "Intro to Ter'Angreal",
                    "seminar_title": "Novice Training",
                    "seminar_semester": "First",
                },
                {
                    "topic": "History of the Ajahs",
                    "seminar_title": "Novice Training",
                    "seminar_semester": "First",
                },
            ],
        }
        deserialized_seminar_object = unpack(seminar_schema.load(seminar_dict, session))
        # Ensure both nested lecture objects weren't forgotten...

        assert len(deserialized_seminar_object.lectures) == 2
        # Assume that if 1 lecture has a field with the correct String value, all strings
        # were successfully deserialized in nested objects
        assert deserialized_seminar_object.lectures[0].topic == "Intro to Ter'Angreal"


class TestMarshmallowContext:
    def test_getting_session_from_marshmallow_context(self, session, models):
        class SchoolSchema(ModelSchema):
            @property
            def session(self):
                return (
                    self.context.get("session", None)
                    or self._session
                    or self.opts.sqla_session
                )

            class Meta:
                model = models.School
                fields = ("name",)

        class SchoolWrapperSchema(Schema):
            school = fields.Nested(SchoolSchema)
            schools = fields.Nested(SchoolSchema, many=True)

        schema = SchoolWrapperSchema(context=dict(session=session))
        data = {
            "school": {"name": "one school"},
            "schools": [{"name": "another school"}, {"name": "yet, another school"}],
        }
        result = unpack(schema.load(data))
        session.add(result["school"])
        session.add_all(result["schools"])
        session.flush()
        assert isinstance(result["school"], models.School)
        assert isinstance(result["school"].id, int)
        for school in result["schools"]:
            assert isinstance(school, models.School)
            assert isinstance(school.id, int)
        dump_result = unpack(schema.dump(result))
        assert dump_result == data


class TestNestedFieldSession:
    """Test the session can be passed to nested fields."""

    @pytest.fixture
    def association_table(self, Base):
        return sa.Table(
            "association",
            Base.metadata,
            sa.Column("left_id", sa.Integer, sa.ForeignKey("left.id")),
            sa.Column("right_id", sa.Integer, sa.ForeignKey("right.id")),
        )

    @pytest.fixture
    def parent_model(self, Base, association_table):
        class Parent(Base):
            __tablename__ = "left"
            id = sa.Column(sa.Integer, primary_key=True)
            name = sa.Column(sa.Text)
            children = relationship(
                "Child", secondary=association_table, back_populates="parents"
            )

        return Parent

    @pytest.fixture
    def child_model(self, Base, parent_model, association_table):
        class Child(Base):
            __tablename__ = "right"
            id = sa.Column(sa.Integer, primary_key=True)
            name = sa.Column(sa.Text)
            parents = relationship(
                "Parent", secondary=association_table, back_populates="children"
            )

        return Child

    @pytest.fixture
    def child_schema(self, child_model):
        class ChildSchema(ModelSchema):
            class Meta:
                model = child_model

        return ChildSchema

    def test_session_is_passed_to_nested_field(
        self, child_schema, parent_model, session
    ):
        class ParentSchema(ModelSchema):
            children = Nested(child_schema, many=True)

            class Meta:
                model = parent_model

        data = {"name": "Parent1", "children": [{"name": "Child1"}]}
        ParentSchema().load(data, session=session)

    def test_session_is_passed_to_nested_field_in_list_field(
        self, parent_model, child_model, child_schema, session
    ):
        class ParentSchema(ModelSchema):
            children = fields.List(Nested(child_schema))

            class Meta:
                model = parent_model

        data = {"name": "Jorge", "children": [{"name": "Jose"}]}
        ParentSchema().load(data, session=session)

    # regression test for https://github.com/marshmallow-code/marshmallow-sqlalchemy/issues/177
    def test_transient_is_propgated_to_nested_field(self, child_schema, parent_model):
        class ParentSchema(ModelSchema):
            children = Nested(child_schema, many=True)

            class Meta:
                model = parent_model

        data = {"name": "Parent1", "children": [{"name": "Child1"}]}
        load_data = unpack(ParentSchema().load(data, transient=True))
        child = load_data.children[0]
        state = sa.inspect(child)
        assert state.transient


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
