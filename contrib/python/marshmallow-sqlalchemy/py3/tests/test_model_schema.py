from collections import OrderedDict
from types import SimpleNamespace
import datetime as dt

import pytest
import marshmallow
import sqlalchemy as sa
from sqlalchemy.orm import relationship
from marshmallow import fields, ValidationError, Schema, post_load

from marshmallow_sqlalchemy import ModelSchema, field_for
from marshmallow_sqlalchemy.fields import Related, RelatedList, Nested


class MyDateField(fields.Date):
    pass


@pytest.fixture()
def schemas(models, session):
    class CourseSchema(ModelSchema):
        class Meta:
            model = models.Course
            sqla_session = session

    class SchoolSchema(ModelSchema):
        class Meta:
            model = models.School
            sqla_session = session

    class StudentSchema(ModelSchema):
        class Meta:
            model = models.Student
            sqla_session = session

    class StudentSchemaWithCustomTypeMapping(ModelSchema):
        TYPE_MAPPING = Schema.TYPE_MAPPING.copy()
        TYPE_MAPPING.update({dt.date: MyDateField})

        class Meta:
            model = models.Student
            sqla_session = session

    class TeacherSchema(ModelSchema):
        class Meta:
            model = models.Teacher
            sqla_session = session

    class SubstituteTeacherSchema(ModelSchema):
        class Meta:
            model = models.SubstituteTeacher

    class PaperSchema(ModelSchema):
        class Meta:
            model = models.Paper
            sqla_session = session

    class GradedPaperSchema(ModelSchema):
        class Meta:
            model = models.GradedPaper
            sqla_session = session

    class HyperlinkStudentSchema(ModelSchema):
        class Meta:
            model = models.Student
            sqla_session = session

    class SeminarSchema(ModelSchema):
        class Meta:
            model = models.Seminar
            sqla_session = session

        label = fields.Str()

    class LectureSchema(ModelSchema):
        class Meta:
            model = models.Lecture
            sqla_session = session

    return SimpleNamespace(
        CourseSchema=CourseSchema,
        SchoolSchema=SchoolSchema,
        StudentSchema=StudentSchema,
        StudentSchemaWithCustomTypeMapping=StudentSchemaWithCustomTypeMapping,
        TeacherSchema=TeacherSchema,
        SubstituteTeacherSchema=SubstituteTeacherSchema,
        PaperSchema=PaperSchema,
        GradedPaperSchema=GradedPaperSchema,
        HyperlinkStudentSchema=HyperlinkStudentSchema,
        SeminarSchema=SeminarSchema,
        LectureSchema=LectureSchema,
    )


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
        data = schema.dump(student)
        # fk excluded by default
        assert "current_school_id" not in data
        # related field dumps to pk
        assert data["current_school"] == student.current_school.id

    def test_model_schema_loading(self, models, schemas, student, session):
        schema = schemas.StudentSchema(unknown=marshmallow.INCLUDE)
        dump_data = schema.dump(student)
        load_data = schema.load(dump_data)

        assert load_data is student
        assert load_data.current_school == student.current_school

    def test_model_schema_loading_invalid_related_type(
        self, models, schemas, student, session
    ):
        schema = schemas.StudentSchema()
        dump_data = schema.dump(student)
        dump_data["current_school"] = [1]
        with pytest.raises(
            ValidationError, match="Could not deserialize related value"
        ):
            schema.load(dump_data)

    def test_model_schema_loading_missing_field(
        self, models, schemas, student, session
    ):
        schema = schemas.StudentSchema()
        dump_data = schema.dump(student)
        dump_data.pop("full_name")
        with pytest.raises(ValidationError) as excinfo:
            schema.load(dump_data)
        err = excinfo.value
        assert err.messages["full_name"] == ["Missing data for required field."]

    def test_model_schema_loading_custom_instance(
        self, models, schemas, student, session
    ):
        schema = schemas.StudentSchema(instance=student)
        dump_data = schema.dump(student)
        dump_data["full_name"] = "Terry Gilliam"
        load_data = schema.load(dump_data)

        assert load_data is student
        assert load_data.current_school == student.current_school

    # Regression test for https://github.com/marshmallow-code/marshmallow-sqlalchemy/issues/78
    def test_model_schema_loading_resets_instance(self, models, schemas, student):
        schema = schemas.StudentSchema()
        data1 = schema.load({"full_name": "new name"}, instance=student)
        assert data1.id == student.id
        assert data1.full_name == student.full_name

        data2 = schema.load({"full_name": "new name2"})
        assert isinstance(data2, models.Student)
        # loaded data is different from first instance (student)
        assert data2 != student
        assert data2.full_name == "new name2"

    def test_model_schema_loading_no_instance_or_pk(
        self, models, schemas, student, session
    ):
        schema = schemas.StudentSchema()
        dump_data = {"full_name": "Terry Gilliam"}
        load_data = schema.load(dump_data)

        assert load_data is not student

    def test_model_schema_compound_key(self, schemas, seminar):
        schema = schemas.SeminarSchema()
        dump_data = schema.dump(seminar)
        load_data = schema.load(dump_data)

        assert load_data is seminar

    def test_model_schema_compound_key_relationship(self, schemas, lecture):
        schema = schemas.LectureSchema()
        dump_data = schema.dump(lecture)
        assert dump_data["seminar"] == {
            "title": lecture.seminar_title,
            "semester": lecture.seminar_semester,
        }
        load_data = schema.load(dump_data)

        assert load_data is lecture

    def test_model_schema_compound_key_relationship_invalid_key(self, schemas, lecture):
        schema = schemas.LectureSchema()
        dump_data = schema.dump(lecture)
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
        dump_data = schema.dump(student)
        load_data = schema.load(dump_data, session=session)
        assert type(load_data) == models.Student
        assert load_data.current_school == student.current_school

    def test_model_schema_validation_passing_session_to_validate(
        self, models, schemas, student, session
    ):
        class StudentSchemaNoSession(ModelSchema):
            class Meta:
                model = models.Student

        schema = StudentSchemaNoSession()
        dump_data = schema.dump(student)
        assert type(schema.validate(dump_data, session=session)) is dict

    def test_model_schema_loading_passing_session_to_constructor(
        self, models, schemas, student, session
    ):
        class StudentSchemaNoSession(ModelSchema):
            class Meta:
                model = models.Student

        schema = StudentSchemaNoSession(session=session)
        dump_data = schema.dump(student)
        load_data = schema.load(dump_data)
        assert type(load_data) == models.Student
        assert load_data.current_school == student.current_school

    def test_model_schema_validation_passing_session_to_constructor(
        self, models, schemas, student, session
    ):
        class StudentSchemaNoSession(ModelSchema):
            class Meta:
                model = models.Student

        schema = StudentSchemaNoSession(session=session)
        dump_data = schema.dump(student)
        assert type(schema.validate(dump_data)) is dict

    def test_model_schema_loading_and_validation_with_no_session_raises_error(
        self, models, schemas, student, session
    ):
        class StudentSchemaNoSession(ModelSchema):
            class Meta:
                model = models.Student

        schema = StudentSchemaNoSession()
        dump_data = schema.dump(student)
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
        dump_data = schema.dump(student)
        load_data = schema.load(dump_data)

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

        dump_data = schema.dump(school)
        dump_data2 = schema2.dump(school)
        dump_data3 = schema3.dump(school)
        load_data = schema.load(dump_data)

        assert dump_data["students"] == dump_data["students"]
        assert dump_data["students"] == dump_data2["students"]
        assert [i.full_name for i in school.students] == dump_data3["students"]

        assert type(load_data) == models.School
        assert load_data.students == school.students

    def test_dump_many_to_one_relationship(self, models, schemas, school, student):
        schema = schemas.SchoolSchema()
        dump_data = schema.dump(school)

        assert dump_data["students"] == [student.id]

    def test_load_many_to_one_relationship(self, models, schemas, school, student):
        schema = schemas.SchoolSchema()
        dump_data = schema.dump(school)
        load_data = schema.load(dump_data)
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
        data = schema.dump(student)

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
        data = schema.dump(student)

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
        data = schema.dump(student)
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
        data = schema.dump(student)
        assert "full_name" in data
        assert data["full_name"] == student.full_name.upper()

    def test_a_teacher_who_is_a_substitute(
        self, models, schemas, teacher, subteacher, session
    ):
        session.commit()
        schema = schemas.TeacherSchema()
        data = schema.dump(teacher)
        load_data = schema.load(data)

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
        dump_data = sch.dump(school)
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
        dump_data = sch.dump(school)
        load_data = sch.load(dump_data)
        assert isinstance(load_data, models.School)
        state = sa.inspect(load_data)
        assert state.transient

    def test_transient_load(self, models, session, school):
        class SchoolSchemaTransient(ModelSchema):
            class Meta:
                model = models.School
                sqla_session = session

        sch = SchoolSchemaTransient()
        dump_data = sch.dump(school)
        load_data = sch.load(dump_data, transient=True)
        assert isinstance(load_data, models.School)
        state = sa.inspect(load_data)
        assert state.transient

    def test_transient_load_with_unknown_include(self, models, session, school):
        class SchoolSchemaTransient(ModelSchema):
            class Meta:
                model = models.School
                sqla_session = session
                unknown = marshmallow.INCLUDE

        sch = SchoolSchemaTransient()
        dump_data = sch.dump(school)
        dump_data["foo"] = "bar"
        load_data = sch.load(dump_data, transient=True)

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
        dump_data = sch.dump(student_with_teachers)
        load_data = sch.load(dump_data)
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
        dump_data = sch.dump(student_with_teachers)
        load_data = sch.load(dump_data)
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
        dump_data = sch.dump(lecture)
        del dump_data["kw"]
        load_data = sch.load(dump_data)
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
        data = schema.dump(school)
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
        dump_data = schema.dump(teacher)
        load_data = schema.load(dump_data)

        assert type(load_data) == models.Teacher
        assert load_data.current_school is None

    def test_a_teacher_who_is_not_a_substitute(self, models, schemas, teacher, session):
        session.commit()
        schema = schemas.TeacherSchema()
        data = schema.dump(teacher)
        load_data = schema.load(data)

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
                    "id": 99999998,
                    "topic": "Intro to Ter'Angreal",
                    "seminar_title": "Novice Training",
                    "seminar_semester": "First",
                },
                {
                    "id": 99999999,
                    "topic": "History of the Ajahs",
                    "seminar_title": "Novice Training",
                    "seminar_semester": "First",
                },
            ],
        }
        deserialized_seminar_object = seminar_schema.load(seminar_dict, session=session)

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
        result = schema.load(data)
        session.add(result["school"])
        session.add_all(result["schools"])
        session.flush()
        assert isinstance(result["school"], models.School)
        assert isinstance(result["school"].id, int)
        for school in result["schools"]:
            assert isinstance(school, models.School)
            assert isinstance(school.id, int)
        dump_result = schema.dump(result)
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
        load_data = ParentSchema().load(data, transient=True)
        child = load_data.children[0]
        state = sa.inspect(child)
        assert state.transient
