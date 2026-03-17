from types import SimpleNamespace
import datetime as dt

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref, column_property, synonym


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
        description = sa.Column(sa.Text, nullable=True)
        level = sa.Column(sa.Enum("Primary", "Secondary"))
        has_prereqs = sa.Column(sa.Boolean, nullable=False)
        started = sa.Column(sa.DateTime, nullable=False)
        grade = sa.Column(AnotherInteger, nullable=False)
        transcription = sa.Column(AnotherText, nullable=False)

        @property
        def url(self):
            return f"/courses/{self.id}"

    class School(Base):
        __tablename__ = "school"
        id = sa.Column("school_id", sa.Integer, primary_key=True)
        name = sa.Column(sa.String(255), nullable=False)

        student_ids = association_proxy(
            "students", "id", creator=lambda sid: Student(id=sid)
        )

        @property
        def url(self):
            return f"/schools/{self.id}"

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
            Course,
            secondary=student_course,
            backref=backref("students", lazy="dynamic"),
        )

        @property
        def url(self):
            return f"/students/{self.id}"

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
        curr_school_id = synonym("current_school_id")

        substitute = relationship("SubstituteTeacher", uselist=False, backref="teacher")

        @property
        def fname(self):
            return self.full_name

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

    return SimpleNamespace(
        Course=Course,
        School=School,
        Student=Student,
        Teacher=Teacher,
        SubstituteTeacher=SubstituteTeacher,
        Paper=Paper,
        GradedPaper=GradedPaper,
        Seminar=Seminar,
        Lecture=Lecture,
        Keyword=Keyword,
    )
