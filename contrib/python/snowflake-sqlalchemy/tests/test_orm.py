#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import enum
import logging

import pytest
from sqlalchemy import (
    Column,
    Enum,
    ForeignKey,
    Integer,
    Sequence,
    String,
    func,
    select,
    text,
)
from sqlalchemy.orm import Session, declarative_base, relationship


def test_basic_orm(engine_testaccount):
    """
    Tests declarative
    """
    Base = declarative_base()

    class UserStatus(enum.Enum):
        ACTIVE = ("active",)
        INACTIVE = "inactive"

    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
        name = Column(String)
        fullname = Column(String)
        status = Column(Enum(UserStatus), default=UserStatus.ACTIVE)

        def __repr__(self):
            return f"<User({self.name!r}, {self.fullname!r})>"

    Base.metadata.create_all(engine_testaccount)
    try:
        ed_user = User(name="ed", fullname="Edward Jones")

        session = Session(bind=engine_testaccount)
        session.add(ed_user)

        our_user = session.query(User).filter_by(name="ed").first()
        assert our_user == ed_user
        session.commit()
    finally:
        Base.metadata.drop_all(engine_testaccount)


def test_orm_one_to_many_relationship(engine_testaccount):
    """
    Tests One to Many relationship
    """
    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
        name = Column(String)
        fullname = Column(String)

        def __repr__(self):
            return f"<User({self.name!r}, {self.fullname!r})>"

    class Address(Base):
        __tablename__ = "address"

        id = Column(Integer, Sequence("address_id_seq"), primary_key=True)
        email_address = Column(String, nullable=False)
        user_id = Column(Integer, ForeignKey("user.id"))

        user = relationship("User", backref="addresses")

        def __repr__(self):
            return f"<Address({repr(self.email_address)})>"

    Base.metadata.create_all(engine_testaccount)

    try:
        jack = User(name="jack", fullname="Jack Bean")
        assert jack.addresses == [], "one to many record is empty list"

        jack.addresses = [
            Address(email_address="jack@gmail.com"),
            Address(email_address="j25@yahoo.com"),
            Address(email_address="jack@hotmail.com"),
        ]

        session = Session(bind=engine_testaccount)
        session.add(jack)  # cascade each Address into the Session as well
        session.commit()

        got_jack = session.query(User).first()
        assert got_jack == jack

        bob = User(name="bob", fullname="Bob Dyran")

        session.add(bob)
        got_bob = session.query(User).filter_by(name="bob").first()
        assert got_bob == bob
        session.rollback()
        got_whoever = session.query(User).all()
        assert len(got_whoever) == 1, "number of user"
        assert got_whoever[0] == jack

        session.delete(jack)
        got_addresses = session.query(Address).all()
        assert len(got_addresses) == 3, (
            "address records still remain in no " "cascade mode"
        )

    finally:
        Base.metadata.drop_all(engine_testaccount)


def test_delete_cascade(engine_testaccount):
    """
    Test delete cascade
    """
    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
        name = Column(String)
        fullname = Column(String)

        addresses = relationship(
            "Address", back_populates="user", cascade="all, delete, delete-orphan"
        )

        def __repr__(self):
            return f"<User({self.name!r}, {self.fullname!r})>"

    class Address(Base):
        __tablename__ = "address"

        id = Column(Integer, Sequence("address_id_seq"), primary_key=True)
        email_address = Column(String, nullable=False)
        user_id = Column(Integer, ForeignKey("user.id"))

        user = relationship("User", back_populates="addresses")

        def __repr__(self):
            return f"<Address({repr(self.email_address)})>"

    Base.metadata.create_all(engine_testaccount)

    try:
        jack = User(name="jack", fullname="Jack Bean")
        assert jack.addresses == [], "one to many record is empty list"

        jack.addresses = [
            Address(email_address="jack@gmail.com"),
            Address(email_address="j25@yahoo.com"),
            Address(email_address="jack@hotmail.com"),
        ]

        session = Session(bind=engine_testaccount)
        session.add(jack)  # cascade each Address into the Session as well
        session.commit()

        got_jack = session.query(User).first()
        assert got_jack == jack

        session.delete(jack)
        got_addresses = session.query(Address).all()
        assert len(got_addresses) == 0, "no address record"
    finally:
        Base.metadata.drop_all(engine_testaccount)


@pytest.mark.skipif(
    True,
    reason="""
WIP
""",
)
def test_orm_query(engine_testaccount):
    """
    Tests ORM query
    """
    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, primary_key=True)
        name = Column(String)
        fullname = Column(String)

        def __repr__(self):
            return f"<User({self.name!r}, {self.fullname!r})>"

    Base.metadata.create_all(engine_testaccount)

    # TODO: insert rows

    session = Session(bind=engine_testaccount)

    # TODO: query.all()
    for name, fullname in session.query(User.name, User.fullname):
        print(name, fullname)

        # TODO: session.query.one() must return always one. NoResultFound and
        # MultipleResultsFound if not one result


def test_schema_including_db(engine_testaccount, db_parameters):
    """
    Test schema parameter including database separated by a dot.
    """
    Base = declarative_base()

    namespace = f"{db_parameters['database']}.{db_parameters['schema']}"

    class User(Base):
        __tablename__ = "users"
        __table_args__ = {"schema": namespace}

        id = Column(
            Integer, Sequence("user_id_orm_seq", schema=namespace), primary_key=True
        )
        name = Column(String)
        fullname = Column(String)

    Base.metadata.create_all(engine_testaccount)
    try:
        ed_user = User(name="ed", fullname="Edward Jones")

        session = Session(bind=engine_testaccount)
        session.add(ed_user)

        ret_user = session.query(User.id, User.name).first()
        assert ret_user[0] == 1
        assert ret_user[1] == "ed"

        session.commit()
    finally:
        Base.metadata.drop_all(engine_testaccount)


def test_schema_including_dot(engine_testaccount, db_parameters):
    """
    Tests pseudo schema name including dot.
    """
    Base = declarative_base()

    namespace = '{db}."{schema}.{schema}".{db}'.format(
        db=db_parameters["database"].lower(), schema=db_parameters["schema"].lower()
    )

    class User(Base):
        __tablename__ = "users"
        __table_args__ = {"schema": namespace}

        id = Column(
            Integer, Sequence("user_id_orm_seq", schema=namespace), primary_key=True
        )
        name = Column(String)
        fullname = Column(String)

    session = Session(bind=engine_testaccount)
    query = session.query(User.id)
    assert str(query).startswith(
        'SELECT {db}."{schema}.{schema}".{db}.users.id'.format(
            db=db_parameters["database"].lower(), schema=db_parameters["schema"].lower()
        )
    )


def test_schema_translate_map(engine_testaccount, db_parameters):
    """
    Test schema translate map execution option works replaces schema correctly
    """
    Base = declarative_base()

    namespace = f"{db_parameters['database']}.{db_parameters['schema']}"
    schema_map = "A"

    class User(Base):
        __tablename__ = "users"
        __table_args__ = {"schema": schema_map}

        id = Column(
            Integer, Sequence("user_id_orm_seq", schema=namespace), primary_key=True
        )
        name = Column(String)
        fullname = Column(String)

    with engine_testaccount.connect().execution_options(
        schema_translate_map={schema_map: db_parameters["schema"]}
    ) as con:
        session = Session(bind=con)
        with con.begin():
            Base.metadata.create_all(con)
            try:
                query = session.query(User)

                # insert some data in a way that makes sure that we're working in the right testing schema
                con.execute(
                    text(
                        f"insert into {db_parameters['schema']}.{User.__tablename__} values (0, 'testuser', 'test_user')"
                    )
                )

                # assert the precompiled query contains the schema_map and not the actual schema
                assert str(query).startswith(
                    f'SELECT "{schema_map}".{User.__tablename__}'
                )

                # run query and see that schema translation was done corectly
                results = query.all()
                assert len(results) == 1
                user = results.pop()
                assert user.id == 0
                assert user.name == "testuser"
                assert user.fullname == "test_user"
            finally:
                Base.metadata.drop_all(con)


def test_outer_lateral_join(engine_testaccount, caplog):
    Base = declarative_base()

    class Employee(Base):
        __tablename__ = "employees"

        employee_id = Column(Integer, primary_key=True)
        last_name = Column(String)

    class Department(Base):
        __tablename__ = "departments"

        department_id = Column(Integer, primary_key=True)
        name = Column(String)

    Base.metadata.create_all(engine_testaccount)
    session = Session(bind=engine_testaccount)
    e1 = Employee(employee_id=101, last_name="Richards")
    d1 = Department(department_id=1, name="Engineering")
    session.add_all([e1, d1])
    session.commit()

    sub = select(Department).lateral()
    query = (
        select(Employee.employee_id, Department.department_id)
        .select_from(Employee)
        .outerjoin(sub)
    )
    compiled_stmts = (
        # v1.x
        "SELECT employees.employee_id, departments.department_id "
        "FROM departments, employees LEFT OUTER JOIN LATERAL "
        "(SELECT departments.department_id AS department_id, departments.name AS name "
        "FROM departments) AS anon_1",
        # v2.x
        "SELECT employees.employee_id, departments.department_id "
        "FROM employees LEFT OUTER JOIN LATERAL "
        "(SELECT departments.department_id AS department_id, departments.name AS name "
        "FROM departments) AS anon_1, departments",
    )
    compiled_stmt = str(query.compile(engine_testaccount)).replace("\n", "")
    assert compiled_stmt in compiled_stmts

    with caplog.at_level(logging.DEBUG):
        assert [res for res in session.execute(query)]
    assert (
        "SELECT employees.employee_id, departments.department_id FROM departments"
        in caplog.text
    ) or (
        "SELECT employees.employee_id, departments.department_id FROM employees"
        in caplog.text
    )


def test_lateral_join_without_condition(engine_testaccount, caplog):
    Base = declarative_base()

    class Employee(Base):
        __tablename__ = "Employee"

        pkey = Column(String, primary_key=True)
        uid = Column(Integer)
        content = Column(String)

    Base.metadata.create_all(engine_testaccount)
    lateral_table = func.flatten(
        func.PARSE_JSON(Employee.content), outer=False
    ).lateral()
    query = (
        select(
            Employee.uid,
        )
        .select_from(Employee)
        .join(lateral_table)
        .where(Employee.uid == "123")
    )
    session = Session(bind=engine_testaccount)
    with caplog.at_level(logging.DEBUG):
        session.execute(query)
    assert (
        '[SELECT "Employee".uid FROM "Employee" JOIN LATERAL flatten(PARSE_JSON("Employee"'
        in caplog.text
    )
