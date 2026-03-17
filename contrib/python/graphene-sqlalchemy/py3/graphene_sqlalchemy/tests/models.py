from __future__ import absolute_import

import datetime
import enum
from decimal import Decimal
from typing import List, Optional, Tuple

from sqlalchemy import (Column, Date, Enum, ForeignKey, Integer, Numeric,
                        String, Table, func, select)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import column_property, composite, mapper, relationship

PetKind = Enum("cat", "dog", name="pet_kind")


class HairKind(enum.Enum):
    LONG = 'long'
    SHORT = 'short'


Base = declarative_base()

association_table = Table(
    "association",
    Base.metadata,
    Column("pet_id", Integer, ForeignKey("pets.id")),
    Column("reporter_id", Integer, ForeignKey("reporters.id")),
)


class Editor(Base):
    __tablename__ = "editors"
    editor_id = Column(Integer(), primary_key=True)
    name = Column(String(100))


class Pet(Base):
    __tablename__ = "pets"
    id = Column(Integer(), primary_key=True)
    name = Column(String(30))
    pet_kind = Column(PetKind, nullable=False)
    hair_kind = Column(Enum(HairKind, name="hair_kind"), nullable=False)
    reporter_id = Column(Integer(), ForeignKey("reporters.id"))


class CompositeFullName(object):
    def __init__(self, first_name, last_name):
        self.first_name = first_name
        self.last_name = last_name

    def __composite_values__(self):
        return self.first_name, self.last_name

    def __repr__(self):
        return "{} {}".format(self.first_name, self.last_name)


class Reporter(Base):
    __tablename__ = "reporters"

    id = Column(Integer(), primary_key=True)
    first_name = Column(String(30), doc="First name")
    last_name = Column(String(30), doc="Last name")
    email = Column(String(), doc="Email")
    favorite_pet_kind = Column(PetKind)
    pets = relationship("Pet", secondary=association_table, backref="reporters", order_by="Pet.id")
    articles = relationship("Article", backref="reporter")
    favorite_article = relationship("Article", uselist=False)

    @hybrid_property
    def hybrid_prop_with_doc(self):
        """Docstring test"""
        return self.first_name

    @hybrid_property
    def hybrid_prop(self):
        return self.first_name

    @hybrid_property
    def hybrid_prop_str(self) -> str:
        return self.first_name

    @hybrid_property
    def hybrid_prop_int(self) -> int:
        return 42

    @hybrid_property
    def hybrid_prop_float(self) -> float:
        return 42.3

    @hybrid_property
    def hybrid_prop_bool(self) -> bool:
        return True

    @hybrid_property
    def hybrid_prop_list(self) -> List[int]:
        return [1, 2, 3]

    column_prop = column_property(
        select([func.cast(func.count(id), Integer)]), doc="Column property"
    )

    composite_prop = composite(CompositeFullName, first_name, last_name, doc="Composite")


class Article(Base):
    __tablename__ = "articles"
    id = Column(Integer(), primary_key=True)
    headline = Column(String(100))
    pub_date = Column(Date())
    reporter_id = Column(Integer(), ForeignKey("reporters.id"))


class ReflectedEditor(type):
    """Same as Editor, but using reflected table."""

    @classmethod
    def __subclasses__(cls):
        return []


editor_table = Table("editors", Base.metadata, autoload=True)

mapper(ReflectedEditor, editor_table)


############################################
# The models below are mainly used in the
# @hybrid_property type inference scenarios
############################################


class ShoppingCartItem(Base):
    __tablename__ = "shopping_cart_items"

    id = Column(Integer(), primary_key=True)

    @hybrid_property
    def hybrid_prop_shopping_cart(self) -> List['ShoppingCart']:
        return [ShoppingCart(id=1)]


class ShoppingCart(Base):
    __tablename__ = "shopping_carts"

    id = Column(Integer(), primary_key=True)

    # Standard Library types

    @hybrid_property
    def hybrid_prop_str(self) -> str:
        return self.first_name

    @hybrid_property
    def hybrid_prop_int(self) -> int:
        return 42

    @hybrid_property
    def hybrid_prop_float(self) -> float:
        return 42.3

    @hybrid_property
    def hybrid_prop_bool(self) -> bool:
        return True

    @hybrid_property
    def hybrid_prop_decimal(self) -> Decimal:
        return Decimal("3.14")

    @hybrid_property
    def hybrid_prop_date(self) -> datetime.date:
        return datetime.datetime.now().date()

    @hybrid_property
    def hybrid_prop_time(self) -> datetime.time:
        return datetime.datetime.now().time()

    @hybrid_property
    def hybrid_prop_datetime(self) -> datetime.datetime:
        return datetime.datetime.now()

    # Lists and Nested Lists

    @hybrid_property
    def hybrid_prop_list_int(self) -> List[int]:
        return [1, 2, 3]

    @hybrid_property
    def hybrid_prop_list_date(self) -> List[datetime.date]:
        return [self.hybrid_prop_date, self.hybrid_prop_date, self.hybrid_prop_date]

    @hybrid_property
    def hybrid_prop_nested_list_int(self) -> List[List[int]]:
        return [self.hybrid_prop_list_int, ]

    @hybrid_property
    def hybrid_prop_deeply_nested_list_int(self) -> List[List[List[int]]]:
        return [[self.hybrid_prop_list_int, ], ]

    # Other SQLAlchemy Instances
    @hybrid_property
    def hybrid_prop_first_shopping_cart_item(self) -> ShoppingCartItem:
        return ShoppingCartItem(id=1)

    # Other SQLAlchemy Instances
    @hybrid_property
    def hybrid_prop_shopping_cart_item_list(self) -> List[ShoppingCartItem]:
        return [ShoppingCartItem(id=1), ShoppingCartItem(id=2)]

    # Unsupported Type
    @hybrid_property
    def hybrid_prop_unsupported_type_tuple(self) -> Tuple[str, str]:
        return "this will actually", "be a string"

    # Self-references

    @hybrid_property
    def hybrid_prop_self_referential(self) -> 'ShoppingCart':
        return ShoppingCart(id=1)

    @hybrid_property
    def hybrid_prop_self_referential_list(self) -> List['ShoppingCart']:
        return [ShoppingCart(id=1)]

    # Optional[T]

    @hybrid_property
    def hybrid_prop_optional_self_referential(self) -> Optional['ShoppingCart']:
        return None


class KeyedModel(Base):
    __tablename__ = "test330"
    id = Column(Integer(), primary_key=True)
    reporter_number = Column("% reporter_number", Numeric, key="reporter_number")
