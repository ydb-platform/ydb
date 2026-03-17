"""This module defines the internals to map the spatial functions to the spatial columns.

This module defines the :class:`GenericFunction` class, which is the base for
the implementation of spatial functions in GeoAlchemy.  This module is also
where actual spatial functions are defined. Spatial functions supported by
GeoAlchemy are defined in this module. See :class:`GenericFunction` to know how
to create new spatial functions.

.. note::

    By convention the names of spatial functions are prefixed by ``ST_``.  This
    is to be consistent with PostGIS', which itself is based on the ``SQL-MM``
    standard.

Functions created by subclassing :class:`GenericFunction` can be called
in several ways:

* By using the ``func`` object, which is the SQLAlchemy standard way of calling
  a function. For example, without the ORM::

      select([func.ST_Area(lake_table.c.geom)])

  and with the ORM::

      Session.query(func.ST_Area(Lake.geom))

* By applying the function to a geometry column. For example, without the
  ORM::

      select([lake_table.c.geom.ST_Area()])

  and with the ORM::

      Session.query(Lake.geom.ST_Area())

* By applying the function to a :class:`geoalchemy2.elements.WKBElement`
  object (:class:`geoalchemy2.elements.WKBElement` is the type into
  which GeoAlchemy converts geometry values read from the database), or
  to a :class:`geoalchemy2.elements.WKTElement` object. For example,
  without the ORM::

      conn.scalar(lake['geom'].ST_Area())

  and with the ORM::

      session.scalar(lake.geom.ST_Area())

.. warning::

    A few functions (like `ST_Transform()`, `ST_Union()`, `ST_SnapToGrid()`, ...) can be used on
    several spatial types (:class:`geoalchemy2.types.Geometry`,
    :class:`geoalchemy2.types.Geography` and / or :class:`geoalchemy2.types.Raster` types). In
    GeoAlchemy2, these functions are only defined for the :class:`geoalchemy2.types.Geometry` type,
    as it can not be defined for several types at the same time. Therefore, using these functions on
    :class:`geoalchemy2.types.Geography` or :class:`geoalchemy2.types.Raster` requires minor
    tweaking to enforce the type by passing the `type_=Geography` or `type_=Raster` argument to the
    function::

        s = select([func.ST_Transform(
                            lake_table.c.raster,
                            2154,
                            type_=Raster)
                        .label('transformed_raster')])

Reference
---------

"""

import re
from typing import List
from typing import Type

from sqlalchemy import inspect
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import annotation
from sqlalchemy.sql import functions
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.selectable import FromClause

from geoalchemy2 import elements
from geoalchemy2._functions import _FUNCTIONS
from geoalchemy2._functions_helpers import _get_docstring

_GeoFunctionBase: Type[functions.GenericFunction]
_GeoFunctionParent: Type[functions.GenericFunction]
try:
    # SQLAlchemy < 2

    from sqlalchemy.sql.functions import _GenericMeta  # type: ignore
    from sqlalchemy.util import with_metaclass  # type: ignore

    class _GeoGenericMeta(_GenericMeta):
        """Extend the registering mechanism of sqlalchemy.

        The spatial functions are registered in a specific registry for geoalchemy2.
        """

        _register = False

        def __init__(cls, clsname, bases, clsdict) -> None:
            # Register the function
            elements.function_registry.add(clsname.lower())

            super(_GeoGenericMeta, cls).__init__(clsname, bases, clsdict)

    _GeoFunctionBase = with_metaclass(_GeoGenericMeta, functions.GenericFunction)
    _GeoFunctionParent = functions.GenericFunction
except ImportError:
    # SQLAlchemy >= 2

    class GeoGenericFunction(functions.GenericFunction):
        def __init_subclass__(cls) -> None:
            if annotation.Annotated not in cls.__mro__:
                cls._register_geo_function(cls.__name__, cls.__dict__)
            super().__init_subclass__()

        @classmethod
        def _register_geo_function(cls, clsname, clsdict) -> None:
            # Check _register attribute status
            cls._register = getattr(cls, "_register", True)

            # Register the function if required
            if cls._register:
                elements.function_registry.add(clsname.lower())
            else:
                # Set _register to True to register child classes by default
                cls._register = True

    _GeoFunctionBase = GeoGenericFunction
    _GeoFunctionParent = GeoGenericFunction


class TableRowElement(ColumnElement):
    inherit_cache: bool = False
    """The cache is disabled for this class."""

    def __init__(self, selectable: FromClause) -> None:
        self.selectable = selectable

    @property
    def _from_objects(self) -> List[FromClause]:
        return [self.selectable]


class ST_AsGeoJSON(_GeoFunctionBase):  # type: ignore
    """Special process for the ST_AsGeoJSON() function.

    This is to be able to work with its feature version introduced in PostGIS 3.
    """

    name: str = "ST_AsGeoJSON"
    inherit_cache: bool = True
    """The cache is enabled for this class."""

    def __init__(self, *args, **kwargs) -> None:
        expr = kwargs.pop("expr", None)
        args_list = list(args)
        if expr is not None:
            args_list = [expr] + args_list
        for idx, element in enumerate(args_list):
            if isinstance(element, functions.Function):
                continue
            elif isinstance(element, elements._SpatialElement):
                if element.extended:
                    func_name = element.geom_from_extended_version
                    func_args = [element.data]
                else:
                    func_name = element.geom_from
                    func_args = [element.data, element.srid]
                args_list[idx] = getattr(functions.func, func_name)(*func_args)
            else:
                try:
                    insp = inspect(element)
                    if hasattr(insp, "selectable"):
                        args_list[idx] = TableRowElement(insp.selectable)
                except Exception:
                    continue

        _GeoFunctionParent.__init__(self, *args_list, **kwargs)

    __doc__ = (
        'Return the geometry as a GeoJSON "geometry" object, or the row as a '
        'GeoJSON feature" object (PostGIS 3 only). (Cf GeoJSON specifications RFC '
        "7946). 2D and 3D Geometries are both supported. GeoJSON only support SFS "
        "1.1 geometry types (no curve support for example). "
        "See https://postgis.net/docs/ST_AsGeoJSON.html"
    )


@compiles(TableRowElement)
def _compile_table_row_thing(element, compiler, **kw):
    # In order to get a name as reliably as possible, noting that some
    # SQL compilers don't say "table AS name" and might not have the "AS",
    # table and alias names can have spaces in them, etc., get it from
    # a column instead because that's what we want to be showing here anyway.

    compiled = compiler.process(list(element.selectable.columns)[0], **kw)

    # 1. check for exact name of the selectable is here, use that.
    # This way if it has dots and spaces and anything else in it, we
    # can get it w/ correct quoting
    schema = getattr(element.selectable, "schema", "")
    name = element.selectable.name
    pattern = r"(.?%s.?\.)?(.?%s.?)\." % (schema, name)
    m = re.match(pattern, compiled)
    if m:
        return m.group(2)

    # 2. just split on the dot, assume anonymized name
    return compiled.split(".")[0]


class GenericFunction(_GeoFunctionBase):  # type: ignore
    """The base class for GeoAlchemy functions.

    This class inherits from ``sqlalchemy.sql.functions.GenericFunction``, so
    functions defined by subclassing this class can be given a fixed return
    type. For example, functions like :class:`ST_Buffer` and
    :class:`ST_Envelope` have their ``type`` attributes set to
    :class:`geoalchemy2.types.Geometry`.

    This class allows constructs like ``Lake.geom.ST_Buffer(2)``. In that
    case the ``Function`` instance is bound to an expression (``Lake.geom``
    here), and that expression is passed to the function when the function
    is actually called.

    If you need to use a function that GeoAlchemy does not provide you will
    certainly want to subclass this class. For example, if you need the
    ``ST_TransScale`` spatial function, which isn't (currently) natively
    supported by GeoAlchemy, you will write this::

        from geoalchemy2 import Geometry
        from geoalchemy2.functions import GenericFunction

        class ST_TransScale(GenericFunction):
            name = 'ST_TransScale'
            type = Geometry
    """

    # Set _register to False in order not to register this class in
    # sqlalchemy.sql.functions._registry. Only its children will be registered.
    _register = False

    def __init__(self, *args, **kwargs) -> None:
        expr = kwargs.pop("expr", None)
        args_list = list(args)
        if expr is not None:
            args_list = [expr] + args_list
        for idx, elem in enumerate(args_list):
            if isinstance(elem, elements._SpatialElement):
                if elem.extended:
                    func_name = elem.geom_from_extended_version
                    func_args = [elem.data]
                else:
                    func_name = elem.geom_from
                    func_args = [elem.data, elem.srid]
                args_list[idx] = getattr(functions.func, func_name)(*func_args)
        _GeoFunctionParent.__init__(self, *args_list, **kwargs)


__all__ = [
    "GenericFunction",
    "ST_AsGeoJSON",
    "TableRowElement",
]


def _create_dynamic_functions() -> None:
    # Iterate through _FUNCTIONS and create GenericFunction classes dynamically
    for name, type_, doc in _FUNCTIONS:
        attributes = {
            "name": name,
            "inherit_cache": True,
            "__doc__": _get_docstring(name, doc, type_),
        }

        if type_ is not None:
            attributes["type"] = type_

        globals()[name] = type(name, (GenericFunction,), attributes)
        __all__.append(name)


_create_dynamic_functions()


def __dir__() -> List[str]:
    return __all__
