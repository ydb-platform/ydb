""" This module defines the :class:`geoalchemy2.types.Geometry`,
:class:`geoalchemy2.types.Geography`, and :class:`geoalchemy2.types.Raster`
classes, that are used when defining geometry, geography and raster
columns/properties in models.

Reference
---------
"""
import warnings

from sqlalchemy.types import UserDefinedType, Integer
from sqlalchemy.sql import func
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql.base import ischema_names

try:
    from .shape import to_shape
    SHAPELY = True
except ImportError:
    SHAPELY = False


from .comparator import BaseComparator, Comparator
from .elements import WKBElement, WKTElement, RasterElement, CompositeElement
from .exc import ArgumentError


class _GISType(UserDefinedType):
    """
    The base class for :class:`geoalchemy2.types.Geometry` and
    :class:`geoalchemy2.types.Geography`.

    This class defines ``bind_expression`` and ``column_expression`` methods
    that wrap column expressions in ``ST_GeomFromEWKT``, ``ST_GeogFromText``,
    or ``ST_AsEWKB`` calls.

    This class also defines ``result_processor`` and ``bind_processor``
    methods. The function returned by ``result_processor`` converts WKB values
    received from the database to :class:`geoalchemy2.elements.WKBElement`
    objects. The function returned by ``bind_processor`` converts
    :class:`geoalchemy2.elements.WKTElement` objects to EWKT strings.

    Constructor arguments:

    ``geometry_type``

        The geometry type.

        Possible values are:

          * ``"GEOMETRY"``,
          * ``"POINT"``,
          * ``"LINESTRING"``,
          * ``"POLYGON"``,
          * ``"MULTIPOINT"``,
          * ``"MULTILINESTRING"``,
          * ``"MULTIPOLYGON"``,
          * ``"GEOMETRYCOLLECTION"``,
          * ``"CURVE"``,
          * ``None``.

       The latter is actually not supported with
       :class:`geoalchemy2.types.Geography`.

       When set to ``None`` then no "geometry type" constraints will be
       attached to the geometry type declaration. Using ``None`` here
       is not compatible with setting ``management`` to ``True``.

       Default is ``"GEOMETRY"``.

    ``srid``

        The SRID for this column. E.g. 4326. Default is ``-1``.

    ``dimension``

        The dimension of the geometry. Default is ``2``.

        With ``management`` set to ``True``, that is when ``AddGeometryColumn`` is used
        to add the geometry column, there are two constraints:

        * The ``geometry_type`` must not end with ``"ZM"``.  This is due to PostGIS'
          ``AddGeometryColumn`` failing with ZM geometry types. Instead the "simple"
          geometry type (e.g. POINT rather POINTZM) should be used with ``dimension``
          set to ``4``.
        * When the ``geometry_type`` ends with ``"Z"`` or ``"M"`` then ``dimension``
          must be set to ``3``.

        With ``management`` set to ``False`` (the default) ``dimension`` is not
        taken into account, and the actual dimension is fully defined with the
        ``geometry_type``.

    ``spatial_index``

        Indicate if a spatial index should be created. Default is ``True``.

    ``use_N_D_index``
        Use the N-D index instead of the standard 2-D index.

    ``management``

        Indicate if the ``AddGeometryColumn`` and ``DropGeometryColumn``
        managements functions should be called when adding and dropping the
        geometry column. Should be set to ``True`` for PostGIS 1.x. Default is
        ``False``. Note that this option has no effect for
        :class:`geoalchemy2.types.Geography`.

    ``use_typmod``

        By default PostgreSQL type modifiers are used to create the geometry
        column. To use check constraints instead set ``use_typmod`` to
        ``False``. By default this option is not included in the call to
        ``AddGeometryColumn``. Note that this option is only taken
        into account if ``management`` is set to ``True`` and is only available
        for PostGIS 2.x.
    """

    name = None
    """ Name used for defining the main geo type (geometry or geography)
        in CREATE TABLE statements. Set in subclasses. """

    from_text = None
    """ The name of "from text" function for this type.
        Set in subclasses. """

    as_binary = None
    """ The name of the "as binary" function for this type.
        Set in subclasses. """

    comparator_factory = Comparator
    """ This is the way by which spatial operators are defined for
        geometry/geography columns. """

    def __init__(self, geometry_type='GEOMETRY', srid=-1, dimension=2,
                 spatial_index=True, use_N_D_index=False, management=False, use_typmod=None,
                 from_text=None, name=None, nullable=True):
        geometry_type, srid = self.check_ctor_args(
            geometry_type, srid, dimension, management, use_typmod, nullable)
        self.geometry_type = geometry_type
        self.srid = srid
        if name is not None:
            self.name = name
        if from_text is not None:
            self.from_text = from_text
        self.dimension = dimension
        self.spatial_index = spatial_index
        self.use_N_D_index = use_N_D_index
        self.management = management
        self.use_typmod = use_typmod
        self.extended = self.as_binary == 'ST_AsEWKB'
        self.nullable = nullable

    def get_col_spec(self):
        if not self.geometry_type:
            return self.name
        return '%s(%s,%d)' % (self.name, self.geometry_type, self.srid)

    def column_expression(self, col):
        """Specific column_expression that automatically adds a conversion function"""
        return getattr(func, self.as_binary)(col, type_=self)

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                kwargs = {}
                if self.srid > 0:
                    kwargs['srid'] = self.srid
                if self.extended is not None:
                    kwargs['extended'] = self.extended
                return self.ElementType(value, **kwargs)
        return process

    def bind_expression(self, bindvalue):
        """Specific bind_expression that automatically adds a conversion function"""
        return getattr(func, self.from_text)(bindvalue, type_=self)

    def bind_processor(self, dialect):
        def process(bindvalue):
            if isinstance(bindvalue, WKTElement):
                if bindvalue.extended:
                    return '%s' % (bindvalue.data)
                else:
                    return 'SRID=%d;%s' % (bindvalue.srid, bindvalue.data)
            elif isinstance(bindvalue, WKBElement):
                if dialect.name == 'sqlite' or not bindvalue.extended:
                    # With SpatiaLite or when the WKBElement includes a WKB value rather
                    # than a EWKB value we use Shapely to convert the WKBElement to an
                    # EWKT string
                    if not SHAPELY:
                        raise ArgumentError('Shapely is required for handling WKBElement bind '
                                            'values when using SpatiaLite or when the bind value '
                                            'is a WKB rather than an EWKB')
                    shape = to_shape(bindvalue)
                    return 'SRID=%d;%s' % (bindvalue.srid, shape.wkt)
                else:
                    # PostGIS ST_GeomFromEWKT works with EWKT strings as well
                    # as EWKB hex strings
                    return bindvalue.desc
            elif isinstance(bindvalue, RasterElement):
                return '%s' % (bindvalue.data)
            else:
                return bindvalue
        return process

    @staticmethod
    def check_ctor_args(geometry_type, srid, dimension, management, use_typmod, nullable):
        try:
            srid = int(srid)
        except ValueError:
            raise ArgumentError('srid must be convertible to an integer')
        if geometry_type:
            geometry_type = geometry_type.upper()
            if management:
                if geometry_type.endswith('ZM'):
                    # PostGIS' AddGeometryColumn does not work with ZM geometry types. Instead
                    # the simple geometry type (e.g. POINT rather POINTZM) should be used with
                    # dimension set to 4
                    raise ArgumentError(
                        'with management=True use geometry_type={!r} and '
                        'dimension=4 for {!r} geometries'.format(geometry_type[:-2], geometry_type))
                elif geometry_type[-1] in ('Z', 'M') and dimension != 3:
                    # If a Z or M geometry type is used then dimension must be set to 3
                    raise ArgumentError(
                        'with management=True dimension must be 3 for '
                        '{!r} geometries'.format(geometry_type))
        else:
            if management:
                raise ArgumentError('geometry_type set to None not compatible '
                                    'with management')
            if srid > 0:
                warnings.warn('srid not enforced when geometry_type is None')

        if use_typmod and not management:
            warnings.warn('use_typmod ignored when management is False')
        if use_typmod is not None and not nullable:
            raise ArgumentError(
                'The "nullable" and "use_typmod" arguments can not be used together'
            )

        return geometry_type, srid


class Geometry(_GISType):
    """
    The Geometry type.

    Creating a geometry column is done like this::

        Column(Geometry(geometry_type='POINT', srid=4326))

    See :class:`geoalchemy2.types._GISType` for the list of arguments that can
    be passed to the constructor.

    If ``srid`` is set then the ``WKBElement`` objects resulting from queries will
    have that SRID, and, when constructing the ``WKBElement`` objects, the SRID
    won't be read from the data returned by the database. If ``srid`` is not set
    (meaning it's ``-1``) then the SRID set in ``WKBElement`` objects will be read
    from the data returned by the database.
    """

    name = 'geometry'
    """ Type name used for defining geometry columns in ``CREATE TABLE``. """

    from_text = 'ST_GeomFromEWKT'
    """ The "from text" geometry constructor. Used by the parent class'
        ``bind_expression`` method. """

    as_binary = 'ST_AsEWKB'
    """ The "as binary" function to use. Used by the parent class'
        ``column_expression`` method. """

    ElementType = WKBElement
    """ The element class to use. Used by the parent class'
        ``result_processor`` method. """


class Geography(_GISType):
    """
    The Geography type.

    Creating a geography column is done like this::

        Column(Geography(geometry_type='POINT', srid=4326))

    See :class:`geoalchemy2.types._GISType` for the list of arguments that can
    be passed to the constructor.

    """

    name = 'geography'
    """ Type name used for defining geography columns in ``CREATE TABLE``. """

    from_text = 'ST_GeogFromText'
    """ The ``FromText`` geography constructor. Used by the parent class'
        ``bind_expression`` method. """

    as_binary = 'ST_AsBinary'
    """ The "as binary" function to use. Used by the parent class'
        ``column_expression`` method. """

    ElementType = WKBElement
    """ The element class to use. Used by the parent class'
        ``result_processor`` method. """


class Raster(_GISType):
    """
    The Raster column type.

    Creating a raster column is done like this::

        Column(Raster)

    This class defines the ``result_processor`` method, so that raster values
    received from the database are converted to
    :class:`geoalchemy2.elements.RasterElement` objects.

    Constructor arguments:

    ``spatial_index``

        Indicate if a spatial index should be created. Default is ``True``.

    """

    comparator_factory = BaseComparator
    """
    This is the way by which spatial operators and functions are
    defined for raster columns.
    """

    name = 'raster'
    """ Type name used for defining raster columns in ``CREATE TABLE``. """

    from_text = 'raster'
    """ The "from text" raster constructor. Used by the parent class'
        ``bind_expression`` method. """

    as_binary = 'raster'
    """ The "as binary" function to use. Used by the parent class'
        ``column_expression`` method. """

    ElementType = RasterElement
    """ The element class to use. Used by the parent class'
        ``result_processor`` method. """

    def __init__(self, *args, **kwargs):
        # Enforce default values
        kwargs['geometry_type'] = None
        kwargs['srid'] = -1
        super(Raster, self).__init__(*args, **kwargs)
        self.extended = None


class CompositeType(UserDefinedType):
    """
    A wrapper for :class:`geoalchemy2.elements.CompositeElement`, that can be
    used as the return type in PostgreSQL functions that return composite
    values.

    This is used as the base class of :class:`geoalchemy2.types.GeometryDump`.
    """

    typemap = {}
    """ Dictionary used for defining the content types and their
        corresponding keys. Set in subclasses. """

    class comparator_factory(UserDefinedType.Comparator):
        def __getattr__(self, key):
            try:
                type_ = self.type.typemap[key]
            except KeyError:
                raise KeyError("Type '%s' doesn't have an attribute: '%s'"
                               % (self.type, key))

            return CompositeElement(self.expr, key, type_)


class GeometryDump(CompositeType):
    """
    The return type for functions like ``ST_Dump``, consisting of a path and
    a geom field. You should normally never use this class directly.
    """

    typemap = {'path': postgresql.ARRAY(Integer), 'geom': Geometry}
    """ Dictionary defining the contents of a ``geometry_dump``. """


# Register Geometry, Geography and Raster to SQLAlchemy's Postgres reflection
# subsystem.
ischema_names['geometry'] = Geometry
ischema_names['geography'] = Geography
ischema_names['raster'] = Raster
