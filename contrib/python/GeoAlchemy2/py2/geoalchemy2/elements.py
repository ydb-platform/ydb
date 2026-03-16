import binascii
import struct

try:
    from sqlalchemy.sql import functions
    from sqlalchemy.sql.functions import FunctionElement
except ImportError:  # SQLA < 0.9  # pragma: no cover
    from sqlalchemy.sql import expression as functions
    from sqlalchemy.sql.expression import FunctionElement
from sqlalchemy.types import to_instance
from sqlalchemy.ext.compiler import compiles

from .compat import PY3, str as str_
from .exc import ArgumentError


if PY3:
    BinasciiError = binascii.Error
else:
    BinasciiError = TypeError


function_registry = set()


class HasFunction(object):
    pass


class _SpatialElement(HasFunction):
    """
    The base class for :class:`geoalchemy2.elements.WKTElement` and
    :class:`geoalchemy2.elements.WKBElement`.

    The first argument passed to the constructor is the data wrapped
    by the ``_SpatialElement` object being constructed.

    Additional arguments:

    ``srid``

        An integer representing the spatial reference system. E.g. 4326.
        Default value is -1, which means no/unknown reference system.

    ``extended``

        A boolean indicating whether the extended format (EWKT or EWKB)
        is used. Default is ``False``.

    """

    def __init__(self, data, srid=-1, extended=False):
        self.srid = srid
        self.data = data
        self.extended = extended

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %s>" % \
            (self.__class__.__name__, id(self), self)  # pragma: no cover

    def __eq__(self, other):
        try:
            return (
                self.extended == other.extended
                and self.srid == other.srid
                and self.desc == other.desc
            )
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.desc, self.srid, self.extended))

    def __getattr__(self, name):
        #
        # This is how things like lake.geom.ST_Buffer(2) creates
        # SQL expressions of this form:
        #
        # ST_Buffer(ST_GeomFromWKB(:ST_GeomFromWKB_1), :param_1)
        #

        # Raise an AttributeError when the attribute name doesn't start
        # with st_. This is to be nice with other librairies that use
        # some ducktyping (e.g. hasattr(element, "copy")) to determine
        # the type of the element.

        if name.lower() not in function_registry:
            raise AttributeError

        # We create our own _FunctionGenerator here, and use it in place of
        # SQLAlchemy's "func" object. This is to be able to "bind" the
        # function to the SQL expression. See also GenericFunction above.
        func_ = functions._FunctionGenerator(expr=self)
        return getattr(func_, name)

    def __getstate__(self):
        state = {
            'srid': self.srid,
            'data': str(self),
            'extended': self.extended,
        }
        return state

    def __setstate__(self, state):
        self.srid = state['srid']
        self.extended = state['extended']
        self.data = self._data_from_desc(state['data'])

    @staticmethod
    def _data_from_desc(desc):
        raise NotImplementedError()


class WKTElement(_SpatialElement):
    """
    Instances of this class wrap a WKT or EWKT value.

    Usage examples::

        wkt_element_1 = WKTElement('POINT(5 45)')
        wkt_element_2 = WKTElement('POINT(5 45)', srid=4326)
        wkt_element_3 = WKTElement('SRID=4326;POINT(5 45)', extended=True)
    """

    geom_from = 'ST_GeomFromText'
    geom_from_extended_version = 'ST_GeomFromEWKT'

    def __init__(self, data, srid=-1, extended=False):
        if extended and srid == -1:
            # read srid from EWKT
            if not data.startswith('SRID='):
                raise ArgumentError('invalid EWKT string {}'.format(data))
            data_s = data.split(';', 1)
            if len(data_s) != 2:
                raise ArgumentError('invalid EWKT string {}'.format(data))
            header = data_s[0]
            try:
                srid = int(header[5:])
            except ValueError:
                raise ArgumentError('invalid EWKT string {}'.format(data))
        _SpatialElement.__init__(self, data, srid, extended)

    @property
    def desc(self):
        """
        This element's description string.
        """
        return self.data

    @staticmethod
    def _data_from_desc(desc):
        return desc


class WKBElement(_SpatialElement):
    """
    Instances of this class wrap a WKB or EWKB value.

    Geometry values read from the database are converted to instances of this
    type. In most cases you won't need to create ``WKBElement`` instances
    yourself.

    If ``extended`` is ``True`` and ``srid`` is ``-1`` at construction time
    then the SRID will be read from the EWKB data.

    Note: you can create ``WKBElement`` objects from Shapely geometries
    using the :func:`geoalchemy2.shape.from_shape` function.
    """

    geom_from = 'ST_GeomFromWKB'
    geom_from_extended_version = 'ST_GeomFromEWKB'

    def __init__(self, data, srid=-1, extended=False):
        if extended and srid == -1:
            # read srid from the EWKB
            #
            # WKB struct {
            #    byte    byteOrder;
            #    uint32  wkbType;
            #    uint32  SRID;
            #    struct  geometry;
            # }
            # byteOrder enum {
            #     WKB_XDR = 0,  // Most Significant Byte First
            #     WKB_NDR = 1,  // Least Significant Byte First
            # }
            if isinstance(data, str_):
                # SpatiaLite case
                # assume that the string is an hex value
                header = binascii.unhexlify(data[:18])
            else:
                header = data[:9]
            if not PY3:
                header = bytearray(header)
            byte_order, srid = header[0], header[5:]
            srid = struct.unpack('<I' if byte_order else '>I', srid)[0]
        _SpatialElement.__init__(self, data, srid, extended)

    @property
    def desc(self):
        """
        This element's description string.
        """
        if isinstance(self.data, str_):
            # SpatiaLite case
            return self.data
        desc = binascii.hexlify(self.data)
        if PY3:
            # hexlify returns a bytes object on py3
            desc = str(desc, encoding="utf-8")
        return desc

    @staticmethod
    def _data_from_desc(desc):
        if PY3:
            desc = desc.encode(encoding="utf-8")
        return binascii.unhexlify(desc)


class RasterElement(_SpatialElement):
    """
    Instances of this class wrap a ``raster`` value. Raster values read
    from the database are converted to instances of this type. In
    most cases you won't need to create ``RasterElement`` instances
    yourself.
    """

    geom_from_extended_version = 'raster'

    def __init__(self, data):
        # read srid from the WKB (binary or hexadecimal format)
        # The WKB structure is documented in the file
        # raster/doc/RFC2-WellKnownBinaryFormat of the PostGIS sources.
        try:
            bin_data = binascii.unhexlify(data[:114])
        except BinasciiError:
            bin_data = data
            data = str(binascii.hexlify(data).decode(encoding='utf-8'))
        byte_order = bin_data[0]
        srid = bin_data[53:57]
        if not PY3:
            byte_order = bytearray(byte_order)[0]
        srid = struct.unpack('<I' if byte_order else '>I', srid)[0]
        _SpatialElement.__init__(self, data, srid, True)

    @property
    def desc(self):
        """
        This element's description string.
        """
        return self.data

    @staticmethod
    def _data_from_desc(desc):
        return desc


class CompositeElement(FunctionElement):
    """
    Instances of this class wrap a Postgres composite type.
    """

    def __init__(self, base, field, type_):
        self.name = field
        self.type = to_instance(type_)

        super(CompositeElement, self).__init__(base)


@compiles(CompositeElement)
def _compile_pgelem(expr, compiler, **kw):
    return '(%s).%s' % (compiler.process(expr.clauses, **kw), expr.name)
