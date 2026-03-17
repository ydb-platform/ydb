"""
The parser:
1. gets and expression
2. parses it
3. handles all boolean logic
4. delegates operator and rvalue parsing to the OperatorMap

SchemaFreeOperatorMap

  supports all mongo operators for all fields.

SchemaAwareOperatorMap

  1. verifies fields exist.
  2. verifies operators are applied to fields of correct type.

currently unsupported:
1. $where - kind of intentionally against injections
2. geospatial
"""
import ast
import bson
import datetime
import dateutil.parser
from calendar import timegm


def parse_date(node):
    if hasattr(node, 'n'): # it's a number!
        return datetime.datetime.fromtimestamp(node.n)
    try:
        return dateutil.parser.parse(node.s)
    except Exception as e:
        raise ParseError('Error parsing date: ' + str(e), col_offset=node.col_offset)

class AstHandler(object):

    def get_options(self):
        return [f.replace('handle_', '') for f in dir(self) if f.startswith('handle_')]

    def resolve(self, thing):
        thing_name = thing.__class__.__name__
        try:
            handler = getattr(self, 'handle_' + thing_name)
        except AttributeError:
            raise ParseError('Unsupported syntax ({0}).'.format(thing_name,
                                                              self.get_options()),
                             col_offset=thing.col_offset if hasattr(thing, 'col_offset') else None,
                             options=self.get_options())
        return handler

    def handle(self, thing):
        return self.resolve(thing)(thing)

    def parse(self, string):
        ex = ast.parse(string, mode='eval')
        return self.handle(ex.body)

class ParseError(Exception):
    def __init__(self, message, col_offset, options=[]):
        super(ParseError, self).__init__(message)
        self.message = message
        self.col_offset = col_offset
        self.options = options
    def __str__(self):
        if self.options:
            return '{0} options: {1}'.format(self.message, self.options)
        return self.message

class Parser(AstHandler):
    def __init__(self, operator_map):
        self._operator_map = operator_map

    def get_options(self):
        return self._operator_map.get_options()

    def handle_BoolOp(self, op):
        return {self.handle(op.op): list(map(self.handle, op.values))}

    def handle_And(self, op):
        '''and'''
        return '$and'

    def handle_Or(self, op):
        '''or'''
        return '$or'

    def handle_UnaryOp(self, op):
        operator = self.handle(op.operand)
        field, value = list(operator.items())[0]
        return {field: {self.handle(op.op): value}}

    def handle_Not(self, not_node):
        '''not'''
        return '$not'

    def handle_Compare(self, compare):
        if len(compare.comparators) != 1:
            raise ParseError('Invalid number of comparators: {0}'.format(len(compare.comparators)),
                             col_offset=compare.comparators[1].col_offset)
        return self._operator_map.handle(left=compare.left,
                                         operator=compare.ops[0],
                                         right=compare.comparators[0])

class SchemaFreeParser(Parser):
    def __init__(self):
        super(SchemaFreeParser, self).__init__(SchemaFreeOperatorMap())

class SchemaAwareParser(Parser):
    def __init__(self, *a, **k):
        super(SchemaAwareParser, self).__init__(SchemaAwareOperatorMap(*a, **k))

class FieldName(AstHandler):
    def handle_Str(self, node):
        return node.s
    def handle_Name(self, name):
        return name.id
    def handle_Attribute(self, attr):
        return '{0}.{1}'.format(self.handle(attr.value), attr.attr)

class OperatorMap(object):
    def resolve_field(self, node):
        return FieldName().handle(node)
    def handle(self, operator, left, right):
        field = self.resolve_field(left)
        return {field: self.resolve_type(field).handle_operator_and_right(operator, right)}

class SchemaFreeOperatorMap(OperatorMap):
    def get_options(self):
        return None
    def resolve_type(self, field):
        return GenericField()

class SchemaAwareOperatorMap(OperatorMap):
    def __init__(self, field_to_type):
        self._field_to_type = field_to_type
    def resolve_field(self, node):
        field = super(SchemaAwareOperatorMap, self).resolve_field(node)
        try:
            self._field_to_type[field]
        except KeyError:
            raise ParseError('Field not found: {0}.'.format(field),
                             col_offset=node.col_offset,
                             options=self._field_to_type.keys())
        return field

    def resolve_type(self, field):
        return self._field_to_type[field]

#---Function-Handlers---#

class Func(AstHandler):

    @staticmethod
    def get_arg(node, index):
        if index > len(node.args) - 1:
            raise ParseError('Missing argument in {0}.'.format(node.func.id),
                             col_offset=node.col_offset)
        return node.args[index]

    @staticmethod
    def parse_arg(node, index, field):
        return field.handle(Func.get_arg(node, index))

    def handle(self, node):
        try:
            handler = getattr(self, 'handle_' + node.func.id)
        except AttributeError:
            raise ParseError('Unsupported function ({0}).'.format(node.func.id),
                             col_offset=node.col_offset,
                             options=self.get_options())
        return handler(node)

    def handle_exists(self, node):
        return {'$exists': self.parse_arg(node, 0, BoolField())}

    def handle_type(self, node):
        return {'$type': self.parse_arg(node, 0, IntField())}

class StringFunc(Func):
    def handle_regex(self, node):
        result = {'$regex': self.parse_arg(node, 0, StringField())}
        try:
            result['$options'] = self.parse_arg(node, 1, StringField())
        except ParseError:
            pass
        return result

class IntFunc(Func):
    def handle_mod(self, node):
        return {'$mod': [self.parse_arg(node, 0, IntField()),
                         self.parse_arg(node, 1, IntField())]}

class ListFunc(Func):
    def handle_size(self, node):
        return {'$size': self.parse_arg(node, 0, IntField())}

    def handle_all(self, node):
        return {'$all': self.parse_arg(node, 0, ListField())}

    def handle_match(self, node):
        return {'$elemMatch': self.parse_arg(node, 0, DictField())}

class DateTimeFunc(Func):
    def handle_date(self, node):
        return parse_date(self.get_arg(node, 0))

class IdFunc(Func):
    def handle_id(self, node):
        return self.parse_arg(node, 0, IdField())

class EpochFunc(Func):
    def handle_epoch(self, node):
        return self.parse_arg(node, 0, EpochField())

class EpochUTCFunc(Func):
    def handle_epoch_utc(self, node):
        return self.parse_arg(node, 0, EpochUTCField())

class GeoShapeFuncParser(Func):

    def handle_Point(self, node):
        return {'$geometry':
                {'type': 'Point',
                 'coordinates': [self.parse_arg(node, 0, IntField()),
                                                  self.parse_arg(node, 1, IntField())]}}

    def handle_LineString(self, node):
        return {'$geometry':
                {'type': 'LineString',
                 'coordinates': self.parse_arg(node, 0, ListField(ListField(IntField())))}}

    def handle_Polygon(self, node):
        return {'$geometry':
                {'type': 'Polygon',
                'coordinates': self.parse_arg(node, 0, ListField(ListField(ListField(IntField()))))}}

    def handle_box(self, node):
        return {'$box': self.parse_arg(node, 0, ListField(ListField(IntField())))}

    def handle_polygon(self, node):
        return {'$polygon': self.parse_arg(node, 0, ListField(ListField(IntField())))}

    def _any_center(self, node, center_name):
        return {center_name: [self.parse_arg(node, 0, ListField(IntField())),
                              self.parse_arg(node, 1, IntField())]}

    def handle_center(self, node):
        return self._any_center(node, '$center')

    def handle_centerSphere(self, node):
        return self._any_center(node, '$centerSphere')

class GeoShapeParser(AstHandler):
    def handle_Call(self, node):
        return GeoShapeFuncParser().handle(node)
    def handle_List(self, node):
        '''
        This is a legacy coordinate pair. consider supporting box, polygon, center, centerSphere
        '''
        return ListField(IntField()).handle(node)

class GeoFunc(Func):
    def _any_near(self, node, near_name):
        shape = GeoShapeParser().handle(self.get_arg(node, 0))
        result = bson.SON({near_name: shape}) # use SON because mongo expects the command before the arguments
        if len(node.args) > 1:
            distance = self.parse_arg(node, 1, IntField()) # meters
            if isinstance(shape, list): # legacy coordinate pair
                result['$maxDistance'] = distance
            else:
                shape['$maxDistance'] = distance
        return result

    def handle_near(self, node):
        return self._any_near(node, '$near')

    def handle_nearSphere(self, node):
        return self._any_near(node, '$nearSphere')

    def handle_geoIntersects(self, node):
        return {'$geoIntersects': GeoShapeParser().handle(self.get_arg(node, 0))}

    def handle_geoWithin(self, node):
        return {'$geoWithin': GeoShapeParser().handle(self.get_arg(node, 0))}

class GenericFunc(StringFunc, IntFunc, ListFunc, DateTimeFunc,
                  IdFunc, EpochFunc, EpochUTCFunc, GeoFunc):
    pass

#---Operators---#

class Operator(AstHandler):
    def __init__(self, field):
        self.field = field
    def handle_Eq(self, node):
        '''=='''
        return self.field.handle(node)
    def handle_NotEq(self, node):
        '''!='''
        return {'$ne': self.field.handle(node)}
    def handle_In(self, node):
        '''in'''
        try:
            elts = node.elts
        except AttributeError:
            raise ParseError('Invalid value type for `in` operator: {0}'.format(node.__class__.__name__),
                             col_offset=node.col_offset)
        return {'$in': list(map(self.field.handle, elts))}
    def handle_NotIn(self, node):
        '''not in'''
        try:
            elts = node.elts
        except AttributeError:
            raise ParseError('Invalid value type for `not in` operator: {0}'.format(node.__class__.__name__),
                             col_offset=node.col_offset)
        return {'$nin': list(map(self.field.handle, elts))}

class AlgebricOperator(Operator):
    def handle_Gt(self, node):
        '''>'''
        return {'$gt': self.field.handle(node)}
    def handle_Lt(self,node):
        '''<'''
        return {'$lt': self.field.handle(node)}
    def handle_GtE(self, node):
        '''>='''
        return {'$gte': self.field.handle(node)}
    def handle_LtE(self, node):
        '''<='''
        return {'$lte': self.field.handle(node)}

#---Field-Types---#

class Field(AstHandler):
    OP_CLASS = Operator

    SPECIAL_VALUES = {'None': None,
                      'null': None}
    def handle_Name(self, node):
        try:
            return self.SPECIAL_VALUES[node.id]
        except KeyError:
            raise ParseError('Invalid name: {0}'.format(node.id), node.col_offset, options=list(self.SPECIAL_VALUES))

    def handle_operator_and_right(self, operator, right):
        return self.OP_CLASS(self).resolve(operator)(right)

class GeoField(Field):
    def handle_Call(self, node):
        return GeoFunc().handle(node)

class AlgebricField(Field):
    OP_CLASS = AlgebricOperator

class StringField(AlgebricField):
    def handle_Call(self, node):
        return StringFunc().handle(node)
    def handle_Str(self, node):
        return node.s

class IntField(AlgebricField):
    def handle_Num(self, node):
        return node.n
    def handle_Call(self, node):
        return IntFunc().handle(node)

class BoolField(Field):
    SPECIAL_VALUES = dict(Field.SPECIAL_VALUES,
                          **{'False': False,
                             'True': True,
                             'false': False,
                             'true': True})

class ListField(Field):
    def __init__(self, field=None):
        self._field = field
    def handle_List(self, node):
        return list(map((self._field or GenericField()).handle, node.elts))
    def handle_Call(self, node):
        return ListFunc().handle(node)

class DictField(Field):
    def __init__(self, field=None):
        self._field = field
    def handle_Dict(self, node):
        return dict((StringField().handle(key), (self._field or GenericField()).handle(value))
                    for key, value in zip(node.keys, node.values))

class DateTimeField(AlgebricField):
    def handle_Str(self, node):
        return parse_date(node)
    def handle_Num(self, node):
        return parse_date(node)
    def handle_Call(self, node):
        return DateTimeFunc().handle(node)

class EpochField(AlgebricField):
    def handle_Str(self, node):
        return float(parse_date(node).strftime('%s.%f'))
    def handle_Num(self, node):
        return node.n
    def handle_Call(self, node):
        return EpochFunc().handle(node)

class EpochUTCField(AlgebricField):
    def handle_Str(self, node):
        return timegm(parse_date(node).timetuple())
    def handle_Num(self, node):
        return node.n
    def handle_Call(self, node):
        return EpochUTCFunc().handle(node)

class IdField(AlgebricField):
    def handle_Str(self, node):
        return bson.ObjectId(node.s)
    def handle_Call(self, node):
        return IdFunc().handle(node)

class GenericField(IntField, BoolField, StringField, ListField, DictField, GeoField):
    def handle_Call(self, node):
        return GenericFunc().handle(node)
