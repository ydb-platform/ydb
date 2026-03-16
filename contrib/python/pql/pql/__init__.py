from functools import wraps
from .aggregation import AggregationGroupParser, AggregationParser
from .matching import (SchemaFreeParser, SchemaAwareParser, ParseError,
                       StringField, IntField, BoolField, IdField,
                       ListField, DictField, DateTimeField)

def find(expression, schema=None):
    '''
    Gets an <expression> and optional <schema>.
    <expression> should be a string of python code.
    <schema> should be a dictionary mapping field names to types.
    '''
    parser = SchemaFreeParser() if schema is None else SchemaAwareParser(schema)
    return parser.parse(expression)

class pipe_element(list):
    def __or__(self, other):
        return pipe_element(self + other)

def pipe(func):
    @wraps(func)
    def decorated(*a, **k):
        return pipe_element([func(*a, **k)])
    return decorated

def _parse_value(parser, value):
    if isinstance(value, str):
        return parser.parse(value)
    elif isinstance(value, int):
        return value
    else:
        raise ValueError('Unexpected type: {}'.format(value.__class__))
def _parse_dict(parser, dct):
    return dict([(k, _parse_value(parser, v)) for k, v in dct.items()])

@pipe
def group(_id, **kwargs):
    group = _parse_dict(parser=AggregationGroupParser(), dct=kwargs)
    if isinstance(_id, pipe_element):
        _id = _id[0]['$project']
    else:
        _id = AggregationParser().parse(_id)
    group['_id'] = _id
    return {'$group': group}

@pipe
def project(**kwargs):
    return {'$project': _parse_dict(parser=AggregationParser(), dct=kwargs)}

@pipe
def match(expression, schema=None):
    return {'$match': find(expression, schema)}

@pipe
def limit(number):
    if not isinstance(number, int):
        raise ValueError("aggregation 'limit' must be a number")
    return {'$limit': number}

@pipe
def skip(number):
    if not isinstance(number, int):
        raise ValueError("aggregation 'skip' must be a number")
    return {'$skip': number}

@pipe
def unwind(list_name):
    if not isinstance(list_name, str):
        raise ValueError("aggregation 'unwind' must be a str")
    return {'$unwind': '$' + list_name}

@pipe
def sort(fields):
    '''
    Gets a list of <fields> to sort by.
    Also supports getting a single string for sorting by one field.
    Reverse sort is supported by appending '-' to the field name.
    Example: sort(['age', '-height']) will sort by ascending age and descending height.
    '''
    from pymongo import ASCENDING, DESCENDING
    from bson import SON

    if isinstance(fields, str):
        fields = [fields]
    if not hasattr(fields, '__iter__'):
        raise ValueError("expected a list of strings or a string. not a {}".format(type(fields)))
    
    sort = []
    for field in fields:
        if field.startswith('-'):
            field = field[1:]
            sort.append((field, DESCENDING))
            continue
        elif field.startswith('+'):
            field = field[1:]
        sort.append((field, ASCENDING))
    return {'$sort': SON(sort)}
