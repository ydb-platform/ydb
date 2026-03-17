"""
Generators are callables that return a value used to populate a field.

If this callable has a `required` attribute (a list, mostly), for each item in
the list, if the item is a string, the field attribute with the same name will
be fetched from the field and used as argument for the generator. If it is a
callable (which will receive `field` as first argument), it should return a
list in the format (key, value) where key is the argument name for generator
and value is the value for that argument.
"""

import string
import warnings
from decimal import Decimal
from os.path import abspath, join, dirname
from random import randint, choice, random, uniform

from model_mommy.timezone import now


MAX_LENGTH = 300
# Using sys.maxint here breaks a bunch of tests when running against a
# Postgres database.
MAX_INT = 10000


def get_content_file(content, name):
    from django.core.files.base import ContentFile
    return ContentFile(content, name=name)


def gen_file_field():
    name = 'mock_file.txt'
    file_path = abspath(join(dirname(__file__), name))
    with open(file_path, 'rb') as f:
        return get_content_file(f.read(), name=name)


def gen_image_field():
    name = 'mock-img.jpeg'
    file_path = abspath(join(dirname(__file__), name))
    with open(file_path, 'rb') as f:
        return get_content_file(f.read(), name=name)


def gen_from_list(L):
    '''Makes sure all values of the field are generated from the list L
    Usage:
    from mommy import Mommy
    class KidMommy(Mommy):
      attr_mapping = {'some_field':gen_from_list([A, B, C])}
    '''
    return lambda: choice(list(L))


# -- DEFAULT GENERATORS --


def gen_from_choices(choices):
    choice_list = []
    for value, label in choices:
        if isinstance(label, (list, tuple)):
            for val, lbl in label:
                choice_list.append(val)
        else:
            choice_list.append(value)
    return gen_from_list(choice_list)


def gen_integer(min_int=-MAX_INT, max_int=MAX_INT):
    return randint(min_int, max_int)


def gen_float():
    return random() * gen_integer()


def gen_decimal(max_digits, decimal_places):
    def num_as_str(x):
        return ''.join([str(randint(0, 9)) for _ in range(x)])

    if decimal_places:
        return Decimal("%s.%s" % (num_as_str(max_digits - decimal_places - 1),
                                  num_as_str(decimal_places)))
    return Decimal(num_as_str(max_digits))


gen_decimal.required = ['max_digits', 'decimal_places']


def gen_date():
    return now().date()


def gen_datetime():
    return now()


def gen_time():
    return now().time()


def gen_string(max_length):
    return str(''.join(choice(string.ascii_letters) for _ in range(max_length)))


gen_string.required = ['max_length']


def gen_slug(max_length):
    valid_chars = string.ascii_letters + string.digits + '_-'
    return str(''.join(choice(valid_chars) for _ in range(max_length)))


gen_slug.required = ['max_length']


def gen_text():
    return gen_string(MAX_LENGTH)


def gen_boolean():
    return choice((True, False))


def gen_null_boolean():
    return choice((True, False, None))


def gen_url():
    return str('http://www.%s.com/' % gen_string(30))


def gen_email():
    return "%s@example.com" % gen_string(10)


def gen_ipv6():
    return ":".join(format(randint(1, 65535), 'x') for _ in range(8))


def gen_ipv4():
    return ".".join(str(randint(1, 255)) for _ in range(4))


def gen_ipv46():
    ip_gen = choice([gen_ipv4, gen_ipv6])
    return ip_gen()


def gen_ip(protocol, default_validators):
    from django.core.exceptions import ValidationError
    protocol = (protocol or '').lower()

    if not protocol:
        field_validator = default_validators[0]
        dummy_ipv4 = '1.1.1.1'
        dummy_ipv6 = 'FE80::0202:B3FF:FE1E:8329'
        try:
            field_validator(dummy_ipv4)
            field_validator(dummy_ipv6)
            generator = gen_ipv46
        except ValidationError:
            try:
                field_validator(dummy_ipv4)
                generator = gen_ipv4
            except ValidationError:
                generator = gen_ipv6
    elif protocol == 'ipv4':
        generator = gen_ipv4
    elif protocol == 'ipv6':
        generator = gen_ipv6
    else:
        generator = gen_ipv46

    return generator()


gen_ip.required = ['protocol', 'default_validators']


def gen_byte_string(max_length=16):
    generator = (randint(0, 255) for x in range(max_length))
    return bytes(generator)


def gen_interval(interval_key='milliseconds'):
    from datetime import timedelta
    interval = gen_integer()
    kwargs = {interval_key: interval}
    return timedelta(**kwargs)


def gen_content_type():
    from django.contrib.contenttypes.models import ContentType
    from django.apps import apps
    try:
        return ContentType.objects.get_for_model(choice(apps.get_models()))
    except AssertionError:
        warnings.warn('Database access disabled, returning ContentType raw instance')
        return ContentType()


def gen_uuid():
    import uuid
    return uuid.uuid4()


def gen_array():
    return []


def gen_json():
    return {}


def gen_hstore():
    return {}


def _fk_model(field):
    try:
        return ('model', field.related_model)
    except AttributeError:
        return ('model', field.related.parent_model)


def _prepare_related(model, **attrs):
    from .mommy import prepare
    return prepare(model, **attrs)


def gen_related(model, **attrs):
    from .mommy import make
    return make(model, **attrs)


gen_related.required = [_fk_model]
gen_related.prepare = _prepare_related


def gen_m2m(model, **attrs):
    from .mommy import make, MAX_MANY_QUANTITY
    return make(model, _quantity=MAX_MANY_QUANTITY, **attrs)


gen_m2m.required = [_fk_model]


# GIS generators

def gen_coord():
    return uniform(0, 1)


def gen_coords():
    return '{x} {y}'.format(x=gen_coord(), y=gen_coord())


def gen_point():
    return 'POINT ({})'.format(
        gen_coords(),
    )


def _gen_line_string_without_prefix():
    return '({}, {})'.format(
        gen_coords(),
        gen_coords(),
    )


def gen_line_string():
    return 'LINESTRING {}'.format(
        _gen_line_string_without_prefix()
    )


def _gen_polygon_without_prefix():
    start = gen_coords()
    return '(({}, {}, {}, {}))'.format(
        start,
        gen_coords(),
        gen_coords(),
        start
    )


def gen_polygon():
    return 'POLYGON {}'.format(
        _gen_polygon_without_prefix(),
    )


def gen_multi_point():
    return 'MULTIPOINT (({}))'.format(
        gen_coords(),
    )


def gen_multi_line_string():
    return 'MULTILINESTRING ({})'.format(
        _gen_line_string_without_prefix(),
    )


def gen_multi_polygon():
    return 'MULTIPOLYGON ({})'.format(
        _gen_polygon_without_prefix(),
    )


def gen_geometry():
    return gen_point()


def gen_geometry_collection():
    return 'GEOMETRYCOLLECTION ({})'.format(
        gen_point(),
    )
