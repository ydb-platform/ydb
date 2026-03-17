from django.contrib.contenttypes.models import ContentType
from django.db.models import (
    BigIntegerField, BinaryField, BooleanField, CharField, DateField, DateTimeField, DecimalField,
    DurationField, EmailField, FileField, FloatField, ForeignKey, GenericIPAddressField,
    ImageField, IntegerField, IPAddressField, ManyToManyField, NullBooleanField, OneToOneField,
    PositiveIntegerField, PositiveSmallIntegerField, SlugField, SmallIntegerField, TextField,
    TimeField, URLField, UUIDField
)

from . import random_gen
from .gis import default_gis_mapping
from .utils import import_if_str

try:
    from django.contrib.postgres.fields import ArrayField
except ImportError:
    ArrayField = None

try:
    from django.contrib.postgres.fields import JSONField
except ImportError:
    JSONField = None

try:
    from django.contrib.postgres.fields import HStoreField
except ImportError:
    HStoreField = None

try:
    from django.contrib.postgres.fields.citext import CICharField, CIEmailField, CITextField
except ImportError:
    CICharField = None
    CIEmailField = None
    CITextField = None


default_mapping = {
    ForeignKey: random_gen.gen_related,
    OneToOneField: random_gen.gen_related,
    ManyToManyField: random_gen.gen_m2m,

    BooleanField: random_gen.gen_boolean,
    NullBooleanField: random_gen.gen_boolean,
    IntegerField: random_gen.gen_integer,
    BigIntegerField: random_gen.gen_integer,
    SmallIntegerField: random_gen.gen_integer,

    PositiveIntegerField: lambda: random_gen.gen_integer(min_int=0),
    PositiveSmallIntegerField: lambda: random_gen.gen_integer(min_int=0),

    FloatField: random_gen.gen_float,
    DecimalField: random_gen.gen_decimal,

    BinaryField: random_gen.gen_byte_string,
    CharField: random_gen.gen_string,
    TextField: random_gen.gen_text,
    SlugField: random_gen.gen_slug,
    UUIDField: random_gen.gen_uuid,

    DateField: random_gen.gen_date,
    DateTimeField: random_gen.gen_datetime,
    TimeField: random_gen.gen_time,

    URLField: random_gen.gen_url,
    EmailField: random_gen.gen_email,
    IPAddressField: random_gen.gen_ipv4,
    GenericIPAddressField: random_gen.gen_ip,
    FileField: random_gen.gen_file_field,
    ImageField: random_gen.gen_image_field,
    DurationField: random_gen.gen_interval,

    ContentType: random_gen.gen_content_type,
}

if ArrayField:
    default_mapping[ArrayField] = random_gen.gen_array
if JSONField:
    default_mapping[JSONField] = random_gen.gen_json
if HStoreField:
    default_mapping[HStoreField] = random_gen.gen_hstore
if CICharField:
    default_mapping[CICharField] = random_gen.gen_string
if CIEmailField:
    default_mapping[CIEmailField] = random_gen.gen_email
if CITextField:
    default_mapping[CITextField] = random_gen.gen_text

# Add GIS fields
default_mapping.update(default_gis_mapping)


def get_type_mapping():
    mapping = default_mapping.copy()
    return mapping.copy()


user_mapping = {}


def add(field, func):
    user_mapping[import_if_str(field)] = import_if_str(func)


def get(field):
    return user_mapping.get(field)
