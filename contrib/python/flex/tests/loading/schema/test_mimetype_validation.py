import itertools
import pytest

from flex.loading.common.mimetypes import (
    mimetype_validator,
)
from flex.loading.schema.host import decompose_hostname
from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES

from tests.utils import (
    assert_message_in_errors,
    assert_path_not_in_errors,
    assert_path_in_errors,
)


@pytest.mark.parametrize(
    'value',
    ('application/json', 1, 1.1, {'a': 1}, None),
)
def test_mimetype_invalid_for_non_array_value(value):
    with pytest.raises(ValidationError) as err:
        mimetype_validator(value)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'type',
    )


@pytest.mark.parametrize(
    'value',
    (
        # different media types with simple subtypes
        ['application/json'],
        ['audio/basic'],
        ['font/collection'],
        ['example/json'],
        ['image/png'],
        ['message/http'],
        ['model/stl'],
        ['multipart/encrypted'],
        ['text/html'],
        ['video/ogg'],
        ['video/mp4'],
        # different complex subtypes
        ['application/1d-interleaved-parityfec'],  # dashes in subtype
        ['video/JPEG'],  # uppercase
        ['video/SMPTE292M'],  # uppercase and numbers
        ['video/smpte291'],  # lowercase and numbers
        ['video/CelB'],  # mixed-case
        # vendor subtypes
        ['application/vnd.apple.pages'],  # simple vendor
        ['application/vnd.oasis.opendocument.text'],  # multiple vendor
        # suffixes
        ['application/xhtml+xml'],  # common
        ['image/svg+xml'],  # common
        ['application/calendar+json'],
        ['application/geo+json'],
        ['application/jose+json'],
        ['application/json-patch+json'],
        ['application/jwk-set+json'],
        ['application/jwk+json'],
        ['application/vcard+json'],
        ['application/senml+cbor'],
        ['application/dssc+der'],
        ['application/soap+fastinfoset'],
        ['application/vnd.nokia.pcd+wbxml'],  # wbxml only occurs with vendor
        ['application/epub+zip'],
        ['application/vnd.oma.lwm2m+tlv'],  # only registered use
        ['application/geo+json-seq'],  # only registered use
        ['application/geopackage+sqlite3'],  # only registered use
        ['application/secevent+jwt'],  # only registered use
        ['application/tlsrpt+gzip'],  # only registered use
        # complex cases
        ['application/3gpdash-qoe-report+xml'],  # dashes in subtype
        ['application/vnd.api+json'],  # vendor and suffix
        ['application/vnd.bbf.usp.msg+json'],  # multiple vendor tokens
        ['application/vnd.ms-excel'],  # vendor with dashes
        ['application/vnd.collabio.xodocuments.spreadsheet-template'],  # long vendor
        ['text/plain; charset=utf-8'],  # unquoted parameter with space
        ['text/plain;charset=utf-8'],  # unquoted parameter without space
        ['video/mp4; codecs="avc1.640028"'],  # double quoted parameter
        ["video/mp4; codecs='avc1.640028'"],  # single quoted parameter
    )
)
def test_mimetype_with_valid_values(value):
    try:
        mimetype_validator(value)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'type',
        errors,
    )
    assert_path_not_in_errors(
        'value',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    (
        ['invalidtypename/json'],
        ['noslash'],
    )
)
def test_mimetype_with_invalid_values(value):
    with pytest.raises(ValidationError) as err:
        mimetype_validator(value)

    assert_message_in_errors(
        MESSAGES['mimetype']['invalid'],
        err.value.detail,
    )


def test_mimetype_with_multiple_valid_values():
    try:
        mimetype_validator(['application/json', 'application/xml'])
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'type',
        errors,
    )
    assert_path_not_in_errors(
        'value',
        errors,
    )


def test_mimetype_with_invalid_value_in_multiple_values():
    with pytest.raises(ValidationError) as err:
        mimetype_validator(['application/json', 'not-a-valid-mimetype'])

    assert_message_in_errors(
        MESSAGES['mimetype']['invalid'],
        err.value.detail,
    )
