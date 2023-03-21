from .scheme import (
    SchemeEntryType,
    _wrap_scheme_entry,
    _wrap_list_directory_response,
)
from ._apis import ydb_scheme


def test_wrap_scheme_entry():
    assert (
        _wrap_scheme_entry(ydb_scheme.Entry(type=1)).type is SchemeEntryType.DIRECTORY
    )
    assert _wrap_scheme_entry(ydb_scheme.Entry(type=17)).type is SchemeEntryType.TOPIC

    assert (
        _wrap_scheme_entry(ydb_scheme.Entry()).type is SchemeEntryType.TYPE_UNSPECIFIED
    )
    assert (
        _wrap_scheme_entry(ydb_scheme.Entry(type=10)).type
        is SchemeEntryType.TYPE_UNSPECIFIED
    )
    assert (
        _wrap_scheme_entry(ydb_scheme.Entry(type=1001)).type
        is SchemeEntryType.TYPE_UNSPECIFIED
    )


def test_wrap_list_directory_response():
    d = _wrap_list_directory_response(None, ydb_scheme.ListDirectoryResponse())
    assert d.type is SchemeEntryType.TYPE_UNSPECIFIED
