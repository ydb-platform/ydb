from pytest import raises
from dbfread.dbf import DBF
from dbfread.exceptions import MissingMemoFile

def test_missing_memofile():
    import yatest.common as yc
    with raises(MissingMemoFile):
        DBF(yc.source_path('contrib/python/dbfread/testcases/no_memofile.dbf'))

    # This should succeed.
    table = DBF(yc.source_path('contrib/python/dbfread/testcases/no_memofile.dbf'), ignore_missing_memofile=True)

    # Memo fields should be returned as None.
    record = next(iter(table))
    assert record['MEMO'] is None
