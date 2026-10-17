PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/nbs/suite.inc)

TEST_SRCS(
    conftest.py
    F5_01_parse_dbg_host_table.py
    F5_02_parse_pbuffer_occupancy.py
    F5_03_vchunk_pending_minlsn.py
    F5_04_volume_request_counters.py
    F5_05_ddisk_directio.py
)

END()
