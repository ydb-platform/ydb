PY3_LIBRARY()

# Standard library only: this package is meant to be copied to a production
# host and run there as `python3 -m consistency`.  Do not add PEERDIRs.
PY_SRCS(
    __init__.py
    __main__.py
    doctor.py
    hive_recovery.py
    model.py
    registry.py
    report.py
    views.py
    checks/__init__.py
    checks/_util.py
    checks/ledger_checks.py
    checks/meta.py
    checks/refs.py
    checks/replay.py
    checks/sequences.py
    checks/storage.py
    checks/tenants.py
    sources/__init__.py
    sources/backup.py
    sources/ledger.py
    sources/live.py
    sources/http_auth.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
