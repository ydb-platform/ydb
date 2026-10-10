# ci: widen the increment graph so auto sharding exceeds one hour, 2026-10-05
import typing as tp


pytest_plugins: tp.Final[list[str]] = ["ydb.tests.library.fixtures"]
"""
Enable all the fixtures from the ydb.tests.library.fixtures file.
"""
