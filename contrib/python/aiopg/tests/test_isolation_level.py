import pytest

from aiopg import IsolationLevel


@pytest.mark.parametrize(
    "isolation_level, name",
    [
        (IsolationLevel.default, "Default"),
        (IsolationLevel.read_committed, "Read committed"),
        (IsolationLevel.repeatable_read, "Repeatable read"),
        (IsolationLevel.serializable, "Serializable"),
    ],
)
def test_isolation_level_name(isolation_level, name):
    assert isolation_level(False, False).name == name


@pytest.mark.parametrize(
    "isolation_level, readonly, deferred, expected_begin",
    [
        (IsolationLevel.default, False, False, "BEGIN"),
        (IsolationLevel.default, True, False, "BEGIN READ ONLY"),
        (
            IsolationLevel.read_committed,
            False,
            False,
            "BEGIN ISOLATION LEVEL READ COMMITTED",
        ),
        (
            IsolationLevel.read_committed,
            True,
            False,
            "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY",
        ),
        (
            IsolationLevel.repeatable_read,
            False,
            False,
            "BEGIN ISOLATION LEVEL REPEATABLE READ",
        ),
        (
            IsolationLevel.repeatable_read,
            True,
            False,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        ),
        (
            IsolationLevel.serializable,
            False,
            False,
            "BEGIN ISOLATION LEVEL SERIALIZABLE",
        ),
        (
            IsolationLevel.serializable,
            True,
            False,
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY",
        ),
        (
            IsolationLevel.serializable,
            True,
            True,
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE",
        ),
    ],
)
def test_isolation_level_begin(
    isolation_level, readonly, deferred, expected_begin
):
    assert isolation_level(readonly, deferred).begin() == expected_begin
