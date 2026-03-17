from typing import Optional  # noqa: D100

from duckdb.experimental.spark.exception import ContributionsAcceptedError


class SparkConf:  # noqa: D101
    def __init__(self) -> None:  # noqa: D107
        raise NotImplementedError

    def contains(self, key: str) -> bool:  # noqa: D102
        raise ContributionsAcceptedError

    def get(self, key: str, defaultValue: Optional[str] = None) -> Optional[str]:  # noqa: D102
        raise ContributionsAcceptedError

    def getAll(self) -> list[tuple[str, str]]:  # noqa: D102
        raise ContributionsAcceptedError

    def set(self, key: str, value: str) -> "SparkConf":  # noqa: D102
        raise ContributionsAcceptedError

    def setAll(self, pairs: list[tuple[str, str]]) -> "SparkConf":  # noqa: D102
        raise ContributionsAcceptedError

    def setAppName(self, value: str) -> "SparkConf":  # noqa: D102
        raise ContributionsAcceptedError

    def setExecutorEnv(  # noqa: D102
        self, key: Optional[str] = None, value: Optional[str] = None, pairs: Optional[list[tuple[str, str]]] = None
    ) -> "SparkConf":
        raise ContributionsAcceptedError

    def setIfMissing(self, key: str, value: str) -> "SparkConf":  # noqa: D102
        raise ContributionsAcceptedError

    def setMaster(self, value: str) -> "SparkConf":  # noqa: D102
        raise ContributionsAcceptedError

    def setSparkHome(self, value: str) -> "SparkConf":  # noqa: D102
        raise ContributionsAcceptedError

    def toDebugString(self) -> str:  # noqa: D102
        raise ContributionsAcceptedError


__all__ = ["SparkConf"]
