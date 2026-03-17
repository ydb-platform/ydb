from typing import TYPE_CHECKING

from yandexcloud._wrappers.dataproc import Dataproc, InitializationAction
from yandexcloud._wrappers.spark import (
    PysparkJobParameters,
    Spark,
    SparkClusterParameters,
    SparkJobParameters,
)

if TYPE_CHECKING:
    from yandexcloud._sdk import SDK


class Wrappers:
    def __init__(self, sdk: "SDK"):
        # pylint: disable-next=invalid-name
        self.Dataproc = Dataproc
        self.Dataproc.sdk = sdk
        # pylint: disable-next=invalid-name
        self.InitializationAction = InitializationAction
        # pylint: disable-next=invalid-name
        self.Spark = Spark
        # pylint: disable-next=invalid-name
        self.SparkClusterParameters = SparkClusterParameters
        # pylint: disable-next=invalid-name
        self.SparkJobParameters = SparkJobParameters
        # pylint: disable-next=invalid-name
        self.PysparkJobParameters = PysparkJobParameters
        self.Spark.sdk = sdk
