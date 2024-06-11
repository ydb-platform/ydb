from typing import TYPE_CHECKING

from yandexcloud._wrappers.dataproc import Dataproc, InitializationAction

if TYPE_CHECKING:
    from yandexcloud._sdk import SDK


class Wrappers:
    def __init__(self, sdk: "SDK"):
        # pylint: disable-next=invalid-name
        self.Dataproc = Dataproc
        self.Dataproc.sdk = sdk
        # pylint: disable-next=invalid-name
        self.InitializationAction = InitializationAction
