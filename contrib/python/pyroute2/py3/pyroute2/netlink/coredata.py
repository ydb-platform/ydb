import os
from dataclasses import asdict, dataclass
from typing import Optional, Type, Union

from pyroute2 import config
from pyroute2.requests.main import RequestFilter, RequestProcessor


@dataclass
class CoreConfig:
    target: str = 'localhost'
    flags: int = os.O_CREAT
    groups: int = 0
    rcvsize: int = 16384
    netns: Optional[str] = None
    tag_field: str = ''
    address: Optional[tuple[str, int]] = None
    telemetry: Optional[tuple[str, int]] = None
    use_socket: bool = False
    use_event_loop: bool = False
    use_libc: bool = False


class CoreSocketSpec:
    defaults: dict[str, Union[bool, int, str, None, tuple[str, ...]]] = {
        'closed': False,
        'compiled': None,
        'uname': config.uname,
    }
    status_filters: list[Type[RequestFilter]] = []

    def __init__(self, config: CoreConfig):
        self.config = config
        self.status = RequestProcessor()
        for flt in self.status_filters:
            self.status.add_filter(flt())
        self.status.update(self.defaults)
        self.status.update(asdict(self.config))

    def __setitem__(self, key, value):
        setattr(self.config, key, value)
        self.status.update(asdict(self.config))

    def __getitem__(self, key):
        return getattr(self.config, key)

    def serializable(self):
        return not any(
            (
                self.config.use_socket,
                self.config.use_event_loop,
                self.config.use_libc,
            )
        )
