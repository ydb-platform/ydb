try:
    from ticket_parser2_py3.ticket_parser2_pymodule import __version__ as ticket_parser2_pymodule_version
except ImportError:
    from ticket_parser2.ticket_parser2_pymodule import __version__ as ticket_parser2_pymodule_version

try:
    from ticket_parser2_py3.ticket_parser2_pymodule import (  # noqa
        BlackboxClientId,
        BlackboxEnv,
        ServiceTicket,
        Status,
        UserTicket,
        TvmApiClientSettings,
        TvmToolClientSettings,
        TvmClientStatus,
        TvmClient as __TvmClientImpl,
    )
except ImportError:
    from ticket_parser2.ticket_parser2_pymodule import (  # noqa
        BlackboxClientId,
        BlackboxEnv,
        ServiceTicket,
        Status,
        UserTicket,
        TvmApiClientSettings,
        TvmToolClientSettings,
        TvmClientStatus,
        TvmClient as __TvmClientImpl,
    )

import warnings


class TvmClient(__TvmClientImpl):
    pass


__version__ = ticket_parser2_pymodule_version


warnings.warn(
    message="ticket_parser2 is deprecated: please use https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth instead. https://clubs.at.yandex-team.ru/passport/3619",
    category=DeprecationWarning,
    stacklevel=2,
)
