try:
    from ticket_parser2_py3.ticket_parser2_pymodule import (  # noqa
        ContextException,
        EmptyTvmKeysException,
        MalformedTvmKeysException,
        MalformedTvmSecretException,
        TicketParsingException,
        TvmException,
        ClientException,
        BrokenTvmClientSettings,
        RetriableException,
        NonRetriableException,
        MissingServiceTicket,
        PermissionDenied,
    )
except ImportError:
    from ticket_parser2.ticket_parser2_pymodule import (  # noqa
        ContextException,
        EmptyTvmKeysException,
        MalformedTvmKeysException,
        MalformedTvmSecretException,
        TicketParsingException,
        TvmException,
        ClientException,
        BrokenTvmClientSettings,
        RetriableException,
        NonRetriableException,
        MissingServiceTicket,
        PermissionDenied,
    )
