try:
    from ticket_parser2_py3.ticket_parser2_pymodule import (  # noqa
        ServiceContext,
        UserContext,
    )
except ImportError:
    from ticket_parser2.ticket_parser2_pymodule import (  # noqa
        ServiceContext,
        UserContext,
    )
