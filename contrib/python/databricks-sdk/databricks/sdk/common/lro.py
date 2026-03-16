from datetime import timedelta
from typing import Optional


class LroOptions:
    """LroOptions is the options for the Long Running Operations.
    DO NOT USE THIS OPTION. This option is still under development
    and can be updated in the future without notice.
    """

    def __init__(self, *, timeout: Optional[timedelta] = None):
        """
        Args:
            timeout: The timeout for the Long Running Operations.
                if not set, then operation will wait forever.
        """
        self.timeout = timeout
