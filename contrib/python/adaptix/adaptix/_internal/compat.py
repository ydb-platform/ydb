try:
    from builtins import BaseExceptionGroup, ExceptionGroup
except ImportError:
    from exceptiongroup import BaseExceptionGroup, ExceptionGroup  # type: ignore[no-redef]

CompatExceptionGroup = ExceptionGroup
CompatBaseExceptionGroup = BaseExceptionGroup
