class BasePypikaException(Exception):
    pass


class QueryException(BasePypikaException):
    pass


class GroupingException(BasePypikaException):
    pass


class CaseException(BasePypikaException):
    pass


class JoinException(BasePypikaException):
    pass


class SetOperationException(BasePypikaException):
    pass


class RollupException(BasePypikaException):
    pass


class DialectNotSupported(BasePypikaException):
    pass


class FunctionException(BasePypikaException):
    pass
