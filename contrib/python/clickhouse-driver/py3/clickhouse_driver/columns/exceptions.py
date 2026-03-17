

class ColumnException(Exception):
    pass


class ColumnTypeMismatchException(ColumnException):
    pass


class StructPackException(ColumnException):
    pass
