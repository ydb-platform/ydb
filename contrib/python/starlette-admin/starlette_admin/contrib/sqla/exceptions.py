from starlette_admin.exceptions import StarletteAdminException


class InvalidModelError(StarletteAdminException):
    pass


class InvalidQuery(StarletteAdminException):
    pass


class NotSupportedColumn(StarletteAdminException):
    pass


class NotSupportedValue(StarletteAdminException):
    pass
