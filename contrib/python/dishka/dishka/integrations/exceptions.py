from dishka.exception_base import DishkaError


class InvalidInjectedFuncTypeError(DishkaError):
    def __str__(self) -> str:
        return "An async container cannot be used in a synchronous context."


class ImproperProvideContextUsageError(DishkaError):
    def __str__(self) -> str:
        return "provide_context can only be used with manage_scope=True."
