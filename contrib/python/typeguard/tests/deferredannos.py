from typeguard import typechecked


@typechecked
def uses_forwardref(x: NotYetDefined) -> NotYetDefined:  # noqa: F821
    return x


class NotYetDefined:
    pass
