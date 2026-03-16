class AresponsesAssertionError(AssertionError):
    pass


class NoRouteFoundError(AresponsesAssertionError):
    pass


class UnusedRouteError(AresponsesAssertionError):
    pass


class UnorderedRouteCallError(AresponsesAssertionError):
    pass
