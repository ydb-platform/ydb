from typing import List, Optional


class StripeContext:
    """
    The StripeContext class provides an immutable container and convenience methods for interacting with the `Stripe-Context` header. All methods return a new instance of StripeContext.

    You can use it whenever you're initializing a `StripeClient` or sending `stripe_context` with a request. It's also found in the `EventNotification.context` property.
    """

    def __init__(self, segments: Optional[List[str]] = None):
        self._segments = segments or []

    def push(self, segment: str) -> "StripeContext":
        return StripeContext(self._segments + [segment])

    def pop(self) -> "StripeContext":
        if not self._segments:
            raise ValueError("Cannot pop from an empty StripeContext")

        return StripeContext(self._segments[:-1])

    def __str__(self) -> str:
        return "/".join(self._segments)

    def __repr__(self) -> str:
        return f"StripeContext({self._segments!r})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, StripeContext):
            return False
        return self._segments == other._segments

    @staticmethod
    def parse(context_str: str) -> "StripeContext":
        if not context_str:
            return StripeContext()

        segments = context_str.split("/")
        return StripeContext(segments)
