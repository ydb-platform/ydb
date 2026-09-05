"""The shared failure boundary for bounded relational encodings."""


class RelationError(ValueError):
    """A valid snapshot uses relational semantics not modeled by this evaluator."""
