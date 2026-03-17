from .builder import Builder


class MakeBuilder:
    """Use E.elemname or E(content) to create initially empty snippets."""

    def __getattr__(self, name):
        return getattr(Builder("E Builder"), name)

    def __call__(self, *args, **kwargs):
        return Builder("E Builder")(*args, **kwargs)


E = MakeBuilder()
