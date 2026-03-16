import pydantic

major_version = int(pydantic.version.VERSION.split(".")[0])

if major_version == 1:
    from lazy_model.parser.old import LazyModel  # noqa: F401
elif major_version == 2:
    from lazy_model.parser.new import LazyModel  # noqa: F401
else:
    raise NotImplementedError(
        f"pydantic version {major_version} not supported"
    )
