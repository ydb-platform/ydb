import sqlalchemy as sa

sa_version = sa.__version__

if sa_version.startswith("2."):
    from .sa20 import YqlCompiler
    from .sa20 import YqlDDLCompiler
    from .sa20 import YqlTypeCompiler
    from .sa20 import YqlIdentifierPreparer
elif sa_version.startswith("1.4."):
    from .sa14 import YqlCompiler
    from .sa14 import YqlDDLCompiler
    from .sa14 import YqlTypeCompiler
    from .sa14 import YqlIdentifierPreparer
else:
    raise RuntimeError("Unsupported SQLAlchemy version.")
