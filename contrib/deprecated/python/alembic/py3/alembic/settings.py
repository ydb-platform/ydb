# Changes Alembic logic from using file system to Python modules for migrations
# FS logic is unsound regarding Arcadia: all code should be in the binary
#
# NB: not all Alembic code has been rewritten to support it,
# you may need to DIY, especially in `alembic.command`
use_arcadia_modules = False
