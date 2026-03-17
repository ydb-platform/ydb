import sys

from sqlalchemy.testing.requirements import Requirements

from alembic import util
from alembic.testing import exclusions
from alembic.util import sqla_compat


class SuiteRequirements(Requirements):
    @property
    def schemas(self):
        """Target database must support external schemas, and have one
        named 'test_schema'."""

        return exclusions.open()

    @property
    def autocommit_isolation(self):
        """target database should support 'AUTOCOMMIT' isolation level"""

        return exclusions.closed()

    @property
    def unique_constraint_reflection(self):
        def doesnt_have_check_uq_constraints(config):
            from sqlalchemy import inspect

            insp = inspect(config.db)
            try:
                insp.get_unique_constraints("x")
            except NotImplementedError:
                return True
            except TypeError:
                return True
            except Exception:
                pass
            return False

        return exclusions.skip_if(doesnt_have_check_uq_constraints)

    @property
    def sequences(self):
        """Target database must support SEQUENCEs."""

        return exclusions.only_if(
            [lambda config: config.db.dialect.supports_sequences],
            "no sequence support",
        )

    @property
    def foreign_key_match(self):
        return exclusions.open()

    @property
    def check_constraints_w_enforcement(self):
        """Target database must support check constraints
        and also enforce them."""

        return exclusions.open()

    @property
    def reflects_pk_names(self):
        return exclusions.closed()

    @property
    def reflects_fk_options(self):
        return exclusions.closed()

    @property
    def editor_installed(self):
        def go():
            try:
                import editor  # noqa
            except ImportError:
                return False
            else:
                return True

        return exclusions.only_if(go, "editor package not installed")

    @property
    def sqlalchemy_13(self):
        return exclusions.skip_if(
            lambda config: not util.sqla_13,
            "SQLAlchemy 1.3 or greater required",
        )

    @property
    def sqlalchemy_14(self):
        return exclusions.skip_if(
            lambda config: not util.sqla_14,
            "SQLAlchemy 1.4 or greater required",
        )

    @property
    def python3(self):
        return exclusions.skip_if(
            lambda: sys.version_info < (3,), "Python version 3.xx is required."
        )

    @property
    def pep3147(self):

        return exclusions.only_if(lambda config: util.compat.has_pep3147())

    @property
    def comments(self):
        return exclusions.only_if(
            lambda config: config.db.dialect.supports_comments
        )

    @property
    def alter_column(self):
        return exclusions.open()

    @property
    def computed_columns(self):
        return exclusions.closed()

    @property
    def computed_columns_api(self):
        return exclusions.only_if(
            exclusions.BooleanPredicate(sqla_compat.has_computed)
        )

    @property
    def identity_columns(self):
        return exclusions.closed()

    @property
    def identity_columns_alter(self):
        return exclusions.closed()

    @property
    def identity_columns_api(self):
        return exclusions.only_if(
            exclusions.BooleanPredicate(sqla_compat.has_identity)
        )

    @property
    def supports_identity_on_null(self):
        return exclusions.closed()
