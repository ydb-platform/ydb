import sqlalchemy as sa

from .exc import ClassNotVersioned as ClassNotVersioned
from .exc import ImproperlyConfigured as ImproperlyConfigured
from .manager import VersioningManager
from .operation import Operation as Operation
from .transaction import TransactionFactory as TransactionFactory
from .unit_of_work import UnitOfWork as UnitOfWork
from .utils import (
    changeset as changeset,
)
from .utils import (
    count_versions as count_versions,
)
from .utils import (
    get_versioning_manager as get_versioning_manager,
)
from .utils import (
    is_modified as is_modified,
)
from .utils import (
    is_session_modified as is_session_modified,
)
from .utils import (
    parent_class as parent_class,
)
from .utils import (
    transaction_class as transaction_class,
)
from .utils import (
    tx_column_name as tx_column_name,
)
from .utils import (
    vacuum as vacuum,
)
from .utils import (
    version_class as version_class,
)

__version__ = '1.6.0'


versioning_manager = VersioningManager()


def make_versioned(
    mapper=sa.orm.Mapper,
    session=sa.orm.session.Session,
    manager=versioning_manager,
    plugins=None,
    options=None,
    user_cls='User',
):
    """
    This is the public API function of SQLAlchemy-Continuum for making certain
    mappers and sessions versioned. By default this applies to all mappers and
    all sessions.

    :param mapper:
        SQLAlchemy mapper to apply the versioning to.
    :param session:
        SQLAlchemy session to apply the versioning to. By default this is
        sa.orm.session.Session meaning it applies to all Session subclasses.
    :param manager:
        SQLAlchemy-Continuum versioning manager.
    :param plugins:
        Plugins to pass for versioning manager.
    :param options:
        A dictionary of VersioningManager options.
    :param user_cls:
        User class which the Transaction class should have relationship to.
        This can either be a class or string name of a class for lazy
        evaluation.
    """
    if plugins is not None:
        manager.plugins = plugins

    if options is not None:
        manager.options.update(options)

    manager.user_cls = user_cls
    manager.apply_class_configuration_listeners(mapper)
    manager.track_operations(mapper)
    manager.track_session(session)

    sa.event.listen(
        sa.engine.Engine, 'before_execute', manager.track_association_operations
    )

    sa.event.listen(sa.engine.Engine, 'rollback', manager.clear_connection)

    sa.event.listen(
        sa.engine.Engine,
        'set_connection_execution_options',
        manager.track_cloned_connections,
    )


def remove_versioning(
    mapper=sa.orm.Mapper, session=sa.orm.session.Session, manager=versioning_manager
):
    """
    Remove the versioning from given mapper / session and manager.

    :param mapper:
        SQLAlchemy mapper to remove the versioning from.
    :param session:
        SQLAlchemy session to remove the versioning from. By default this is
        sa.orm.session.Session meaning it applies to all sessions.
    :param manager:
        SQLAlchemy-Continuum versioning manager.
    """
    manager.reset()
    manager.remove_class_configuration_listeners(mapper)
    manager.remove_operations_tracking(mapper)
    manager.remove_session_tracking(session)
    sa.event.remove(
        sa.engine.Engine, 'before_execute', manager.track_association_operations
    )

    sa.event.remove(sa.engine.Engine, 'rollback', manager.clear_connection)

    sa.event.remove(
        sa.engine.Engine,
        'set_connection_execution_options',
        manager.track_cloned_connections,
    )
