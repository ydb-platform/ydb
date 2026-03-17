import argparse
from functools import wraps
import logging
import os
import sys
from flask import current_app, g
from alembic import __version__ as __alembic_version__
from alembic.config import Config as AlembicConfig
from alembic import command
from alembic.util import CommandError

alembic_version = tuple([int(v) for v in __alembic_version__.split('.')[0:3]])
log = logging.getLogger(__name__)


class _MigrateConfig(object):
    def __init__(self, migrate, db, **kwargs):
        self.migrate = migrate
        self.db = db
        self.directory = migrate.directory
        self.configure_args = kwargs

    @property
    def metadata(self):
        """
        Backwards compatibility, in old releases app.extensions['migrate']
        was set to db, and env.py accessed app.extensions['migrate'].metadata
        """
        return self.db.metadata


class Config(AlembicConfig):
    def __init__(self, *args, **kwargs):
        self.template_directory = kwargs.pop('template_directory', None)
        super().__init__(*args, **kwargs)

    def get_template_directory(self):
        if self.template_directory:
            return self.template_directory
        import tempfile
        from library.python import resource

        package_dir = os.path.dirname(__file__)
        prefix = 'resfs/file/{}/templates/'.format(package_dir)

        self.tmpdir = tempfile.TemporaryDirectory()

        for key, data in resource.iteritems(prefix=prefix, strip_prefix=True):
            filename = os.path.join(self.tmpdir.name, key)
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'wb') as f_data:
                f_data.write(data)

        return self.tmpdir.name

    def __del__(self):
        if hasattr(self, 'tmpdir'):
            self.tmpdir.cleanup()


class Migrate(object):
    def __init__(self, app=None, db=None, directory='migrations', command='db',
                 compare_type=True, render_as_batch=True, **kwargs):
        self.configure_callbacks = []
        self.db = db
        self.command = command
        self.directory = str(directory)
        self.alembic_ctx_kwargs = kwargs
        self.alembic_ctx_kwargs['compare_type'] = compare_type
        self.alembic_ctx_kwargs['render_as_batch'] = render_as_batch
        if app is not None and db is not None:
            self.init_app(app, db, directory)

    def init_app(self, app, db=None, directory=None, command=None,
                 compare_type=None, render_as_batch=None, **kwargs):
        self.db = db or self.db
        self.command = command or self.command
        self.directory = str(directory or self.directory)
        self.alembic_ctx_kwargs.update(kwargs)
        if compare_type is not None:
            self.alembic_ctx_kwargs['compare_type'] = compare_type
        if render_as_batch is not None:
            self.alembic_ctx_kwargs['render_as_batch'] = render_as_batch
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions['migrate'] = _MigrateConfig(
            self, self.db, **self.alembic_ctx_kwargs)

        from flask_migrate.cli import db as db_cli_group
        app.cli.add_command(db_cli_group, name=self.command)

    def configure(self, f):
        self.configure_callbacks.append(f)
        return f

    def call_configure_callbacks(self, config):
        for f in self.configure_callbacks:
            config = f(config)
        return config

    def get_config(self, directory=None, x_arg=None, opts=None):
        if directory is None:
            directory = self.directory
        directory = str(directory)
        config = Config(os.path.join(directory, 'alembic.ini'))
        config.set_main_option('script_location', directory)
        if config.cmd_opts is None:
            config.cmd_opts = argparse.Namespace()
        for opt in opts or []:
            setattr(config.cmd_opts, opt, True)
        if not hasattr(config.cmd_opts, 'x'):
            setattr(config.cmd_opts, 'x', [])
            for x in getattr(g, 'x_arg', []):
                config.cmd_opts.x.append(x)
            if x_arg is not None:
                if isinstance(x_arg, list) or isinstance(x_arg, tuple):
                    for x in x_arg:
                        config.cmd_opts.x.append(x)
                else:
                    config.cmd_opts.x.append(x_arg)
        return self.call_configure_callbacks(config)


def catch_errors(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            f(*args, **kwargs)
        except (CommandError, RuntimeError) as exc:
            log.error('Error: ' + str(exc))
            sys.exit(1)
    return wrapped


@catch_errors
def list_templates():
    """List available templates."""
    config = Config()
    config.print_stdout("Available templates:\n")
    for tempname in sorted(os.listdir(config.get_template_directory())):
        with open(
            os.path.join(config.get_template_directory(), tempname, "README")
        ) as readme:
            synopsis = next(readme).strip()
        config.print_stdout("%s - %s", tempname, synopsis)


@catch_errors
def init(directory=None, multidb=False, template=None, package=False):
    """Creates a new migration repository"""
    if directory is None:
        directory = current_app.extensions['migrate'].directory
    template_directory = None
    if template is not None and ('/' in template or '\\' in template):
        template_directory, template = os.path.split(template)
    config = Config(template_directory=template_directory)
    config.set_main_option('script_location', directory)
    config.config_file_name = os.path.join(directory, 'alembic.ini')
    config = current_app.extensions['migrate'].\
        migrate.call_configure_callbacks(config)
    if multidb and template is None:
        template = 'flask-multidb'
    elif template is None:
        template = 'flask'
    command.init(config, directory, template=template, package=package)


@catch_errors
def revision(directory=None, message=None, autogenerate=False, sql=False,
             head='head', splice=False, branch_label=None, version_path=None,
             rev_id=None):
    """Create a new revision file."""
    opts = ['autogenerate'] if autogenerate else None
    config = current_app.extensions['migrate'].migrate.get_config(
        directory, opts=opts)
    command.revision(config, message, autogenerate=autogenerate, sql=sql,
                     head=head, splice=splice, branch_label=branch_label,
                     version_path=version_path, rev_id=rev_id)


@catch_errors
def migrate(directory=None, message=None, sql=False, head='head', splice=False,
            branch_label=None, version_path=None, rev_id=None, x_arg=None):
    """Alias for 'revision --autogenerate'"""
    config = current_app.extensions['migrate'].migrate.get_config(
        directory, opts=['autogenerate'], x_arg=x_arg)
    command.revision(config, message, autogenerate=True, sql=sql,
                     head=head, splice=splice, branch_label=branch_label,
                     version_path=version_path, rev_id=rev_id)


@catch_errors
def edit(directory=None, revision='current'):
    """Edit current revision."""
    if alembic_version >= (0, 8, 0):
        config = current_app.extensions['migrate'].migrate.get_config(
            directory)
        command.edit(config, revision)
    else:
        raise RuntimeError('Alembic 0.8.0 or greater is required')


@catch_errors
def merge(directory=None, revisions='', message=None, branch_label=None,
          rev_id=None):
    """Merge two revisions together.  Creates a new migration file"""
    config = current_app.extensions['migrate'].migrate.get_config(directory)
    command.merge(config, revisions, message=message,
                  branch_label=branch_label, rev_id=rev_id)


@catch_errors
def upgrade(directory=None, revision='head', sql=False, tag=None, x_arg=None):
    """Upgrade to a later version"""
    config = current_app.extensions['migrate'].migrate.get_config(directory,
                                                                  x_arg=x_arg)
    command.upgrade(config, revision, sql=sql, tag=tag)


@catch_errors
def downgrade(directory=None, revision='-1', sql=False, tag=None, x_arg=None):
    """Revert to a previous version"""
    config = current_app.extensions['migrate'].migrate.get_config(directory,
                                                                  x_arg=x_arg)
    if sql and revision == '-1':
        revision = 'head:-1'
    command.downgrade(config, revision, sql=sql, tag=tag)


@catch_errors
def show(directory=None, revision='head'):
    """Show the revision denoted by the given symbol."""
    config = current_app.extensions['migrate'].migrate.get_config(directory)
    command.show(config, revision)


@catch_errors
def history(directory=None, rev_range=None, verbose=False,
            indicate_current=False):
    """List changeset scripts in chronological order."""
    config = current_app.extensions['migrate'].migrate.get_config(directory)
    if alembic_version >= (0, 9, 9):
        command.history(config, rev_range, verbose=verbose,
                        indicate_current=indicate_current)
    else:
        command.history(config, rev_range, verbose=verbose)


@catch_errors
def heads(directory=None, verbose=False, resolve_dependencies=False):
    """Show current available heads in the script directory"""
    config = current_app.extensions['migrate'].migrate.get_config(directory)
    command.heads(config, verbose=verbose,
                  resolve_dependencies=resolve_dependencies)


@catch_errors
def branches(directory=None, verbose=False):
    """Show current branch points"""
    config = current_app.extensions['migrate'].migrate.get_config(directory)
    command.branches(config, verbose=verbose)


@catch_errors
def current(directory=None, verbose=False):
    """Display the current revision for each database."""
    config = current_app.extensions['migrate'].migrate.get_config(directory)
    command.current(config, verbose=verbose)


@catch_errors
def stamp(directory=None, revision='head', sql=False, tag=None, purge=False):
    """'stamp' the revision table with the given revision; don't run any
    migrations"""
    config = current_app.extensions['migrate'].migrate.get_config(directory)
    command.stamp(config, revision, sql=sql, tag=tag, purge=purge)


@catch_errors
def check(directory=None):
    """Check if there are any new operations to migrate"""
    config = current_app.extensions['migrate'].migrate.get_config(directory)
    command.check(config)
