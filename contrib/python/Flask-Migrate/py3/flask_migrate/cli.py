import click
from flask import g
from flask.cli import with_appcontext
from flask_migrate import list_templates as _list_templates
from flask_migrate import init as _init
from flask_migrate import revision as _revision
from flask_migrate import migrate as _migrate
from flask_migrate import edit as _edit
from flask_migrate import merge as _merge
from flask_migrate import upgrade as _upgrade
from flask_migrate import downgrade as _downgrade
from flask_migrate import show as _show
from flask_migrate import history as _history
from flask_migrate import heads as _heads
from flask_migrate import branches as _branches
from flask_migrate import current as _current
from flask_migrate import stamp as _stamp
from flask_migrate import check as _check


@click.group()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('-x', '--x-arg', multiple=True,
              help='Additional arguments consumed by custom env.py scripts')
@with_appcontext
def db(directory, x_arg):
    """Perform database migrations."""
    g.directory = directory
    g.x_arg = x_arg  # these will be picked up by Migrate.get_config()


@db.command()
@with_appcontext
def list_templates():
    """List available templates."""
    _list_templates()


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('--multidb', is_flag=True,
              help=('Support multiple databases'))
@click.option('-t', '--template', default=None,
              help=('Repository template to use (default is "flask")'))
@click.option('--package', is_flag=True,
              help=('Write empty __init__.py files to the environment and '
                    'version locations'))
@with_appcontext
def init(directory, multidb, template, package):
    """Creates a new migration repository."""
    _init(directory or g.directory, multidb, template, package)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('-m', '--message', default=None, help='Revision message')
@click.option('--autogenerate', is_flag=True,
              help=('Populate revision script with candidate migration '
                    'operations, based on comparison of database to model'))
@click.option('--sql', is_flag=True,
              help=('Don\'t emit SQL to database - dump to standard output '
                    'instead'))
@click.option('--head', default='head',
              help=('Specify head revision or <branchname>@head to base new '
                    'revision on'))
@click.option('--splice', is_flag=True,
              help=('Allow a non-head revision as the "head" to splice onto'))
@click.option('--branch-label', default=None,
              help=('Specify a branch label to apply to the new revision'))
@click.option('--version-path', default=None,
              help=('Specify specific path from config for version file'))
@click.option('--rev-id', default=None,
              help=('Specify a hardcoded revision id instead of generating '
                    'one'))
@with_appcontext
def revision(directory, message, autogenerate, sql, head, splice, branch_label,
             version_path, rev_id):
    """Create a new revision file."""
    _revision(directory or g.directory, message, autogenerate, sql, head,
              splice, branch_label, version_path, rev_id)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('-m', '--message', default=None, help='Revision message')
@click.option('--sql', is_flag=True,
              help=('Don\'t emit SQL to database - dump to standard output '
                    'instead'))
@click.option('--head', default='head',
              help=('Specify head revision or <branchname>@head to base new '
                    'revision on'))
@click.option('--splice', is_flag=True,
              help=('Allow a non-head revision as the "head" to splice onto'))
@click.option('--branch-label', default=None,
              help=('Specify a branch label to apply to the new revision'))
@click.option('--version-path', default=None,
              help=('Specify specific path from config for version file'))
@click.option('--rev-id', default=None,
              help=('Specify a hardcoded revision id instead of generating '
                    'one'))
@click.option('-x', '--x-arg', multiple=True,
              help='Additional arguments consumed by custom env.py scripts')
@with_appcontext
def migrate(directory, message, sql, head, splice, branch_label, version_path,
            rev_id, x_arg):
    """Autogenerate a new revision file (Alias for
    'revision --autogenerate')"""
    _migrate(directory or g.directory, message, sql, head, splice,
             branch_label, version_path, rev_id, x_arg or g.x_arg)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.argument('revision', default='head')
@with_appcontext
def edit(directory, revision):
    """Edit a revision file"""
    _edit(directory or g.directory, revision)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('-m', '--message', default=None, help='Merge revision message')
@click.option('--branch-label', default=None,
              help=('Specify a branch label to apply to the new revision'))
@click.option('--rev-id', default=None,
              help=('Specify a hardcoded revision id instead of generating '
                    'one'))
@click.argument('revisions', nargs=-1)
@with_appcontext
def merge(directory, message, branch_label, rev_id, revisions):
    """Merge two revisions together, creating a new revision file"""
    _merge(directory or g.directory, revisions, message, branch_label, rev_id)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('--sql', is_flag=True,
              help=('Don\'t emit SQL to database - dump to standard output '
                    'instead'))
@click.option('--tag', default=None,
              help=('Arbitrary "tag" name - can be used by custom env.py '
                    'scripts'))
@click.option('-x', '--x-arg', multiple=True,
              help='Additional arguments consumed by custom env.py scripts')
@click.argument('revision', default='head')
@with_appcontext
def upgrade(directory, sql, tag, x_arg, revision):
    """Upgrade to a later version"""
    _upgrade(directory or g.directory, revision, sql, tag, x_arg or g.x_arg)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('--sql', is_flag=True,
              help=('Don\'t emit SQL to database - dump to standard output '
                    'instead'))
@click.option('--tag', default=None,
              help=('Arbitrary "tag" name - can be used by custom env.py '
                    'scripts'))
@click.option('-x', '--x-arg', multiple=True,
              help='Additional arguments consumed by custom env.py scripts')
@click.argument('revision', default='-1')
@with_appcontext
def downgrade(directory, sql, tag, x_arg, revision):
    """Revert to a previous version"""
    _downgrade(directory or g.directory, revision, sql, tag, x_arg or g.x_arg)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.argument('revision', default='head')
@with_appcontext
def show(directory, revision):
    """Show the revision denoted by the given symbol."""
    _show(directory or g.directory, revision)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('-r', '--rev-range', default=None,
              help='Specify a revision range; format is [start]:[end]')
@click.option('-v', '--verbose', is_flag=True, help='Use more verbose output')
@click.option('-i', '--indicate-current', is_flag=True,
              help=('Indicate current version (Alembic 0.9.9 or greater is '
                    'required)'))
@with_appcontext
def history(directory, rev_range, verbose, indicate_current):
    """List changeset scripts in chronological order."""
    _history(directory or g.directory, rev_range, verbose, indicate_current)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('-v', '--verbose', is_flag=True, help='Use more verbose output')
@click.option('--resolve-dependencies', is_flag=True,
              help='Treat dependency versions as down revisions')
@with_appcontext
def heads(directory, verbose, resolve_dependencies):
    """Show current available heads in the script directory"""
    _heads(directory or g.directory, verbose, resolve_dependencies)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('-v', '--verbose', is_flag=True, help='Use more verbose output')
@with_appcontext
def branches(directory, verbose):
    """Show current branch points"""
    _branches(directory or g.directory, verbose)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('-v', '--verbose', is_flag=True, help='Use more verbose output')
@with_appcontext
def current(directory, verbose):
    """Display the current revision for each database."""
    _current(directory or g.directory, verbose)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@click.option('--sql', is_flag=True,
              help=('Don\'t emit SQL to database - dump to standard output '
                    'instead'))
@click.option('--tag', default=None,
              help=('Arbitrary "tag" name - can be used by custom env.py '
                    'scripts'))
@click.option('--purge', is_flag=True,
              help=('Delete the version in the alembic_version table before '
                    'stamping'))
@click.argument('revision', default='head')
@with_appcontext
def stamp(directory, sql, tag, revision, purge):
    """'stamp' the revision table with the given revision; don't run any
    migrations"""
    _stamp(directory or g.directory, revision, sql, tag, purge)


@db.command()
@click.option('-d', '--directory', default=None,
              help=('Migration script directory (default is "migrations")'))
@with_appcontext
def check(directory):
    """Check if there are any new operations to migrate"""
    _check(directory or g.directory)
