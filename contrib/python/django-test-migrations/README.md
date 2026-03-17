# django-test-migrations

[![wemake.services](https://img.shields.io/badge/%20-wemake.services-green.svg?label=%20&logo=data%3Aimage%2Fpng%3Bbase64%2CiVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAABGdBTUEAALGPC%2FxhBQAAAAFzUkdCAK7OHOkAAAAbUExURQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP%2F%2F%2F5TvxDIAAAAIdFJOUwAjRA8xXANAL%2Bv0SAAAADNJREFUGNNjYCAIOJjRBdBFWMkVQeGzcHAwksJnAPPZGOGAASzPzAEHEGVsLExQwE7YswCb7AFZSF3bbAAAAABJRU5ErkJggg%3D%3D)](https://wemake-services.github.io)
[![Build status](https://github.com/wemake-services/django-test-migrations/workflows/test/badge.svg?branch=master&event=push)](https://github.com/wemake-services/django-test-migrations/actions?query=workflow%3Atest)
[![codecov](https://codecov.io/gh/wemake-services/django-test-migrations/branch/master/graph/badge.svg)](https://codecov.io/gh/wemake-services/django-test-migrations)
[![Python Version](https://img.shields.io/pypi/pyversions/django-test-migrations.svg)](https://pypi.org/project/django-test-migrations/)
![PyPI - Django Version](https://img.shields.io/pypi/djversions/django-test-migrations)
[![wemake-python-styleguide](https://img.shields.io/badge/style-wemake-000000.svg)](https://github.com/wemake-services/wemake-python-styleguide)


## Features

- Allows to test `django` schema and data migrations
- Allows to test both forward and rollback migrations
- Allows to test the migrations order
- Allows to test migration names
- Allows to test database configuration
- Fully typed with annotations and checked with `mypy`, [PEP561 compatible](https://www.python.org/dev/peps/pep-0561/)
- Easy to start: has lots of docs, tests, and tutorials

Read the [announcing post](https://sobolevn.me/2019/10/testing-django-migrations).
See real-world [usage example](https://github.com/wemake-services/wemake-django-template).


## Installation

```bash
pip install django-test-migrations
```

We support several `django` versions:

- `3.2`
- `4.1`
- `4.2`
- `5.0`

Other versions most likely will work too,
but they are not officially supported.


## Testing Django migrations

Testing migrations is not a frequent thing in `django` land.
But, sometimes it is totally required. When?

When we do complex schema or data changes
and what to be sure that existing data won't be corrupted.
We might also want to be sure that all migrations can be safely rolled back.
And as a final touch, we want to be sure that migrations
are in the correct order and have correct dependencies.

### Testing forward migrations

To test all migrations we have a [`Migrator`](https://github.com/wemake-services/django-test-migrations/blob/master/django_test_migrations/migrator.py) class.

It has three methods to work with:

- `.apply_initial_migration()` which takes app and migration names to generate
  a state before the actual migration happens. It creates the `before state`
  by applying all migrations up to and including the ones passed as an argument.

- `.apply_tested_migration()` which takes app and migration names to perform the
  actual migration

- `.reset()` to clean everything up after we are done with testing

So, here's an example:

```python
from django_test_migrations.migrator import Migrator

migrator = Migrator(database='default')

# Initial migration, currently our model has only a single string field:
# Note:
# We are testing migration `0002_someitem_is_clean`, so we are specifying
# the name of the previous migration (`0001_initial`) in the
# .apply_initial_migration() method in order to prepare a state of the database
# before applying the migration we are going to test.
#
old_state = migrator.apply_initial_migration(('main_app', '0001_initial'))
SomeItem = old_state.apps.get_model('main_app', 'SomeItem')

# Let's create a model with just a single field specified:
SomeItem.objects.create(string_field='a')
assert len(SomeItem._meta.get_fields()) == 2  # id + string_field

# Now this migration will add `is_clean` field to the model:
new_state = migrator.apply_tested_migration(
    ('main_app', '0002_someitem_is_clean'),
)
SomeItem = new_state.apps.get_model('main_app', 'SomeItem')

# We can now test how our migration worked, new field is there:
assert SomeItem.objects.filter(is_clean=True).count() == 0
assert len(SomeItem._meta.get_fields()) == 3  # id + string_field + is_clean

# Cleanup:
migrator.reset()
```

That was an example of a forward migration.

### Backward migration

The thing is that you can also test backward migrations.
Nothing really changes except migration names that you pass and your logic:

```python
migrator = Migrator()

# Currently our model has two field, but we need a rollback:
old_state = migrator.apply_initial_migration(
    ('main_app', '0002_someitem_is_clean'),
)
SomeItem = old_state.apps.get_model('main_app', 'SomeItem')

# Create some data to illustrate your cases:
# ...

# Now this migration will drop `is_clean` field:
new_state = migrator.apply_tested_migration(('main_app', '0001_initial'))

# Assert the results:
# ...

# Cleanup:
migrator.reset()
```

### Testing migrations ordering

Sometimes we also want to be sure that our migrations are in the correct order
and that all our `dependencies = [...]` are correct.

To achieve that we have [`plan.py`](https://github.com/wemake-services/django-test-migrations/blob/master/django_test_migrations/plan.py) module.

That's how it can be used:

```python
from django_test_migrations.plan import all_migrations, nodes_to_tuples

main_migrations = all_migrations('default', ['main_app', 'other_app'])
assert nodes_to_tuples(main_migrations) == [
    ('main_app', '0001_initial'),
    ('main_app', '0002_someitem_is_clean'),
    ('other_app', '0001_initial'),
    ('main_app', '0003_update_is_clean'),
    ('main_app', '0004_auto_20191119_2125'),
    ('other_app', '0002_auto_20191120_2230'),
]
```

This way you can be sure that migrations
and apps that depend on each other will be executed in the correct order.

### `factory_boy` integration

If you use factories to create models, you can replace their respective
`.build()` or `.create()` calls with methods of `factory` and pass the
model name and factory class as arguments:

```python
import factory

old_state = migrator.apply_initial_migration(
    ('main_app', '0002_someitem_is_clean'),
)
SomeItem = old_state.apps.get_model('main_app', 'SomeItem')

# instead of
# item = SomeItemFactory.create()
# use this:
factory.create(SomeItem, FACTORY_CLASS=SomeItemFactory)

# ...
```


## Test framework integrations 🐍

We support several test frameworks as first-class citizens.
That's a testing tool after all!

Note that the Django `post_migrate` signal's receiver list is cleared at
the start of tests and restored afterwards. If you need to test your
own `post_migrate` signals then attach/remove them during a test.

### pytest

We ship `django-test-migrations` with a `pytest` plugin
that provides two convenient fixtures:

- `migrator_factory` that gives you an opportunity
  to create `Migrator` classes for any database
- `migrator` instance for the `'default'` database

That's how it can be used:

```python
import pytest

@pytest.mark.django_db
def test_pytest_plugin_initial(migrator):
    """Ensures that the initial migration works."""
    old_state = migrator.apply_initial_migration(('main_app', None))

    with pytest.raises(LookupError):
        # Model does not yet exist:
        old_state.apps.get_model('main_app', 'SomeItem')

    new_state = migrator.apply_tested_migration(('main_app', '0001_initial'))
    # After the initial migration is done, we can use the model state:
    SomeItem = new_state.apps.get_model('main_app', 'SomeItem')
    assert SomeItem.objects.filter(string_field='').count() == 0
```

### unittest

We also ship an integration with the built-in `unittest` framework.

Here's how it can be used:

```python
from django_test_migrations.contrib.unittest_case import MigratorTestCase

class TestDirectMigration(MigratorTestCase):
    """This class is used to test direct migrations."""

    migrate_from = ('main_app', '0002_someitem_is_clean')
    migrate_to = ('main_app', '0003_update_is_clean')

    def prepare(self):
        """Prepare some data before the migration."""
        SomeItem = self.old_state.apps.get_model('main_app', 'SomeItem')
        SomeItem.objects.create(string_field='a')
        SomeItem.objects.create(string_field='a b')

    def test_migration_main0003(self):
        """Run the test itself."""
        SomeItem = self.new_state.apps.get_model('main_app', 'SomeItem')

        assert SomeItem.objects.count() == 2
        assert SomeItem.objects.filter(is_clean=True).count() == 1
```

### Choosing only migrations tests

In CI systems it is important to get instant feedback. Running tests that
apply database migration can slow down tests execution, so it is often a good
idea to run standard, fast, regular unit tests without migrations in parallel
with slower migrations tests.

#### pytest

`django_test_migrations` adds `migration_test` marker to each test using
`migrator_factory` or `migrator` fixture.
To run only migrations test, use `-m` option:

```bash
pytest -m migration_test  # Runs only migration tests
pytest -m "not migration_test"  # Runs all except migration tests
```

#### unittest

`django_test_migrations` adds `migration_test`
[tag](https://docs.djangoproject.com/en/3.0/topics/testing/tools/#tagging-tests)
to every `MigratorTestCase` subclass.
To run only migrations tests, use `--tag` option:

```bash
python mange.py test --tag=migration_test  # Runs only migration tests
python mange.py test --exclude-tag=migration_test  # Runs all except migration tests
```


## Django Checks

`django_test_migrations` comes with 2 groups of Django's checks for:

+ detecting migrations scripts automatically generated names
+ validating some subset of database settings

### Testing migration names

`django` generates migration names for you when you run `makemigrations`.
These names are bad ([read more](https://adamj.eu/tech/2020/02/24/how-to-disallow-auto-named-django-migrations/) about why it is bad)!
Just look at this: `0004_auto_20191119_2125.py`

What does this migration do? What changes does it have?

One can also pass `--name` attribute when creating migrations, but it is easy to forget.

We offer an automated solution: `django` check
that produces an error for each badly named migration.

Add our check into your `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    # ...

    # Our custom check:
    'django_test_migrations.contrib.django_checks.AutoNames',
]
```

Then in your CI run:

```bash
python manage.py check --deploy
```

This way you will be safe from wrong names in your migrations.

Do you have a migrations that cannot be renamed? Add them to the ignore list:

```python
# settings.py

DTM_IGNORED_MIGRATIONS = {
    ('main_app', '0004_auto_20191119_2125'),
    ('dependency_app', '0001_auto_20201110_2100'),
}
```

Then we won't complain about them.

Or you can completely ignore entire app:

```python
# settings.py

DTM_IGNORED_MIGRATIONS = {
    ('dependency_app', '*'),
    ('another_dependency_app', '*'),
}
```

### Database configuration

Add our check to `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    # ...

    # Our custom check:
    'django_test_migrations.contrib.django_checks.DatabaseConfiguration',
]
```

Then just run `check` management command in your CI like listed in section
above.


## Related projects

You might also like:

- [django-migration-linter](https://github.com/3YOURMIND/django-migration-linter) - Detect backward incompatible migrations for your django project.
- [wemake-django-template](https://github.com/wemake-services/wemake-django-template/) - Bleeding edge django template focused on code quality and security with both `django-test-migrations` and `django-migration-linter` on board.


## Credits

This project is based on work of other awesome people:

- [@asfaltboy](https://gist.github.com/asfaltboy/b3e6f9b5d95af8ba2cc46f2ba6eae5e2)
- [@blueyed](https://gist.github.com/blueyed/4fb0a807104551f103e6)
- [@fernandogrd](https://gist.github.com/blueyed/4fb0a807104551f103e6#gistcomment-1546191)
- [@adamchainz](https://adamj.eu/tech/2020/02/24/how-to-disallow-auto-named-django-migrations/)


## License

MIT.
