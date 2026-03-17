# Django migration linter

Detect backward incompatible migrations for your Django project.
It will save you time by making sure migrations will not break with a older codebase.

[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2F3YOURMIND%2Fdjango-migration-linter%2Fbadge%3Fref%3Dmain&style=flat)](https://actions-badge.atrox.dev/3YOURMIND/django-migration-linter/goto?ref=main)
[![PyPI](https://img.shields.io/pypi/v/django-migration-linter.svg)](https://pypi.python.org/pypi/django-migration-linter/)
[![PR_Welcome](https://img.shields.io/badge/PR-welcome-green.svg)](https://github.com/3YOURMIND/django-migration-linter/pulls)
[![3YD_Hiring](https://img.shields.io/badge/3YOURMIND-Hiring-brightgreen.svg)](https://www.3yourmind.com/career)
[![GitHub_Stars](https://img.shields.io/github/stars/3YOURMIND/django-migration-linter.svg?style=social&label=Stars)](https://github.com/3YOURMIND/django-migration-linter/stargazers)

## Quick installation

```
pip install django-migration-linter
```

And add the migration linter to your ``INSTALLED_APPS``:
```
INSTALLED_APPS = [
    ...,
    "django_migration_linter",
    ...,
]
```

Optionally, add a configuration:
```
MIGRATION_LINTER_OPTIONS = {
    ...
}
```

For details about configuration options, checkout [Usage](docs/usage.md).

## Usage example

```
$ python manage.py lintmigrations

(app_add_not_null_column, 0001_create_table)... OK
(app_add_not_null_column, 0002_add_new_not_null_field)... ERR
        NOT NULL constraint on columns
(app_drop_table, 0001_initial)... OK
(app_drop_table, 0002_delete_a)... ERR
        DROPPING table
(app_ignore_migration, 0001_initial)... OK
(app_ignore_migration, 0002_ignore_migration)... IGNORE
(app_rename_table, 0001_initial)... OK
(app_rename_table, 0002_auto_20190414_1500)... ERR
        RENAMING tables

*** Summary ***
Valid migrations: 4/8
Erroneous migrations: 3/8
Migrations with warnings: 0/8
Ignored migrations: 1/8
```

The linter analysed all migrations from the Django project.
It found 3 migrations that are doing backward incompatible operations and 1 that is explicitly ignored.
The list of incompatibilities that the linter analyses [can be found at docs/incompatibilities.md](./docs/incompatibilities.md).

More advanced usages of the linter and options [can be found at docs/usage.md](./docs/usage.md).

## Integration

One can either integrate the linter in the CI using its `lintmigrations` command, or detect incompatibilities during generation of migrations with
```
$ python manage.py makemigrations --lint

Migrations for 'app_correct':
  tests/test_project/app_correct/migrations/0003_a_column.py
    - Add field column to a
Linting for 'app_correct':
(app_correct, 0003_a_column)... ERR
        NOT NULL constraint on columns

The migration linter detected that this migration is not backward compatible.
- If you keep the migration, you will want to fix the issue or ignore the migration.
- By default, the newly created migration file will be deleted.
Do you want to keep the migration? [y/N] n
Deleted tests/test_project/app_correct/migrations/0003_a_column.py
```

The linter found that the newly created migration is not backward compatible and deleted the file after confirmation.
This behaviour can be the default of the `makemigrations` command through the `MIGRATION_LINTER_OVERRIDE_MAKEMIGRATIONS` Django setting.
Find out more about the [makemigrations command at docs/makemigrations.md](./docs/makemigrations.md).

### More information

Please find more documentation [in the docs/ folder](./docs/).

Some implementation details [can be found in the ./docs/internals/ folder](./docs/internals/).

### Blog post

* [Keeping Django database migrations backward compatible](https://medium.com/3yourmind/keeping-django-database-migrations-backward-compatible-727820260dbb)
* [Django and its default values](https://medium.com/botify-labs/django-and-its-default-values-c21a13cff9f)

### They talk about the linter

* [Django News](https://django-news.com/issues/6?m=web#uMmosw7)
* [wemake-django-template](https://wemake-django-template.readthedocs.io/en/latest/pages/template/linters.html#django-migration-linter)
* [Testing Django migrations on sobolevn's blog](https://sobolevn.me/2019/10/testing-django-migrations#existing-setup)

### Related

* [django-test-migrations](https://github.com/wemake-services/django-test-migrations) - Test django schema and data migrations, including migrations' order and best practices.

### License

*django-migration-linter* is released under the [Apache 2.0 License](./LICENSE).

##### Maintained by [David Wobrock](https://github.com/David-Wobrock)
