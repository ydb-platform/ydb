# Django contrib package

## Usage

See https://procrastinate.readthedocs.io/en/stable/howto/django.html

## Contributing: Migrations

Whenever a migration is added to procrastinate/sql/migrations, you should
add a corresponding Django migration in procrastinate/contrib/django/migrations.

For now, this is a manual process. Look at existing migrations and copy one
of them, you'll need to change:

-   the reference to the SQL migration file name
-   the name of the django migration
-   the name of the previous migration
