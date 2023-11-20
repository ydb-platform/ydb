## PostgreSQL recipe
This recipe allows to add PostgreSQL to the suite objects. 

### Usage
Add the following include to the tests's ya.make:

    INCLUDE(${ARCADIA_ROOT}/library/recipes/postgresql/recipe.inc)
 
Sets environment variables:
    POSTGRES_RECIPE_HOST
    POSTGRES_RECIPE_PORT
    POSTGRES_RECIPE_DBNAME
    POSTGRES_RECIPE_USER
    POSTGRES_RECIPE_MAX_CONNECTIONS
