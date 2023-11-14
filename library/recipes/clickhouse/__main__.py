from library.python.testing.recipe import declare_recipe
from library.recipes.clickhouse.recipe import start
from library.recipes.clickhouse.recipe import stop


if __name__ == '__main__':
    declare_recipe(start, stop)
