from library.python.testing.recipe import declare_recipe

from ydb.tests.library.logbroker_federation import LogbrokerFederation


_recipe_instance = None


def start(args):
    global _recipe_instance
    _recipe_instance = LogbrokerFederation(accounts=["prod", "test", "admin"])
    _recipe_instance.start(args)


def stop(args):
    if _recipe_instance is not None:
        _recipe_instance.stop(args)


if __name__ == "__main__":
    declare_recipe(start, stop)
