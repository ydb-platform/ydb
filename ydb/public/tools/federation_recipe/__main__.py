from library.python.testing.recipe import declare_recipe

from ydb.tests.library.logbroker_federation import LogbrokerFederation


STATE_FILE = "federation_recipe.json"


def start(args):
    LogbrokerFederation.stop_from_file(STATE_FILE)
    federation = LogbrokerFederation(process_state_file=STATE_FILE)
    try:
        federation.start(args)
    except BaseException:
        try:
            federation.stop(args)
        finally:
            stop(args)
        raise


def stop(args):
    LogbrokerFederation.stop_from_file(STATE_FILE)


if __name__ == "__main__":
    declare_recipe(start, stop)
