# -*- coding: utf-8 -*-
from ydb.public.tools.lib import cmds
from library.python.testing.recipe import declare_recipe


if __name__ == "__main__":
    declare_recipe(cmds.start_recipe, cmds.stop_recipe)
