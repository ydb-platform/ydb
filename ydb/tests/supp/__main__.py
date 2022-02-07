# -*- coding: utf-8 -*-
import os
import sys
import yatest.common

from library.python.testing.recipe import declare_recipe
from library.python.testing.recipe import set_env


def setup_suppression():
    supp_txt = yatest.common.source_path(os.getenv("YDB_UBSAN_OPTIONS"))
    assert os.path.exists(supp_txt)

    with open(supp_txt, 'r') as supp:
        sys.stderr.write("Using suppressions:\n=====\n%s" % supp.read())

    set_env(
        "UBSAN_OPTIONS",
        "suppressions=%s" % supp_txt
    )


def stop():
    pass


if __name__ == "__main__":
    declare_recipe(
        lambda args: setup_suppression(),
        lambda args: stop(),
    )
