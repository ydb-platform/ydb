import app

from build.build_handler import do_ya_make
from build.build_opts import ya_make_options

import core.yarg


class YaTestYaHandler(core.yarg.OptsHandler):
    description = 'Build and run all tests\n[[imp]]ya test[[rst]] is alias for [[imp]]ya make -A[[rst]]'

    def __init__(self):
        core.yarg.OptsHandler.__init__(
            self,
            action=app.execute(action=do_ya_make),
            examples=[
                core.yarg.UsageExample(
                    '{prefix}',
                    'Build and run all tests',
                    good_looking=101,
                ),
                core.yarg.UsageExample(
                    '{prefix} -t',
                    'Build and run small tests only',
                    good_looking=102,
                ),
                core.yarg.UsageExample(
                    '{prefix} -tt',
                    'Build and run small and medium tests',
                    good_looking=103,
                ),
            ],
            description=self.description,
            opts=ya_make_options(
                free_build_targets=True,
                is_ya_test=True,
                strip_idle_build_results=True,
            ),
            visible=True,
        )
