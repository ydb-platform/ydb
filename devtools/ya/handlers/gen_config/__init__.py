from __future__ import absolute_import
import core.common_opts
import core.yarg

from . import gen_config


class GenConfigOptions(core.yarg.Options):
    def __init__(self):
        self.output = None
        self.dump_defaults = False

    @staticmethod
    def consumer():
        return [
            core.yarg.SingleFreeArgConsumer(
                help='ya.conf',
                hook=core.yarg.SetValueHook('output'),
                required=False,
            ),
            core.yarg.ArgConsumer(
                ['--dump-defaults'],
                help='Dump default values as JSON',
                hook=core.yarg.SetConstValueHook('dump_defaults', True),
            ),
        ]


class GenConfigYaHandler(core.yarg.OptsHandler):
    description = 'Generate default ya config'

    def __init__(self):
        self._root_handler = None
        super(GenConfigYaHandler, self).__init__(
            action=self.do_generate,
            description=self.description,
            opts=[
                core.common_opts.ShowHelpOptions(),
                GenConfigOptions(),
            ],
        )

    def handle(self, root_handler, args, prefix):
        self._root_handler = root_handler
        super(GenConfigYaHandler, self).handle(root_handler, args, prefix)

    def do_generate(self, args):
        gen_config.generate_config(self._root_handler, args.output, args.dump_defaults)
