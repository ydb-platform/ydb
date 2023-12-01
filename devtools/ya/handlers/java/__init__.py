from __future__ import absolute_import
from . import helpers

import app
import core.yarg
from build import build_opts
import devtools.ya.test.opts as test_opts


def default_options():
    return [
        build_opts.BuildTargetsOptions(with_free=True),
        build_opts.BeVerboseOptions(),
        build_opts.ShowHelpOptions(),
        build_opts.YMakeDebugOptions(),
        build_opts.YMakeBinOptions(),
        build_opts.YMakeRetryOptions(),
        build_opts.FlagsOptions(),
    ]


class JavaYaHandler(core.yarg.CompositeHandler):
    def __init__(self):
        super(JavaYaHandler, self).__init__(description='Java build helpers')

        self['dependency-tree'] = core.yarg.OptsHandler(
            action=app.execute(action=helpers.print_ymake_dep_tree),
            description='Print dependency tree',
            opts=default_options() + [build_opts.BuildTypeOptions('release')],
            visible=True,
        )
        self['classpath'] = core.yarg.OptsHandler(
            action=app.execute(action=helpers.print_classpath),
            description='Print classpath',
            opts=default_options() + [build_opts.BuildTypeOptions('release')],
            visible=True,
        )
        self['test-classpath'] = core.yarg.OptsHandler(
            action=app.execute(action=helpers.print_test_classpath),
            description='Print run classpath for test module',
            opts=default_options() + [test_opts.RunTestOptions()],
            visible=True,
        )
        self['find-all-paths'] = core.yarg.OptsHandler(
            action=app.execute(action=helpers.find_all_paths),
            description='Find all PEERDIR paths of between two targets',
            opts=default_options() + [build_opts.FindPathOptions()],
            visible=True,
        )
