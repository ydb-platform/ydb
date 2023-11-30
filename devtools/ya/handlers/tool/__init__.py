from __future__ import absolute_import
from __future__ import print_function
import os
import sys
import logging

from core.common_opts import CrossCompilationOptions
from core.yarg import (
    ArgConsumer,
    CompositeHandler,
    EnvConsumer,
    SetConstValueHook,
    SetValueHook,
    Options,
    OptsHandler,
    FreeArgConsumer,
    ConfigConsumer,
    ExtendHook,
    ShowHelpException,
    SetAppendHook,
    BaseHook,
)

import app

from build.build_opts import CustomFetcherOptions, SandboxAuthOptions, ToolsOptions
from core.yarg.groups import PRINT_CONTROL_GROUP
from core.yarg.help_level import HelpLevel
from yalibrary.tools import environ, param, resource_id, task_id, tool, tools, toolchain_root, toolchain_sys_libs
from yalibrary.toolscache import lock_resource
from yalibrary.platform_matcher import is_darwin_arm64
import core.config
import core.respawn
import exts.process
import exts.windows

logger = logging.getLogger(__name__)


class ToolYaHandler(CompositeHandler):
    description = 'Execute specific tool'

    @staticmethod
    def common_download_options():
        return [SandboxAuthOptions(), CustomFetcherOptions(), ToolsOptions()]

    def __init__(self):
        CompositeHandler.__init__(
            self,
            description=self.description,
            examples=[
                core.yarg.UsageExample('{prefix} --ya-help', 'Print yatool specific options', good_looking=20),
                core.yarg.UsageExample('{prefix} --print-path', 'Print path to tool executable file', good_looking=10),
                core.yarg.UsageExample(
                    '{prefix} --force-update',
                    'Check tool for updates before the update interval elapses',
                    good_looking=10,
                ),
            ],
        )
        for x in tools():
            self[x.name] = OptsHandler(
                action=app.execute(action=do_tool, respawn=app.RespawnType.OPTIONAL),
                description=x.description,
                visible=x.visible,
                opts=[ToolOptions(x.name)] + self.common_download_options(),
                unknown_args_as_free=True,
            )


class DummyHook(BaseHook):
    def __call__(self, to, *args):
        # type: ("Options", tp.Optional[tp.Any]) -> None
        pass

    @staticmethod
    def need_value():
        return False


class ToolOptions(Options):
    def __init__(self, tool):
        Options.__init__(self)
        self.tool = tool
        self.print_path = None
        self.print_toolchain_path = None
        self.print_toolchain_sys_libs = None
        self.toolchain = None
        self.param = None
        self.platform = None
        self.target_platforms = []
        self.need_task_id = None
        self.need_resource_id = None
        self.show_help = False
        self.tail_args = []
        self.host_platform = None
        self.hide_arm64_host_warning = False
        self.force_update = False
        self.force_refetch = False

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--print-path'],
                help='Only print path to tool, do not execute',
                hook=SetConstValueHook('print_path', True),
            ),
            ArgConsumer(
                ['--print-toolchain-path'],
                help='Print path to toolchain root',
                hook=SetConstValueHook('print_toolchain_path', True),
            ),
            ArgConsumer(
                ['--print-toolchain-sys-libs'],
                help='Print pathes to toolchsin system libraries',
                hook=SetConstValueHook('print_toolchain_sys_libs', True),
            ),
            ArgConsumer(['--platform'], help="Set specific platform", hook=SetValueHook('platform')),
            ArgConsumer(['--host-platform'], help="Set host platform", hook=SetValueHook('host_platform')),
            EnvConsumer('YA_TOOL_HOST_PLATFORM', hook=SetValueHook('host_platform')),
            ArgConsumer(['--toolchain'], help="Specify toolchain", hook=SetValueHook('toolchain')),
            ArgConsumer(['--get-param'], help="Get specified param", hook=SetValueHook('param')),
            ArgConsumer(
                ['--get-resource-id'],
                help="Get resource id for specific platform (the platform should be specified)",
                hook=SetConstValueHook('need_resource_id', True),
            ),
            ArgConsumer(['--get-task-id'], help="Get task id", hook=SetConstValueHook('need_task_id', True)),
            ArgConsumer(['--ya-help'], help="Show help", hook=SetConstValueHook('show_help', True)),
            ArgConsumer(
                ['--target-platform'],
                help='Target platform',
                hook=SetAppendHook('target_platforms', values=CrossCompilationOptions.generate_target_platforms_cxx()),
            ),
            ArgConsumer(
                ['--hide-arm64-host-warning'],
                help='Hide MacOS arm64 host warning',
                hook=SetConstValueHook('hide_arm64_host_warning', True),
                group=PRINT_CONTROL_GROUP,
                visible=HelpLevel.EXPERT if is_darwin_arm64() else False,
            ),
            EnvConsumer('YA_TOOL_HIDE_ARM64_HOST_WARNING', hook=SetConstValueHook('hide_arm64_host_warning', True)),
            ConfigConsumer('hide_arm64_host_warning'),
            ArgConsumer(
                ['--force-update'],
                help='Check tool for updates before the update interval elapses',
                hook=SetConstValueHook('force_update', True),
            ),
            ArgConsumer(['--force-refetch'], help='Refetch toolchain', hook=SetConstValueHook('force_refetch', True)),
            ArgConsumer(['--print-fastpath-error'], help='Print fast path failure error', hook=DummyHook()),
            FreeArgConsumer(help='arg', hook=ExtendHook(name='tail_args')),
        ]

    def postprocess(self):
        if self.show_help:
            raise ShowHelpException()
        if self.toolchain and self.target_platforms:
            raise core.yarg.ArgsValidatingException("Do not use --toolchain and --target-platform args together")
        if self.force_update:
            os.environ['YA_TOOL_FORCE_UPDATE'] = "1"


def _replace(s, transformations):
    for k, v in transformations.items():
        s = s.replace('$({})'.format(k), v)
    return s


def _useful_env_vars():
    return {'YA_TOOL': sys.argv[0]}


def do_tool(params):
    tool_name = params.tool
    extra_args = params.tail_args
    target_platform = params.target_platforms
    host_platform = params.host_platform
    if target_platform:
        if len(target_platform) > 1:
            raise Exception('Multiple target platforms are not supported by this code for now')
        target_platform = target_platform[0]
    else:
        target_platform = None

    if is_darwin_arm64() and not host_platform:
        host_platform = 'darwin'
        if not params.hide_arm64_host_warning:
            try:
                import app_ctx

                app_ctx.display.emit_message("You use x86_64 version of selected tool.")
            except Exception as e:
                logger.error("Can't print arm64 warning message: {}".format(e))

    tool_path = tool(
        tool_name,
        params.toolchain,
        target_platform=target_platform,
        for_platform=host_platform,
        force_refetch=params.force_refetch,
    )
    if exts.windows.on_win() and not tool_path.endswith('.exe'):  # XXX: hack. Think about ya.conf.json format
        logger.debug('Rename tool for win: %s', tool_path)
        tool_path += '.exe'

    lock_result = False
    for_platform = params.platform or params.host_platform or None

    if params.need_task_id:
        tid = task_id(tool_name, params.toolchain)
        if tid is not None:
            print(tid)
        else:
            raise Exception("Tool '{}' has no task id".format(tool_name))
    elif params.need_resource_id:
        print(resource_id(tool_name, params.toolchain, for_platform))
    elif params.param:
        print(param(tool_name, params.toolchain, params.param))
    elif params.print_toolchain_path:
        print(toolchain_root(tool_name, params.toolchain, for_platform))
        lock_result = True
    elif params.print_toolchain_sys_libs:
        print(toolchain_sys_libs(tool_name, params.toolchain, for_platform))
        lock_result = True
    elif params.print_path:
        print(tool_path)
        lock_result = True
    elif os.path.isfile(tool_path):
        env = core.respawn.filter_env(os.environ.copy())

        # Remove environment variables set by 'ya' wrapper.
        # They are actually one-time ya-bin parameters rather than inheritable environment
        # for all descendant processes.
        for key in ('YA_SOURCE_ROOT', 'YA_PYVER_REQUIRE', 'YA_PYVER_SET_FORCED'):
            env.pop(key, None)

        env.update(_useful_env_vars())
        for key, value in environ(tool_name, params.toolchain).items():
            env[key] = _replace(
                os.pathsep.join(value), {'ROOT': toolchain_root(tool_name, params.toolchain, for_platform)}
            )
        if tool_name == 'gdb':
            # gdb does not fit in 8 MB stack with large cores (DEVTOOLS-5040).
            try:
                import resource as r
            except ImportError:
                pass
            else:
                soft, hard = r.getrlimit(r.RLIMIT_STACK)
                new = 128 << 20
                logger.debug("Limit info: soft=%d hard=%d new=%d", soft, hard, new)
                if hard != r.RLIM_INFINITY:
                    new = min(new, hard)
                    logger.debug("Limit info: new=%d", new)
                if new > soft:
                    logger.debug("Limit info: setting new limits=(%d, %d)", new, hard)
                    try:
                        r.setrlimit(r.RLIMIT_STACK, (new, hard))
                    except ValueError as e:
                        logger.error("Failure while setting RLIMIT_STACK ({}, {}), {}".format(new, hard, e))
                        logger.exception("While setting RLIMIT_STACK")
            arc_root = core.config.find_root(fail_on_error=False)
            if arc_root is not None:
                logger.debug('Arcadia root found: %s', arc_root)
                extra_args = ['-ex', 'set substitute-path /-S/ {}/'.format(arc_root)] + extra_args
                extra_args = ['-ex', 'set filename-display absolute'] + extra_args
        if (
            tool_name == 'arc'
            and params.username not in {'sandbox', 'root'}
            and os.getenv('YA_ALLOW_TOOL_ARC', 'no') != 'yes'
        ):
            message = (
                'Please, use natively installed arc, install guide:'
                ' https://docs.yandex-team.ru/devtools/intro/quick-start-guide#arc-setup'
            )
            raise core.yarg.ArgsValidatingException(message)
        exts.process.execve(tool_path, extra_args, env=env)

    if lock_result:
        lock_resource(toolchain_root(tool_name, params.toolchain, for_platform))
