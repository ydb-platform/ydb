from __future__ import absolute_import
from __future__ import print_function
import csv
import pickle
import exts.yjson as json
import logging
import os
import sys
import enum
import six

from build.build_facade import (
    gen_all_loops,
    gen_dir_loops,
    gen_graph,
    gen_licenses_list,
    gen_forced_deps_list,
    gen_modules,
    gen_module_info,
    gen_plan,
    gen_json_graph,
    gen_uids,
    gen_srcdeps,
    gen_dependencies,
    gen_test_dart,
    gen_conf,
    gen_filelist,
    gen_relation,
)
from handlers.dump.gen_conf_docs import dump_mmm_docs
from build.dir_graph import reachable, gen_dir_graph
from build.compilation_database import dump_compilation_database, COMPILATION_DATABASE_OPTS
from exts.strtobool import strtobool
from exts.tmp import temp_dir
from core.yarg import (
    CompositeHandler,
    OptsHandler,
    ArgConsumer,
    FreeArgConsumer,
    SetValueHook,
    Options,
    SetConstValueHook,
    SetConstAppendHook,
    UpdateValueHook,
    SetAppendHook,
    ExtendHook,
    ArgsValidatingException,
    Group,
)
from yalibrary.vcs import vcsversion
from core.imprint import imprint
from build.build_opts import (
    YMakeDebugOptions,
    YMakeBinOptions,
    FlagsOptions,
    CustomFetcherOptions,
    ToolsOptions,
    SandboxAuthOptions,
    JavaSpecificOptions,
)
from build.build_opts import (
    BuildTypeOptions,
    BuildTargetsOptions,
    ShowHelpOptions,
    CustomBuildRootOptions,
    ContinueOnFailOptions,
)
from build.build_opts import YMakeRetryOptions, ConfigurationPresetsOptions
from core.common_opts import CrossCompilationOptions, YaBin3Options
from test.explore import generate_tests_by_dart
from devtools.ya.test.dartfile import decode_recipe_cmdline

import app
import app_config

logger = logging.getLogger(__name__)


class DumpYaHandler(CompositeHandler):
    common_opts = [ShowHelpOptions()]

    @staticmethod
    def common_build_facade_opts(with_free_targets=True):
        return [
            FlagsOptions(),
            CustomFetcherOptions(),
            SandboxAuthOptions(),
            YMakeBinOptions(),
            YMakeRetryOptions(),
            CrossCompilationOptions(),
            BuildTypeOptions('release'),
            ContinueOnFailOptions(),
            BuildTargetsOptions(with_free=with_free_targets),
            ToolsOptions(),
            ConfigurationPresetsOptions(),
            YaBin3Options(),
        ]

    def __init__(self):
        CompositeHandler.__init__(self, description='Repository related information')
        self['modules'] = OptsHandler(
            action=app.execute(action=do_modules),
            description='All modules',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                DepTraverseOptions(),
                DepFilterOptions(),
                PeerDirectoriesOptions(),
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
            ],
        )
        self['module-info'] = OptsHandler(
            action=app.execute(action=do_module_info),
            description='Modules info',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                DepTraverseOptions(),
                DepFilterOptions(),
                PeerDirectoriesOptions(),
                DataOptions(),
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
                DumpModuleInfoOptions(),
            ],
        )
        self['src-deps'] = OptsHandler(
            action=app.execute(action=do_dump_srcdeps),
            description='Dump of all source dependencies',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                DumpSrcDepsOptions(),
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
            ],
        )
        self['dir-graph'] = OptsHandler(
            action=app.execute(action=do_dir_graph),
            description='Dependencies between directories',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                DumpDirOptions(),
                SplitByTypeOptions(),
                DumpTestListOptions(),
                DepTraverseOptions(),
                DepFilterOptions(),
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
                LegacyDepsOptions(),
            ],
        )
        self['dep-graph'] = OptsHandler(
            action=app.execute(action=do_dep_graph),
            description='Dependency internal graph',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                DepGraphOutputOptions(),
                DepTraverseOptions(),
                DepFilterOptions(),
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
                LegacyDepsOptions(),
            ],
        )
        self['dot-graph'] = OptsHandler(
            action=app.execute(action=do_dot_graph),
            description='Dependency between directories in dot format',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                DepTraverseOptions(),
                DepFilterOptions(),
                PeerDirectoriesOptions(),
                CustomBuildRootOptions(),
            ],
        )
        self['json-dep-graph'] = OptsHandler(
            action=app.execute(action=do_json_dep_graph),
            description='Dependency graph as json',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                DepTraverseOptions(),
                DepFilterOptions(),
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
                LegacyDepsOptions(),
            ],
        )
        self['build-plan'] = OptsHandler(
            action=app.execute(action=do_build_plan),
            description='Build plan',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                DepTraverseOptions(),
                CustomBuildRootOptions(),
                YMakeDebugOptions(),
            ],
        )
        self['compile-commands'] = OptsHandler(
            action=app.execute(action=dump_compilation_database),
            description='JSON compilation database',
            opts=COMPILATION_DATABASE_OPTS + [ToolsOptions(), CustomFetcherOptions(), SandboxAuthOptions()],
        )
        self['compilation-database'] = OptsHandler(
            action=app.execute(action=dump_compilation_database),
            description='Alias for compile-commands',
            opts=COMPILATION_DATABASE_OPTS + [ToolsOptions(), CustomFetcherOptions(), SandboxAuthOptions()],
        )
        self['relation'] = OptsHandler(
            action=app.execute(action=do_relation),
            description='PEERDIR relations.  Please don\'t run from the arcadia root.',
            opts=self.common_opts
            + self.common_build_facade_opts(False)
            + [
                RelationSrcDstOptions(free_dst=True),
                DepTraverseOptions(),
                DepFilterOptions(),
                PeerDirectoriesOptions(),
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
            ],
        )
        self['all-relations'] = OptsHandler(
            action=app.execute(action=do_all_relations),
            description='All relations between internal graph nodes in dot format. Please don\'t run from the arcadia root.',
            opts=self.common_opts
            + self.common_build_facade_opts(False)
            + [
                RelationSrcDstOptions(free_dst=True),
                DumpAllRelationsOptions(),
                DepTraverseOptions(),
                DepFilterOptions(),
                PeerDirectoriesOptions(),
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
            ],
        )
        self['files'] = OptsHandler(
            action=app.execute(action=do_files),
            description='File list',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                FilesFilterOptions(),
                DepTraverseOptions(),
                DepFilterOptions(),
                DataOptions(),
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
            ],
        )
        self['loops'] = OptsHandler(
            action=app.execute(action=do_loops),
            description='All loops in arcadia',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                DepTraverseOptions(),
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
            ],
        )
        self['peerdir-loops'] = OptsHandler(
            action=app.execute(action=do_peerdir_loops),
            description='Loops by peerdirs',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                DepTraverseOptions(),
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
            ],
        )
        self['imprint'] = OptsHandler(
            action=app.execute(action=do_imprint),
            description='Directory imprint',
            opts=self.common_opts + [CustomFetcherOptions(), SandboxAuthOptions(), ToolsOptions()],
        )
        self['uids'] = OptsHandler(
            action=app.execute(action=do_uids),
            description='All targets uids',
            opts=self.common_opts + self.common_build_facade_opts(),
            visible=False,
        )
        self['test-list'] = OptsHandler(
            action=app.execute(action=do_test_list),
            description='All test entries',
            opts=self.common_opts + self.common_build_facade_opts() + [DumpTestListOptions()],
        )
        self['json-test-list'] = OptsHandler(
            action=app.execute(action=do_json_test_list),
            description='All test entries as json',
            opts=self.common_opts + self.common_build_facade_opts() + [DumpTestListOptions()],
        )
        self['conf'] = OptsHandler(
            action=app.execute(action=do_conf),
            description='Print build conf',
            opts=self.common_opts
            + [
                FlagsOptions(),
                CustomFetcherOptions(),
                SandboxAuthOptions(),
                CrossCompilationOptions(),
                ToolsOptions(),
                BuildTypeOptions('release'),
                JavaSpecificOptions(),
            ],
            visible=False,
        )
        self['licenses'] = OptsHandler(
            action=app.execute(action=do_licenses),
            description='Print known licenses grouped by their properties',
            opts=self.common_opts
            + self.common_build_facade_opts()
            + [
                YMakeDebugOptions(),
                CustomBuildRootOptions(),
                DumpLicenseOptions(),
            ],
            visible=False,
        )
        self['forced-deps'] = OptsHandler(
            action=app.execute(action=do_forced_deps),
            description='Print known forced dependency management',
            opts=FullForcedDepsOptions(),
            visible=False,
        )
        self['conf-docs'] = OptsHandler(
            action=app.execute(action=do_conf_docs),
            description='Print descriptions of entities (modules, macros, multimodules, etc.)',
            opts=self.common_opts + self.common_build_facade_opts() + [DumpDescriptionOptions()],
        )
        self['root'] = OptsHandler(
            action=app.execute(lambda params: sys.stdout.write(params.arc_root) and 0),
            description='Print Arcadia root',
            opts=self.common_opts,
        )
        self['vcs-info'] = OptsHandler(
            action=app.execute(action=do_dump_vcs_info),
            description='Print VCS revision information.',
            opts=self.common_opts + self.common_build_facade_opts(),
            visible=False,
        )
        self['raw-vcs-info'] = OptsHandler(
            action=app.execute(action=do_dump_raw_vcs_info),
            description='Print VCS revision information.',
            opts=self.common_opts + self.common_build_facade_opts(),
            visible=False,
        )
        self['svn-revision'] = OptsHandler(
            action=app.execute(action=do_dump_svn_revision),
            description='Print SVN revision information.',
            opts=self.common_opts + self.common_build_facade_opts(),
            visible=False,
        )
        self['recipes'] = OptsHandler(
            action=app.execute(action=do_recipes),
            description='All recipes used in tests',
            opts=self.common_opts + self.common_build_facade_opts() + [DumpTestListOptions(), DumpRecipesOptions()],
        )
        if app_config.in_house:
            import devtools.ya.handlers.dump.arcadia_specific as arcadia_specific
            from handlers.dump.debug import debug_handler

            self['groups'] = arcadia_specific.GroupsHandler()
            self['atd-revisions'] = OptsHandler(
                action=app.execute(action=arcadia_specific.do_atd_revisions),
                description='Dump revisions of trunk/arcadia_tests_data',
                opts=self.common_opts
                + [DumpAtdRevisionOptions(), CustomFetcherOptions(), SandboxAuthOptions(), ToolsOptions()],
            )
            self['debug'] = debug_handler


def _do_dump(gen_func, params, debug_options=[], write_stdout=True, build_root=None, **kwargs):
    for name in 'debug_options', 'filter_opts', 'peerdir_opts':
        if hasattr(params, name):
            debug_options.extend(getattr(params, name))
    logger.debug('abs_targets: %s', params.abs_targets)
    with temp_dir() as tmp:
        res = gen_func(
            build_root=build_root or tmp,
            build_type=params.build_type,
            build_targets=params.abs_targets,
            debug_options=debug_options,
            flags=params.flags,
            ymake_bin=getattr(params, 'ymake_bin', None),
            host_platform=params.host_platform,
            target_platforms=params.target_platforms,
            **kwargs
        )
    if write_stdout:
        sys.stdout.write(res.stdout)
    return res


def do_modules(params):
    _do_dump(gen_modules, params)


def do_module_info(params, write_stdout=True):
    if params.with_data:
        params.flags['YMAKE_ADD_DATA'] = 'yes'
    return _do_dump(
        gen_func=gen_module_info,
        params=params,
        modules_info_filter=getattr(params, 'modules_info_filter'),
        modules_info_file=getattr(params, 'modules_info_file'),
        write_stdout=write_stdout,
    )


def do_dir_graph(params):
    def merge_lists(one, two):
        return sorted(set(one + two))

    def get_test_dependencies(deps, arc_root, test, test_data=None):
        data = merge_lists(test_data or [], deps)
        return merge_lists(data, get_canondata_paths(arc_root, test))

    if params.dump_deps:
        deps = _do_dump(gen_dependencies, params, params.legacy_deps_opts, write_stdout=False)
        json.dump(json.loads(deps.stdout), sys.stdout, indent=4, sort_keys=True)
        return

    if params.dump_reachable_dirs:
        _do_dump(gen_graph, params, params.legacy_deps_opts + ['x', 'M'])
        return

    dg = _do_dump(
        gen_dir_graph, params, params.legacy_deps_opts, write_stdout=False, split_by_types=params.split_by_type
    )

    tests = get_tests(params)
    for test in tests:
        test_data = test.get_arcadia_test_data()
        if test.project_path in test_data:
            test_data.remove(test.project_path)
        if params.split_by_type:
            if test.project_path not in dg:
                dg[test.project_path] = {}
            dg[test.project_path]["INCLUDE"] = get_test_dependencies(
                dg[test.project_path].get("INCLUDE", []), params.arc_root, test
            )
            dg[test.project_path]["DATA"] = merge_lists(dg[test.project_path].get("DATA", []), test_data)
        else:
            dg[test.project_path] = get_test_dependencies(
                dg.get(test.project_path, []), params.arc_root, test, test_data
            )

    if params.trace_from is not None:
        print('\n'.join(reachable(dg, params.trace_from, params.split_by_type)))

    elif params.plain_dump:

        def get_plain_deps():
            plain_deps = set()
            for k, v in six.iteritems(dg):
                plain_deps.add(k)
                plain_deps |= set(v)
            return sorted(list(plain_deps))

        json.dump(get_plain_deps(), sys.stdout, indent=4, sort_keys=True)
    else:
        json.dump(dg, sys.stdout, indent=4, sort_keys=True)


def get_canondata_paths(arc_root, test):
    paths = []
    canondata_path = os.path.join(arc_root, test.project_path, "canondata")
    if os.path.exists(canondata_path):
        for root, dirs, files in os.walk(canondata_path):
            paths.append(os.path.relpath(root, arc_root))
    return paths


DUMP_OPTS_GROUP = Group("Dump options", 0)


class SplitByTypeOptions(Options):
    def __init__(self):
        self.split_by_type = False

    @staticmethod
    def consumer():
        return ArgConsumer(
            ['--split'],
            help='split by type',
            hook=SetConstValueHook('split_by_type', True),
            group=DUMP_OPTS_GROUP,
        )


class DumpDirOptions(Options):
    def __init__(self):
        self.trace_from = None
        self.plain_dump = False
        self.dump_deps = False
        self.dump_reachable_dirs = False

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--trace'],
                help='trace from sources',
                hook=SetValueHook('trace_from'),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--plain'],
                help='plain dump mode',
                hook=SetConstValueHook('plain_dump', True),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--dump-deps'],
                help='Dump dependencies (legacy)',
                hook=SetConstValueHook('dump_deps', True),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--reachable-dirs'],
                help='Dump reachable dirs',
                hook=SetConstValueHook('dump_reachable_dirs', True),
                group=DUMP_OPTS_GROUP,
            ),
        ]


class PeerDirectoriesOptions(Options):
    def __init__(self):
        self.peerdir_opts = []

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--old-peerdirs'],
                help='Include indirect (module->dir->module) PEERDIRs',
                hook=SetConstAppendHook('peerdir_opts', 'I'),
                group=DUMP_OPTS_GROUP,
                visible=False,
            )
        ]


class LegacyDepsOptions(Options):
    def __init__(self):
        self.legacy_deps_opts = ['I']

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--no-legacy-deps'],
                help='Exclude legacy dependencies (inderect PEERDIR relations)',
                hook=SetConstValueHook('legacy_deps_opts', []),
                group=DUMP_OPTS_GROUP,
            )
        ]


class DumpSrcDepsOptions(Options):
    def __init__(self):
        self.with_yamake = False

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--with-yamakes'],
                help='Include ya.make files, that are only used as a build configuration',
                hook=SetConstValueHook('with_yamake', True),
                group=DUMP_OPTS_GROUP,
            ),
        ]


class DepFilterOptions(Options):
    def __init__(self):
        self.filter_opts = []

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--no-tools'],
                help='Exclude tools dependencies',
                hook=SetConstAppendHook('filter_opts', 'V'),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--no-addincls'],
                help='Exclude ADDINCLs dependencies',
                hook=SetConstAppendHook('filter_opts', 'T'),
                group=DUMP_OPTS_GROUP,
            ),
        ]


class DepGraphOutputType(enum.Enum):
    CUSTOM = 0
    FLAT_JSON = 1
    FLAT_JSON_FILES = 2


class DepGraphOutputOptions(Options):
    def __init__(self):
        self.output_type = DepGraphOutputType.CUSTOM

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--flat-json'],
                help='Dump dep-graph in flat json format',
                hook=SetConstValueHook('output_type', DepGraphOutputType.FLAT_JSON),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--flat-json-files'],
                help='Dump dep-graph in flat json format without commands',
                hook=SetConstValueHook('output_type', DepGraphOutputType.FLAT_JSON_FILES),
                group=DUMP_OPTS_GROUP,
            ),
        ]


class DepTraverseOptions(Options):
    @staticmethod
    def consumer():
        def test_depends_updater(flags):
            flags['TRAVERSE_DEPENDS'] = 'yes'
            flags['TRAVERSE_RECURSE_FOR_TESTS'] = 'yes'
            return flags

        def ignore_recurses_updater(flags):
            flags['TRAVERSE_RECURSE'] = 'no'
            return flags

        # Hooks update flags property from FlagsOptions
        return [
            ArgConsumer(
                ['-t', '--force-build-depends'],
                help='Include DEPENDS and RECURSE_FOR_TESTS dependencies',
                hook=UpdateValueHook('flags', test_depends_updater),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['-A'],
                help='Same as -t',
                hook=UpdateValueHook('flags', test_depends_updater),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--ignore-recurses'],
                help='Exclude all RECURSE dependencies',
                hook=UpdateValueHook('flags', ignore_recurses_updater),
                group=DUMP_OPTS_GROUP,
            ),
        ]


class DataOptions(Options):
    def __init__(self):
        self.with_data = False

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--with-data'],
                help='Include DATA files and dirs',
                hook=SetConstValueHook('with_data', True),
                group=DUMP_OPTS_GROUP,
            ),
        ]


class DumpTestListOptions(Options):
    def __init__(self):
        self.follow_dependencies = False

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--skip-deps'],
                help='Now default and ignored',
                hook=SetConstValueHook('follow_dependencies', False),
                group=DUMP_OPTS_GROUP,
                visible=False,
                deprecated=True,
            ),
        ]


class DumpLicenseOptions(Options):
    def __init__(self):
        self.json_licenses = False
        self.link_type = 'static'
        self.custom_tags = []

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--json'],
                help='Machine oriented json representation',
                hook=SetConstValueHook('json_licenses', True),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--link-type'],
                help='Specify library link type (static|dynamic)',
                hook=SetValueHook('link_type'),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--custom-license-tag'],
                help='Specify library link type (static|dynamic)',
                hook=SetAppendHook('custom_tags'),
                group=DUMP_OPTS_GROUP,
            ),
        ]


class DumpForcedDepsOptions(Options):
    def __init__(self):
        self.json_forced_deps = False

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--json'],
                help='Json forced dependency representation',
                hook=SetConstValueHook('json_forced_deps', True),
                group=DUMP_OPTS_GROUP,
            ),
        ]


def FullForcedDepsOptions():
    return (
        DumpYaHandler.common_opts
        + DumpYaHandler.common_build_facade_opts()
        + [
            YMakeDebugOptions(),
            CustomBuildRootOptions(),
            DumpForcedDepsOptions(),
        ]
    )


class RelationSrcDstOptions(Options):
    def __init__(self, free_dst=False):
        self._free_dst = free_dst
        self.recursive = False
        self.relation_src = []
        self.relation_dst = []

    def consumer(self):
        res = [
            ArgConsumer(
                ['--from'],
                help='Dump relations from this target (path relative to the arcadia root)',
                hook=SetAppendHook('relation_src'),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--to'],
                help='Dump relations to this target (path relative to the arcadia root)',
                hook=SetAppendHook('relation_dst'),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--recursive'],
                help='Show relations between RELATION_SRC and all modules from RELATION_DST directories',
                hook=SetConstValueHook('recursive', True),
                group=DUMP_OPTS_GROUP,
            ),
        ]
        if self._free_dst:
            res.append(
                FreeArgConsumer(
                    help='relation destination target',
                    hook=ExtendHook('relation_dst'),
                )
            )
        return res

    def postprocess(self):
        if len(self.relation_dst) == 0:
            raise ArgsValidatingException('Error: no target is set')


class FilesFilterOptions(Options):
    def __init__(self):
        self.skip_make_files = False
        self.mark_make_files = False

    def consumer(self):
        res = [
            ArgConsumer(
                ['--skip-make-files'],
                help='Skip all make files',
                hook=SetConstValueHook('skip_make_files', True),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--mark-make-files'],
                help='Mark all make files as Makefile',
                hook=SetConstValueHook('mark_make_files', True),
                group=DUMP_OPTS_GROUP,
            ),
        ]
        return res


class DumpAllRelationsOptions(Options):
    def __init__(self):
        self.json_format = False
        self.show_targets_deps = False

    def consumer(self):
        res = [
            ArgConsumer(
                ['--json'],
                help='Dump relations in json format',
                hook=SetConstValueHook('json_format', True),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--show-targets-deps'],
                help='Show dependencies between RELATION_DST targets',
                hook=SetConstValueHook('show_targets_deps', True),
                group=DUMP_OPTS_GROUP,
            ),
        ]
        return res


class DumpDescriptionOptions(Options):
    def __init__(self):
        self.dump_all_conf_docs = False
        self.conf_docs_json = False

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--dump-all'],
                help='Dump information for all entities including internal',
                hook=SetConstValueHook('dump_all_conf_docs', True),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--json'],
                help='Dump information for all entities including internal in json format, uses are included',
                hook=SetConstValueHook('conf_docs_json', True),
                group=DUMP_OPTS_GROUP,
            ),
        ]


class DumpAtdRevisionOptions(Options):
    def __init__(self):
        self.human_readable = False
        self.print_size = False
        self.arcadia_revision = None
        self.path = None

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--human-readable'],
                help='Add human-readable paths to comments',
                hook=SetConstValueHook('human_readable', True),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--print-size'],
                help='Add human-readable paths to comments',
                hook=SetConstValueHook('print_size', True),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--revision'],
                help='Gather revisions for this arcadia revision',
                hook=SetValueHook('arcadia_revision'),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--path'],
                help='Gather revisions for this path recursively',
                hook=SetValueHook('path'),
                group=DUMP_OPTS_GROUP,
            ),
        ]


class DumpModuleInfoOptions(Options):
    def __init__(self):
        self.modules_info_file = ''
        self.modules_info_filter = ''

    @staticmethod
    def consumer():
        return [
            ArgConsumer(
                ['--modules-info-file'],
                help='Save modules informaton into file specified',
                hook=SetValueHook('modules_info_file'),
                group=DUMP_OPTS_GROUP,
            ),
            ArgConsumer(
                ['--modules-info-filter'],
                help='Dump information only for modules matching regexp',
                hook=SetValueHook('modules_info_filter'),
                group=DUMP_OPTS_GROUP,
            ),
        ]


class DumpRecipesOptions(Options):
    def __init__(self):
        self.json_format = False

    def consumer(self):
        res = [
            ArgConsumer(
                ['--json'],
                help='Dump recipes in json format',
                hook=SetConstValueHook('json_format', True),
                group=DUMP_OPTS_GROUP,
            ),
        ]
        return res


def do_dump_srcdeps(params):
    debug_options = []
    params.flags['YMAKE_ADD_DATA'] = 'yes'
    if params.with_yamake:
        debug_options.append('mkf')
    _do_dump(gen_srcdeps, params, debug_options)


def do_dot_graph(params):
    _do_dump(gen_modules, params, ['D'])


def do_dep_graph(params):
    options = params.legacy_deps_opts
    if params.output_type == DepGraphOutputType.FLAT_JSON:
        options.append('flat-json-with-cmds')
    elif params.output_type == DepGraphOutputType.FLAT_JSON_FILES:
        options.append('flat-json')
    _do_dump(gen_graph, params, options)


def do_json_dep_graph(params):
    _do_dump(gen_json_graph, params, params.legacy_deps_opts)


def do_licenses(params):
    _do_dump(
        gen_licenses_list,
        params,
        write_stdout=True,
        debug_options=['lic-json' if params.json_licenses else 'lic'],
        lic_link_type=params.link_type,
        lic_custom_tags=params.custom_tags,
    )


def do_forced_deps(params, write_stdout=True):
    res = _do_dump(
        gen_forced_deps_list,
        params,
        write_stdout=write_stdout,
        debug_options=['fdm-json' if params.json_forced_deps else 'fdm'],
    )
    return '' if write_stdout else res.stdout


# TODO: support host/target platforms opts
def do_build_plan(params):
    with temp_dir() as tmp:
        graph = gen_plan(
            arc_root=params.arc_root,
            build_root=tmp,
            build_type=params.build_type,
            build_targets=params.abs_targets,
            debug_options=params.debug_options,
            flags=params.flags,
            ymake_bin=params.ymake_bin,
            no_ymake_resource=params.no_ymake_resource,
            vcs_file=params.vcs_file,
        )
        json.dump(graph, sys.stdout, indent=4, sort_keys=True)


def do_relation(params):
    options = []
    if params.recursive:
        options.append('recursive')
    for dst in params.relation_dst:
        _do_dump(
            gen_relation,
            params,
            options,
            find_path_from=params.relation_src,
            find_path_to=[dst],
        )


def do_all_relations(params):
    options = ['J' if params.json_format else 'D']
    if params.recursive:
        options.append('recursive')
    if params.show_targets_deps:
        options.append('show-targets-deps')
    _do_dump(
        gen_relation,
        params,
        options,
        find_path_from=params.relation_src,
        find_path_to=params.relation_dst,
    )


def do_files(params):
    options = []
    if params.with_data:
        options = ['dump-data']
        params.flags['YMAKE_ADD_DATA'] = 'yes'
    if params.skip_make_files:
        options.append('skip-make-files')
    if params.mark_make_files:
        options.append('mark-make-files')
    _do_dump(gen_filelist, params, options)


def do_loops(params):
    _do_dump(gen_all_loops, params)


def do_peerdir_loops(params):
    _do_dump(gen_dir_loops, params)


def do_imprint(params):
    lines = imprint.generate_detailed_imprints(os.curdir)
    writer = csv.writer(sys.stdout, delimiter='\t')
    for line in lines:
        writer.writerow(line)


# TODO: support host/target platforms opts
def do_uids(params):
    uids = gen_uids(
        arc_root=params.arc_root,
        build_root=None,
        build_type=params.build_type,
        build_targets=params.abs_targets,
        debug_options=[],
        flags={},
        ymake_bin=None,
    )
    json.dump(uids, sys.stdout, indent=4, sort_keys=True)


def get_tests(params):
    params.flags['TRAVERSE_RECURSE_FOR_TESTS'] = 'yes'
    kwargs = {
        'build_root': params.bld_root,
        'build_type': params.build_type,
        'abs_targets': params.abs_targets,
        'debug_options': [],
        'flags': params.flags,
        'ymake_bin': params.ymake_bin,
        'arcadia_tests_data_path': 'arcadia_tests_data',
    }
    (
        _,
        test_dart,
    ) = gen_test_dart(**kwargs)

    return sorted(
        generate_tests_by_dart(
            test_dart,
            opts=params,
        ),
        key=lambda test: os.path.join(test.project_path, test.name),
    )


def do_json_test_list(params):
    json.dump([t.save() for t in get_tests(params)], sys.stdout, indent=4, sort_keys=True)


def do_test_list(params):
    print(pickle.dumps(get_tests(params)))


def do_conf(params):
    with temp_dir() as tmp:
        generation_conf = _do_dump(gen_conf, params, write_stdout=False, build_root=tmp)
        print(open(generation_conf, 'r').read())


def do_conf_docs(params):
    _do_dump(dump_mmm_docs, params, dump_all_conf_docs=params.dump_all_conf_docs, conf_docs_json=params.conf_docs_json)


def do_dump_raw_vcs_info(params):
    sys.stdout.write(json.dumps(vcsversion.get_raw_version_info(params.arc_root, params.bld_root)))


def do_dump_vcs_info(params):
    fake_data = strtobool(params.flags.get('NO_VCS_DEPENDS', 'no'))
    fake_build_info = strtobool(params.flags.get('CONSISTENT_BUILD', 'no'))
    sys.stdout.write(vcsversion.get_version_info(params.arc_root, params.bld_root, fake_data, fake_build_info) + '\n')


def do_dump_svn_revision(params):
    sys.stdout.write(str(vcsversion.repo_config(params.arc_root)))


def do_recipes(params):
    tests = get_tests(params)
    test_recipes = []
    for test in tests:
        encoded_recipes = test.recipes
        if not encoded_recipes:
            continue
        decoded_recipes = decode_recipe_cmdline(encoded_recipes)
        test_recipes.append(decoded_recipes)

    recipes = set()
    for test_recipe in test_recipes:
        for recipe in test_recipe:
            recipes.add(tuple(recipe))

    if params.json_format:
        output = json.dumps(recipes, indent=4, sort_keys=True)
    else:
        recipes_str = [' '.join(recipe) for recipe in recipes]
        output = ' '.join(recipes_str)
    print(output)
