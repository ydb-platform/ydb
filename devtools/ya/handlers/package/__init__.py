from __future__ import absolute_import
import logging

import app
import core.yarg
import core.common_opts
import build.ya_make
import build.targets_deref
import build.build_opts
import devtools.ya.test.opts as test_opts

import package.docker
import package.packager
import devtools.ya.handlers.package.opts as package_opts
from core.yarg.help_level import HelpLevel

logger = logging.getLogger(__name__)


class PackageYaHandler(core.yarg.OptsHandler):
    description = """Build package using json package description in the release build type by default.
For more info see https://docs.yandex-team.ru/ya-make/usage/ya_package"""

    def __init__(self):
        super(PackageYaHandler, self).__init__(
            action=app.execute(package.packager.do_package, respawn=app.RespawnType.OPTIONAL),
            description=self.description,
            examples=[
                core.yarg.UsageExample(
                    cmd='{prefix} <path to json description>',
                    description='Create tarball package from json description',
                )
            ],
            opts=[
                package_opts.PackageOperationalOptions(),
                package_opts.PackageCustomizableOptions(),
                package_opts.InterimOptions(),
                core.common_opts.LogFileOptions(),
                core.common_opts.EventLogFileOptions(),
                build.build_opts.BuildTypeOptions('release'),
                build.build_opts.BuildThreadsOptions(build_threads=None),
                core.common_opts.CrossCompilationOptions(),
                build.build_opts.ArcPrefetchOptions(),
                build.build_opts.ContentUidsOptions(),
                build.build_opts.KeepTempsOptions(),
                build.build_opts.RebuildOptions(),
                build.build_opts.StrictInputsOptions(),
                build.build_opts.DumpReportOptions(),
                build.build_opts.OutputOptions(),
                build.build_opts.AuthOptions(),
                build.build_opts.YMakeDumpGraphOptions(),
                build.build_opts.YMakeDebugOptions(),
                build.build_opts.YMakeBinOptions(),
                build.build_opts.YMakeRetryOptions(),
                build.build_opts.ExecutorOptions(),
                build.build_opts.ForceDependsOptions(),
                build.build_opts.IgnoreRecursesOptions(),
                core.common_opts.CustomSourceRootOptions(),
                core.common_opts.CustomBuildRootOptions(),
                core.common_opts.ShowHelpOptions(),
                core.common_opts.BeVerboseOptions(),
                core.common_opts.HtmlDisplayOptions(),
                core.common_opts.CommonUploadOptions(),
                build.build_opts.SandboxUploadOptions(ssh_key_option_name="--ssh-key", visible=HelpLevel.BASIC),
                build.build_opts.MDSUploadOptions(visible=HelpLevel.BASIC),
                core.common_opts.TransportOptions(),
                build.build_opts.CustomFetcherOptions(),
                build.build_opts.DistCacheOptions(),
                build.build_opts.FlagsOptions(),
                build.build_opts.PGOOptions(),
                test_opts.RunTestOptions(),
                test_opts.DebuggingOptions(),
                # strip_idle_build_results must be False to avoid removal of build nodes which are
                # reachable due RECURSE and used in package, but not required for tests
                test_opts.DepsOptions(strip_idle_build_results=False),
                test_opts.FileReportsOptions(),
                test_opts.FilteringOptions(test_size_filters=None),
                test_opts.PytestOptions(),
                test_opts.JUnitOptions(),
                test_opts.RuntimeEnvironOptions(),
                test_opts.TestToolOptions(),
                test_opts.UidCalculationOptions(cache_tests=False),
                core.common_opts.YaBin3Options(),
            ]
            + build.build_opts.distbs_options()
            + build.build_opts.checkout_options()
            + build.build_opts.svn_checkout_options(),
        )
