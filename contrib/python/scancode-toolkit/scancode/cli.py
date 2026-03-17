#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

# Import first because this import has monkey-patching side effects
from scancode.pool import get_pool

# Import early because of the side effects
import scancode_config

from collections import defaultdict
from functools import partial
import os
import logging
import sys
from time import sleep
from time import time
import traceback

# this exception is not available on posix
try:
    WindowsError  # NOQA
except NameError:

    class WindowsError(Exception):
        pass

import click  # NOQA

from commoncode import cliutils
from commoncode.cliutils import GroupedHelpCommand
from commoncode.cliutils import path_progress_message
from commoncode.cliutils import progressmanager
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.fileutils import as_posixpath
from commoncode.timeutils import time2tstamp
from commoncode.resource import Codebase
from commoncode.resource import VirtualCodebase
from commoncode.system import on_windows

# these are important to register plugin managers
from plugincode import PluginManager
from plugincode import pre_scan
from plugincode import scan
from plugincode import post_scan
from plugincode import output_filter
from plugincode import output

from scancode import ScancodeError
from scancode import ScancodeCliUsageError
from scancode import notice
from scancode import print_about
from scancode import Scanner
from scancode.help import epilog_text
from scancode.help import examples_text
from scancode.interrupt import DEFAULT_TIMEOUT
from scancode.interrupt import fake_interruptible
from scancode.interrupt import interruptible

# Tracing flags
TRACE = False
TRACE_DEEP = False


def logger_debug(*args):
    pass


if TRACE or TRACE_DEEP:
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str)
                                     and a or repr(a) for a in args))

echo_stderr = partial(click.secho, err=True)


def print_examples(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(examples_text)
    ctx.exit()


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo('ScanCode version ' + scancode_config.__version__)
    ctx.exit()


class ScancodeCommand(GroupedHelpCommand):
    short_usage_help = '''
Try the 'scancode --help' option for help on options and arguments.'''


try:
    # IMPORTANT: this discovers, loads and validates all available plugins
    plugin_classes, plugin_options = PluginManager.load_plugins()
    if TRACE:
        logger_debug('plugin_options:')
        for clio in plugin_options:
            logger_debug('   ', clio)
            logger_debug('name:', clio.name, 'default:', clio.default)
except ImportError as e:
    echo_stderr('========================================================================')
    echo_stderr('ERROR: Unable to import ScanCode plugins.'.upper())
    echo_stderr('Check your installation configuration (setup.py) or re-install/re-configure ScanCode.')
    echo_stderr('The following plugin(s) are referenced and cannot be loaded/imported:')
    echo_stderr(str(e), color='red')
    echo_stderr('========================================================================')
    raise e


def print_plugins(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    for plugin_cls in sorted(plugin_classes, key=lambda pc: (pc.stage, pc.name)):
        click.echo('--------------------------------------------')
        click.echo('Plugin: scancode_{self.stage}:{self.name}'.format(self=plugin_cls), nl=False)
        click.echo('  class: {self.__module__}:{self.__name__}'.format(self=plugin_cls))
        codebase_attributes = ', '.join(plugin_cls.codebase_attributes)
        click.echo('  codebase_attributes: {}'.format(codebase_attributes))
        resource_attributes = ', '.join(plugin_cls.resource_attributes)
        click.echo('  resource_attributes: {}'.format(resource_attributes))
        click.echo('  sort_order: {self.sort_order}'.format(self=plugin_cls))
        required_plugins = ', '.join(plugin_cls.required_plugins)
        click.echo('  required_plugins: {}'.format(required_plugins))
        click.echo('  options:')
        for option in plugin_cls.options:
            name = option.name
            opts = ', '.join(option.opts)
            help_group = option.help_group
            help_txt = option.help  # noqa
            click.echo('    help_group: {help_group!s}, name: {name!s}: {opts}\n      help: {help_txt!s}'.format(**locals()))
        click.echo('  doc: {self.__doc__}'.format(self=plugin_cls))
        click.echo('')
    ctx.exit()


def print_options(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    values = ctx.params
    click.echo('Options:')

    for name, val in sorted(values.items()):
        click.echo('  {name}: {val}'.format(**locals()))
    click.echo('')
    ctx.exit()


def validate_depth(ctx, param, value):
    if value < 0:
        raise click.BadParameter("max-depth needs to be a positive integer or 0")
    return value


@click.command(name='scancode',
    epilog=epilog_text,
    cls=ScancodeCommand,
    plugin_options=plugin_options)

@click.pass_context

@click.argument('input',
    metavar='<OUTPUT FORMAT OPTION(s)> <input>...', nargs=-1,
    type=click.Path(exists=True, readable=True, path_type=str))

@click.option('--strip-root',
    is_flag=True,
    conflicting_options=['full_root'],
    help='Strip the root directory segment of all paths. The default is to '
         'always include the last directory segment of the scanned path such '
         'that all paths have a common root directory.',
    help_group=cliutils.OUTPUT_CONTROL_GROUP, cls=PluggableCommandLineOption)

@click.option('--full-root',
    is_flag=True,
    conflicting_options=['strip_root'],
    help='Report full, absolute paths.',
    help_group=cliutils.OUTPUT_CONTROL_GROUP, cls=PluggableCommandLineOption)

@click.option('-n', '--processes',
    type=int,
    default=1,
    metavar='INT',
    help='Set the number of parallel processes to use. '
         'Disable parallel processing if 0. Also disable threading if -1. ''[default: 1]',
    help_group=cliutils.CORE_GROUP, sort_order=10, cls=PluggableCommandLineOption)

@click.option('--timeout',
    type=float,
    default=DEFAULT_TIMEOUT,
    metavar='<secs>',
    help='Stop an unfinished file scan after a timeout in seconds.  '
         '[default: %d seconds]' % DEFAULT_TIMEOUT,
    help_group=cliutils.CORE_GROUP, sort_order=10, cls=PluggableCommandLineOption)

@click.option('--quiet',
    is_flag=True,
    conflicting_options=['verbose'],
    help='Do not print summary or progress.',
    help_group=cliutils.CORE_GROUP, sort_order=20, cls=PluggableCommandLineOption)

@click.option('--verbose',
    is_flag=True,
    conflicting_options=['quiet'],
    help='Print progress as file-by-file path instead of a progress bar. '
         'Print verbose scan counters.',
    help_group=cliutils.CORE_GROUP, sort_order=20, cls=PluggableCommandLineOption)

@click.option('--from-json',
    is_flag=True,
    help='Load codebase from one or more <input> JSON scan file(s).',
    help_group=cliutils.CORE_GROUP, sort_order=25, cls=PluggableCommandLineOption)

@click.option('--timing',
    is_flag=True,
    hidden=True,
    help='Collect scan timing for each scan/scanned file.',
    help_group=cliutils.CORE_GROUP, sort_order=250, cls=PluggableCommandLineOption)

@click.option('--max-in-memory',
    type=int, default=10000,
    show_default=True,
    help='Maximum number of files and directories scan details kept in memory '
         'during a scan. Additional files and directories scan details above this '
         'number are cached on-disk rather than in memory. '
         'Use 0 to use unlimited memory and disable on-disk caching. '
         'Use -1 to use only on-disk caching.',
    help_group=cliutils.CORE_GROUP, sort_order=300, cls=PluggableCommandLineOption)

@click.option('--max-depth',
    type=int,
    default=0,
    show_default=False,
    callback=validate_depth,
    help='Maximum nesting depth of subdirectories to scan. '
        'Descend at most INTEGER levels of directories below and including '
        'the starting directory. Use 0 for no scan depth limit.',
    help_group=cliutils.CORE_GROUP, sort_order=301, cls=PluggableCommandLineOption)

@click.help_option('-h', '--help',
    help_group=cliutils.DOC_GROUP, sort_order=10, cls=PluggableCommandLineOption)

@click.option('--about',
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=print_about,
    help='Show information about ScanCode and licensing and exit.',
    help_group=cliutils.DOC_GROUP, sort_order=20, cls=PluggableCommandLineOption)

@click.option('--version',
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=print_version,
    help='Show the version and exit.',
    help_group=cliutils.DOC_GROUP, sort_order=20, cls=PluggableCommandLineOption)

@click.option('--examples',
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=print_examples,
    help=('Show command examples and exit.'),
    help_group=cliutils.DOC_GROUP, sort_order=50, cls=PluggableCommandLineOption)

@click.option('--plugins',
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=print_plugins,
    help='Show the list of available ScanCode plugins and exit.',
    help_group=cliutils.DOC_GROUP, cls=PluggableCommandLineOption)

@click.option('--test-mode',
    is_flag=True,
    default=False,
    # not yet supported in Click 6.7 but added in PluggableCommandLineOption
    hidden=True,
    help='Run ScanCode in a special "test mode". Only for testing.',
    help_group=cliutils.MISC_GROUP, sort_order=1000, cls=PluggableCommandLineOption)

@click.option('--test-slow-mode',
    is_flag=True,
    default=False,
    # not yet supported in Click 6.7 but added in PluggableCommandLineOption
    hidden=True,
    help='Run ScanCode in a special "test slow mode" to ensure that --email '
         'scan needs at least one second to complete. Only for testing.',
    help_group=cliutils.MISC_GROUP, sort_order=1000, cls=PluggableCommandLineOption)

@click.option('--test-error-mode',
    is_flag=True,
    default=False,
    # not yet supported in Click 6.7 but added in PluggableCommandLineOption
    hidden=True,
    help='Run ScanCode in a special "test error mode" to trigger errors with '
         'the --email scan. Only for testing.',
    help_group=cliutils.MISC_GROUP, sort_order=1000, cls=PluggableCommandLineOption)

@click.option('--print-options',
    is_flag=True,
    expose_value=False,
    callback=print_options,
    help='Show the list of selected options and exit.',
    help_group=cliutils.DOC_GROUP, cls=PluggableCommandLineOption)

@click.option('--keep-temp-files',
    is_flag=True,
    default=False,
    # not yet supported in Click 6.7 but added in PluggableCommandLineOption
    hidden=True,
    help='Keep temporary files and show the directory where temporary files '
         'are stored. (By default temporary files are deleted when a scan is '
         'completed.)',
    help_group=cliutils.MISC_GROUP, sort_order=1000, cls=PluggableCommandLineOption)
def scancode(
    ctx,
    input,  # NOQA
    strip_root,
    full_root,
    processes,
    timeout,
    quiet,
    verbose,
    max_depth,
    from_json,
    timing,
    max_in_memory,
    test_mode,
    test_slow_mode,
    test_error_mode,
    keep_temp_files,
    echo_func=echo_stderr,
    *args,
    **kwargs,
):
    """scan the <input> file or directory for license, origin and packages and save results to FILE(s) using one or more output format option.

    Error and progress are printed to stderr.
    """

    # notes: the above docstring of this function is used in the CLI help Here is
    # it's actual docstring:
    """
    This function is the main ScanCode CLI entry point.

    Return a return code of 0 on success or a positive integer on error from
    running all the scanning "stages" with the `input` file or
    directory.

    The scanning stages are:

    - `inventory`: collect the codebase inventory resources tree for the
      `input`. This is a built-in stage that does not accept plugins.

    - `setup`: as part of the plugins system, each plugin is loaded and
      its `setup` method is called if it is enabled.

    - `pre-scan`: each enabled pre-scan plugin `process_codebase(codebase)`
      method is called to update/transforme the whole codebase.

    - `scan`: the codebase is walked and each enabled scan plugin
      `get_scanner()` scanner function is called once for each codebase
       resource receiving the `resource.location` as an argument.

    - `post-scan`: each enabled post-scan plugin `process_codebase(codebase)`
      method is called to update/transform the whole codebase.

    - `output_filter`: the `process_resource` method of each enabled
      output_filter plugin is called on each resource to determine if the
      resource should be kept or not in the output stage.

    - `output`: each enabled output plugin `process_codebase(codebase)`
      method is called to create an output for the codebase filtered resources.

    Beside `input`, the other arguments are:

    - `strip_root` and `full_root`: boolean flags: In the outputs, strip the
      first path segment of a file if `strip_root` is True unless the `input` is
      a single file. If `full_root` is True report the path as an absolute path.
      These options are mutually exclusive.

    - `processes`: int: run the scan using up to this number of processes in
      parallel. If 0, disable the multiprocessing machinery. if -1 also
      disable the multithreading machinery.

    - `timeout`: float: intterup the scan of a file if it does not finish within
      `timeout` seconds. This applied to each file and scan individually (e.g.
      if the license scan is interrupted they other scans may complete, each
      withing the timeout)

    - `quiet` and `verbose`: boolean flags: Do not display any message if
      `quiet` is True. Otherwise, display extra verbose messages if `quiet` is
      False and `verbose` is True. These two options are mutually exclusive.

    - `timing`: boolean flag: collect per-scan and per-file scan timings if
      True.

    Other **kwargs are passed down to plugins as CommandOption indirectly
    through Click context machinery.
    """

    # configure a null root handler ONLY when used as a command line
    # otherwise no root handler is set
    logging.getLogger().addHandler(logging.NullHandler())

    success = False
    try:
        # Validate CLI UI options dependencies and other CLI-specific inits
        if TRACE_DEEP:
            logger_debug('scancode: ctx.params:')
            for co in sorted(ctx.params.items()):
                logger_debug('  scancode: ctx.params:', co)

        cliutils.validate_option_dependencies(ctx)
        pretty_params = get_pretty_params(ctx, generic_paths=test_mode)

        # run proper
        success, _results = run_scan(
            input=input,
            from_json=from_json,
            strip_root=strip_root,
            full_root=full_root,
            processes=processes,
            timeout=timeout,
            quiet=quiet,
            verbose=verbose,
            max_depth=max_depth,
            timing=timing,
            max_in_memory=max_in_memory,
            test_mode=test_mode,
            test_slow_mode=test_slow_mode,
            test_error_mode=test_error_mode,
            keep_temp_files=keep_temp_files,
            pretty_params=pretty_params,
            # results are saved to file, no need to get them back in a cli context
            return_results=False,
            echo_func=echo_stderr,
            *args,
            **kwargs
        )

        # check for updates
        from scancode.outdated import check_scancode_version
        outdated = check_scancode_version()
        if not quiet and outdated:
            echo_stderr(outdated, fg='yellow')

    except click.UsageError as e:
        # this will exit
        raise e

    except ScancodeError as se:
        # this will exit
        raise click.BadParameter(str(se))

    rc = 0 if success else 1
    ctx.exit(rc)


def run_scan(
    input,  # NOQA
    from_json=False,
    strip_root=False,
    full_root=False,
    max_in_memory=10000,
    processes=1,
    timeout=120,
    quiet=True,
    verbose=False,
    max_depth=0,
    echo_func=None,
    timing=False,
    keep_temp_files=False,
    return_results=True,
    test_mode=False,
    test_slow_mode=False,
    test_error_mode=False,
    pretty_params=None,
    plugin_options=plugin_options,
    *args,
    **kwargs
):
    """
    Run a scan on `input` path (or a list of input paths) and return a tuple of
    (success, results) where success is a boolean and results is a list of
    "files" items using the same data structure as the "files" in the JSON scan
    results but as native Python. Raise Exceptions (e.g. ScancodeError) on
    error. See scancode() for arguments details.
    """

    plugins_option_defaults = {clio.name: clio.default for clio in plugin_options}
    requested_options = dict(plugins_option_defaults)
    requested_options.update(kwargs)

    if not echo_func:

        def echo_func(*_args, **_kwargs):
            pass

    if not input:
        msg = 'At least one input path is required.'
        raise ScancodeError(msg)

    if not isinstance(input, (list, tuple)):
        if not isinstance(input, str):
            msg = 'Unknown <input> format: "{}".'.format(repr(input))
            raise ScancodeError(msg)

    elif len(input) == 1:
        # we received a single input path, so we treat this as a single path
        input = input[0]  # NOQA

    # Note that If the `from_json` option is available and we have a list of
    # input paths, we can pass this `input` list just fine when we create a
    # VirtualCodebase; otherwise we have to process `input` to make it a single
    # root with excludes.
    elif not from_json:
        # FIXME: support the multiple root better. This is quirky at best

        # This is the case where we have a list of input path and the
        # `from_json` option is not selected: we can handle this IFF they share
        # a common root directory and none is an absolute path

        if any(os.path.isabs(p) for p in input):
            msg = (
                'Invalid inputs: all input paths must be relative when '
                'using multiple inputs.'
            )
            raise ScancodeError(msg)

        # find the common prefix directory (note that this is a pre string
        # operation hence it may return non-existing paths
        common_prefix = os.path.commonprefix(input)

        if not common_prefix:
            # we have no common prefix, but all relative. therefore the
            # parent/root is the current ddirectory
            common_prefix = str('.')

        elif not os.path.isdir(common_prefix):
            msg = (
                'Invalid inputs: all input paths must share a '
                'common single parent directory.'
            )

            raise ScancodeError(msg)

        # and we craft a list of synthetic --include path pattern options from
        # the input list of paths
        included_paths = [as_posixpath(path).rstrip('/') for path in input]
        # FIXME: this is a hack as this "include" is from an external plugin!!!
        include = list(requested_options.get('include', []) or [])
        include.extend(included_paths)
        requested_options['include'] = include

        # ... and use the common prefix as our new input
        input = common_prefix  # NOQA

    # build mappings of all options to pass down to plugins
    standard_options = dict(
        input=input,
        strip_root=strip_root,
        full_root=full_root,
        processes=processes,
        timeout=timeout,
        quiet=quiet,
        verbose=verbose,
        from_json=from_json,
        timing=timing,
        max_in_memory=max_in_memory,
        test_mode=test_mode,
        test_slow_mode=test_slow_mode,
        test_error_mode=test_error_mode,
    )

    requested_options.update(standard_options)

    success = True
    results = None
    codebase = None
    processing_start = time()

    start_timestamp = time2tstamp()

    if not quiet:
        if not processes:
            echo_func('Disabling multi-processing for debugging.', fg='yellow')

        elif processes == -1:
            echo_func('Disabling multi-processing '
                      'and multi-threading for debugging.', fg='yellow')

    try:
        ########################################################################
        # Find and create known plugin instances and collect the enabled
        ########################################################################

        enabled_plugins_by_stage = {}
        all_enabled_plugins_by_qname = {}
        non_enabled_plugins_by_qname = {}

        for stage, manager in PluginManager.managers.items():
            enabled_plugins_by_stage[stage] = stage_plugins = []
            for plugin_cls in manager.plugin_classes:
                try:
                    name = plugin_cls.name
                    qname = plugin_cls.qname()
                    plugin = plugin_cls(**requested_options)
                    is_enabled = False
                    try:
                        is_enabled = plugin.is_enabled(**requested_options)
                    except TypeError as te:
                        if not 'takes exactly' in str(te):
                            raise te
                    if is_enabled:
                        stage_plugins.append(plugin)
                        all_enabled_plugins_by_qname[qname] = plugin
                    else:
                        non_enabled_plugins_by_qname[qname] = plugin
                except:
                    msg = 'Failed to load plugin: %(qname)s:' % locals()
                    raise ScancodeError(msg + '\n' + traceback.format_exc())

        # NOTE: these are list of plugin instances, not classes!
        pre_scan_plugins = enabled_plugins_by_stage[pre_scan.stage]
        scanner_plugins = enabled_plugins_by_stage[scan.stage]
        post_scan_plugins = enabled_plugins_by_stage[post_scan.stage]
        output_filter_plugins = enabled_plugins_by_stage[output_filter.stage]
        output_plugins = enabled_plugins_by_stage[output.stage]

        if from_json and scanner_plugins:
            msg = ('ERROR: Data loaded from JSON: no file scan options can be '
                   'selected when using only scan data.')
            raise ScancodeCliUsageError(msg)

        if not output_plugins and not return_results:
            msg = ('ERROR: Missing output option(s): at least one output '
                   'option is required to save scan results.')
            raise ScancodeCliUsageError(msg)

        ########################################################################
        # Get required and enabled plugins instance so we can run their setup
        ########################################################################
        plugins_to_setup = []
        requestors_by_missing_qname = defaultdict(set)

        if TRACE_DEEP:
            logger_debug('scancode: all_enabled_plugins_by_qname:', all_enabled_plugins_by_qname)
            logger_debug('scancode: non_enabled_plugins_by_qname:', non_enabled_plugins_by_qname)

        for qname, enabled_plugin in all_enabled_plugins_by_qname.items():
            for required_qname in enabled_plugin.required_plugins or []:
                if required_qname in all_enabled_plugins_by_qname:
                    # there is nothing to do since we have it already as enabled
                    pass
                elif required_qname in non_enabled_plugins_by_qname:
                    plugins_to_setup.append(non_enabled_plugins_by_qname[required_qname])
                else:
                    # we have a required but not loaded plugin
                    requestors_by_missing_qname[required_qname].add(qname)

        if requestors_by_missing_qname:
            msg = 'Some required plugins are missing from available plugins:\n'
            for qn, requestors in requestors_by_missing_qname.items():
                rqs = ', '.join(sorted(requestors))
                msg += '  Plugin: {qn} is required by plugins: {rqs}.\n'.format(**locals())
            raise ScancodeError(msg)

        if TRACE_DEEP:
            logger_debug('scancode: plugins_to_setup: from required:', plugins_to_setup)

        plugins_to_setup.extend(all_enabled_plugins_by_qname.values())

        if TRACE_DEEP:
            logger_debug('scancode: plugins_to_setup: includng enabled:', plugins_to_setup)

        ########################################################################
        # Setup enabled and required plugins
        ########################################################################

        setup_timings = {}
        plugins_setup_start = time()

        if not quiet and not verbose:
            echo_func('Setup plugins...', fg='green')

        # TODO: add progress indicator
        for plugin in plugins_to_setup:
            plugin_setup_start = time()
            stage = plugin.stage
            name = plugin.name
            if verbose:
                echo_func(' Setup plugin: %(stage)s:%(name)s...' % locals(),
                            fg='green')
            try:
                plugin.setup(**requested_options)
            except:
                msg = 'ERROR: failed to setup plugin: %(stage)s:%(name)s:' % locals()
                raise ScancodeError(msg + '\n' + traceback.format_exc())

            timing_key = 'setup_%(stage)s:%(name)s' % locals()
            setup_timings[timing_key] = time() - plugin_setup_start

        setup_timings['setup'] = time() - plugins_setup_start

        ########################################################################
        # Collect Resource attributes requested for this scan
        ########################################################################
        # Craft a new Resource class with the attributes contributed by plugins
        sortable_resource_attributes = []

        # mapping of {"plugin stage:name": [list of attribute keys]}
        # also available as a kwarg entry for plugin
        requested_options['resource_attributes_by_plugin'] = resource_attributes_by_plugin = {}
        for stage, stage_plugins in enabled_plugins_by_stage.items():
            for plugin in stage_plugins:
                name = plugin.name
                try:
                    sortable_resource_attributes.append(
                        (plugin.sort_order, name, plugin.resource_attributes,)
                    )
                    resource_attributes_by_plugin[plugin.qname()] = plugin.resource_attributes.keys()
                except:
                    msg = ('ERROR: failed to collect resource_attributes for plugin: '
                           '%(stage)s:%(name)s:' % locals())
                    raise ScancodeError(msg + '\n' + traceback.format_exc())

        resource_attributes = {}
        for _, name, attribs in sorted(sortable_resource_attributes):
            resource_attributes.update(attribs)

        # FIXME: workaround for https://github.com/python-attrs/attrs/issues/339
        # we reset the _CountingAttribute internal ".counter" to a proper value
        # that matches our ordering
        for order, attrib in enumerate(resource_attributes.values(), 100):
            attrib.counter = order

        if TRACE_DEEP:
            logger_debug('scancode:resource_attributes')
            for a in resource_attributes.items():
                logger_debug(a)

        ########################################################################
        # Collect Codebase attributes requested for this run
        ########################################################################
        sortable_codebase_attributes = []

        # mapping of {"plugin stage:name": [list of attribute keys]}
        # also available as a kwarg entry for plugin
        requested_options['codebase_attributes_by_plugin'] = codebase_attributes_by_plugin = {}
        for stage, stage_plugins in enabled_plugins_by_stage.items():
            for plugin in stage_plugins:
                name = plugin.name
                try:
                    sortable_codebase_attributes.append(
                        (plugin.sort_order, name, plugin.codebase_attributes,)
                    )
                    codebase_attributes_by_plugin[plugin.qname()] = plugin.codebase_attributes.keys()
                except:
                    msg = ('ERROR: failed to collect codebase_attributes for plugin: '
                           '%(stage)s:%(name)s:' % locals())
                    raise ScancodeError(msg + '\n' + traceback.format_exc())

        codebase_attributes = {}
        for _, name, attribs in sorted(sortable_codebase_attributes):
            codebase_attributes.update(attribs)

        # FIXME: workaround for https://github.com/python-attrs/attrs/issues/339
        # we reset the _CountingAttribute internal ".counter" to a proper value
        # that matches our ordering
        for order, attrib in enumerate(codebase_attributes.values(), 100):
            attrib.counter = order

        if TRACE_DEEP:
            logger_debug('scancode:codebase_attributes')
            for a in codebase_attributes.items():
                logger_debug(a)

        ########################################################################
        # Collect codebase inventory
        ########################################################################

        inventory_start = time()

        if not quiet:
            echo_func('Collect file inventory...', fg='green')

        if from_json:
            codebase_class = VirtualCodebase
            codebase_load_error_msg = 'ERROR: failed to load codebase from scan file at: %(input)r'
        else:
            codebase_class = Codebase
            codebase_load_error_msg = 'ERROR: failed to collect codebase at: %(input)r'

        # TODO: add progress indicator
        # Note: inventory timing collection is built in Codebase initialization
        # TODO: this should also collect the basic size/dates
        try:
            codebase = codebase_class(
                location=input,
                resource_attributes=resource_attributes,
                codebase_attributes=codebase_attributes,
                full_root=full_root,
                strip_root=strip_root,
                max_in_memory=max_in_memory,
                max_depth=max_depth,
            )
        except:
            msg = 'ERROR: failed to collect codebase at: %(input)r' % locals()
            raise ScancodeError(msg + '\n' + traceback.format_exc())

        # update headers
        cle = codebase.get_or_create_current_header()
        cle.start_timestamp = start_timestamp
        cle.tool_name = 'scancode-toolkit'
        cle.tool_version = scancode_config.__version__
        cle.notice = notice
        cle.options = pretty_params or {}

        # TODO: this is weird: may be the timings should NOT be stored on the
        # codebase, since they exist in abstract of it??
        codebase.timings.update(setup_timings)
        codebase.timings['inventory'] = time() - inventory_start
        files_count, dirs_count, size_count = codebase.compute_counts()
        codebase.counters['initial:files_count'] = files_count
        codebase.counters['initial:dirs_count'] = dirs_count
        codebase.counters['initial:size_count'] = size_count

        ########################################################################
        # Run prescans
        ########################################################################

        # TODO: add progress indicator
        pre_scan_success = run_codebase_plugins(
            stage='pre-scan',
            plugins=pre_scan_plugins,
            codebase=codebase,
            stage_msg='Run %(stage)ss...',
            plugin_msg=' Run %(stage)s: %(name)s...',
            quiet=quiet,
            verbose=verbose,
            kwargs=requested_options,
            echo_func=echo_func,
        )
        success = success and pre_scan_success

        ########################################################################
        # 6. run scans.
        ########################################################################

        scan_success = run_scanners(
            stage='scan',
            plugins=scanner_plugins,
            codebase=codebase,
            processes=processes,
            timeout=timeout,
            timing=timeout,
            quiet=quiet,
            verbose=verbose,
            kwargs=requested_options,
            echo_func=echo_func,
        )
        success = success and scan_success

        ########################################################################
        # 7. run postscans
        ########################################################################

        # TODO: add progress indicator
        post_scan_success = run_codebase_plugins(
            stage='post-scan',
            plugins=post_scan_plugins,
            codebase=codebase,
            stage_msg='Run %(stage)ss...',
            plugin_msg=' Run %(stage)s: %(name)s...',
            quiet=quiet,
            verbose=verbose,
            kwargs=requested_options,
            echo_func=echo_func,
        )
        success = success and post_scan_success

        ########################################################################
        # 8. apply output filters
        ########################################################################

        # TODO: add progress indicator
        output_filter_success = run_codebase_plugins(
            stage='output-filter',
            plugins=output_filter_plugins,
            codebase=codebase,
            stage_msg='Apply %(stage)ss...',
            plugin_msg=' Apply %(stage)s: %(name)s...',
            quiet=quiet,
            verbose=verbose,
            kwargs=requested_options,
            echo_func=echo_func,
        )
        success = success and output_filter_success

        ########################################################################
        # 9. save outputs
        ########################################################################

        counts = codebase.compute_counts(skip_root=strip_root, skip_filtered=True)
        files_count, dirs_count, size_count = counts

        codebase.counters['final:files_count'] = files_count
        codebase.counters['final:dirs_count'] = dirs_count
        codebase.counters['final:size_count'] = size_count

        cle.end_timestamp = time2tstamp()
        cle.duration = time() - processing_start
        # collect these once as they are use in the headers and in the displayed summary
        errors = collect_errors(codebase, verbose)
        cle.errors = errors

        # when called from Python we can only get results back and not have
        # any output plugin
        if output_plugins:
            # TODO: add progress indicator
            output_success = run_codebase_plugins(
                stage='output',
                plugins=output_plugins,
                codebase=codebase,
                stage_msg='Save scan results...',
                plugin_msg=' Save scan results as: %(name)s...',
                quiet=quiet,
                verbose=verbose,
                kwargs=requested_options,
                echo_func=echo_func,
            )
            success = success and output_success

        ########################################################################
        # 9. display summary
        ########################################################################
        codebase.timings['total'] = time() - processing_start

        # TODO: compute summary for output plugins too??
        if not quiet:
            scan_names = ', '.join(p.name for p in scanner_plugins)
            echo_func('Scanning done.', fg='green' if success else 'red')
            display_summary(
                codebase=codebase,
                scan_names=scan_names,
                processes=processes,
                errors=errors,
                echo_func=echo_func,
            )

        ########################################################################
        # 10. optionally assemble results to return
        ########################################################################
        if return_results:
            # the structure is exactly the same as the JSON output
            from formattedcode.output_json import get_results
            results = get_results(codebase, as_list=True, **requested_options)

    finally:
        # remove temporary files
        scancode_temp_dir = scancode_config.scancode_temp_dir
        if keep_temp_files:
            if not quiet:
                msg = 'Keeping temporary files in: "{}".'.format(scancode_temp_dir)
                echo_func(msg, fg='green' if success else 'red')
        else:
            if not quiet:
                echo_func('Removing temporary files...', fg='green', nl=False)

            from commoncode import fileutils
            fileutils.delete(scancode_temp_dir)

            if not quiet:
                echo_func('done.', fg='green')

    return success, results


def run_codebase_plugins(
    stage,
    plugins,
    codebase,
    stage_msg='',
    plugin_msg='',
    quiet=False,
    verbose=False,
    kwargs=None,
    echo_func=echo_stderr,
):
    """
    Run the list of `stage` `plugins` on `codebase` and return True on success and
    False otherwise.
    Display errors and messages based on the `stage_msg` and
    `plugin_msg` string templates and the `quiet` and `verbose` flags.
    Kwargs are passed down when calling the process_codebase method of each
    plugin.
    """
    kwargs = kwargs or {}

    stage_start = time()
    if verbose and plugins:
        echo_func(stage_msg % locals(), fg='green')

    success = True
    # TODO: add progress indicator
    for plugin in plugins:
        name = plugin.name
        plugin_start = time()

        if verbose:
            echo_func(plugin_msg % locals(), fg='green')

        try:
            if TRACE_DEEP:
                from pprint import pformat
                logger_debug('run_codebase_plugins: kwargs passed to %(stage)s:%(name)s' % locals())
                logger_debug(pformat(sorted(kwargs.items())))
                logger_debug()

            plugin.process_codebase(codebase, **kwargs)

        except Exception as _e:
            msg = 'ERROR: failed to run %(stage)s plugin: %(name)s:' % locals()
            echo_func(msg, fg='red')
            tb = traceback.format_exc()
            echo_func(tb)
            codebase.errors.append(msg + '\n' + tb)
            success = False

        timing_key = '%(stage)s:%(name)s' % locals()
        codebase.timings[timing_key] = time() - plugin_start

    codebase.timings[stage] = time() - stage_start
    return success


def run_scanners(
    stage,
    plugins,
    codebase,
    processes,
    timeout,
    timing,
    quiet=False,
    verbose=False,
    kwargs=None,
    echo_func=echo_stderr,
):
    """
    Run the list of `stage` ScanPlugin `plugins` on `codebase`.
    Update the codebase with computed counts and scan results.
    Return True on success and False otherwise.

    Kwargs are passed down when calling the process_codebase method of each
    plugin.

    Use multiple `processes` and limit the runtime of a single scanner
    function to `timeout` seconds.
    Compute detailed timings if `timing` is True.
    Display progress and errors based on the `quiet` and `verbose` flags.
    """

    kwargs = kwargs or {}

    scan_start = time()

    scanners = []
    for plugin in plugins:
        func = plugin.get_scanner(**kwargs)
        scanners.append(Scanner(name=plugin.name, function=func))

    if TRACE_DEEP: logger_debug('run_scanners: scanners:', scanners)
    if not scanners:
        return True

    scan_names = ', '.join(s.name for s in scanners)

    progress_manager = None
    if not quiet:
        echo_func('Scan files for: %(scan_names)s '
                    'with %(processes)d process(es)...' % locals())
        item_show_func = partial(path_progress_message, verbose=verbose)
        progress_manager = partial(progressmanager,
            item_show_func=item_show_func,
            verbose=verbose, file=sys.stderr)

    # TODO: add CLI option to bypass cache entirely?
    scan_success = scan_codebase(
        codebase, scanners, processes, timeout,
        with_timing=timing, progress_manager=progress_manager)

    # TODO: add progress indicator
    # run the process codebase of each scan plugin (most often a no-op)
    scan_process_codebase_success = run_codebase_plugins(
        stage, plugins, codebase,
        stage_msg='Filter %(stage)ss...',
        plugin_msg=' Filter %(stage)s: %(name)s...',
        quiet=quiet, verbose=verbose, kwargs=kwargs,
    )

    scan_success = scan_success and scan_process_codebase_success
    codebase.timings[stage] = time() - scan_start
    scanned_fc = scanned_dc = scanned_sc = 0
    try:
        scanned_fc, scanned_dc, scanned_sc = codebase.compute_counts()
    except:
        msg = 'ERROR: in run_scanners, failed to compute codebase counts:\n'
        msg += traceback.format_exc()
        codebase.errors.append(msg)
        scan_success = False

    codebase.counters[stage + ':scanners'] = scan_names
    codebase.counters[stage + ':files_count'] = scanned_fc
    codebase.counters[stage + ':dirs_count'] = scanned_dc
    codebase.counters[stage + ':size_count'] = scanned_sc

    return scan_success


def scan_codebase(
    codebase,
    scanners,
    processes=1,
    timeout=DEFAULT_TIMEOUT,
    with_timing=False,
    progress_manager=None,
    echo_func=echo_stderr,
):
    """
    Run the `scanners` Scanner objects on the `codebase` Codebase. Return True
    on success or False otherwise.

    Use multiprocessing with `processes` number of processes defaulting to
    single process. Disable multiprocessing with processes 0 or -1. Disable
    threading is processes is -1.

    Run each scanner function for up to `timeout` seconds and fail it otherwise.

    If `with_timing` is True, each Resource is updated with per-scanner
    execution time (as a float in seconds). This is added to the `scan_timings`
    mapping of each Resource as {scanner.name: execution time}.

    Provide optional progress feedback in the UI using the `progress_manager`
    callable that accepts an iterable of tuple of (location, rid, scan_errors,
    scan_result ) as argument.
    """

    # FIXME: this path computation is super inefficient tuples of  (absolute
    # location, resource id)

    # NOTE: we never scan directories
    resources = ((r.location, r.rid) for r in codebase.walk() if r.is_file)

    use_threading = processes >= 0
    runner = partial(
        scan_resource,
        scanners=scanners,
        timeout=timeout,
        with_timing=with_timing,
        with_threading=use_threading
    )

    if TRACE:
        logger_debug('scan_codebase: scanners:', ', '.join(s.name for s in scanners))

    get_resource = codebase.get_resource

    success = True
    pool = None
    scans = None
    try:
        if processes >= 1:
            # maxtasksperchild helps with recycling processes in case of leaks
            pool = get_pool(processes=processes, maxtasksperchild=1000)
            # Using chunksize is documented as much more efficient in the Python
            # doc. Yet "1" still provides a better and more progressive
            # feedback. With imap_unordered, results are returned as soon as
            # ready and out of order so we never know exactly what is processing
            # until completed.
            scans = pool.imap_unordered(runner, resources, chunksize=1)
            pool.close()
        else:
            # no multiprocessing with processes=0 or -1
            scans = map(runner, resources)

        if progress_manager:
            scans = progress_manager(scans)
            # hack to avoid using a context manager
            if hasattr(scans, '__enter__'):
                scans.__enter__()

        while True:
            try:
                (location,
                 rid,
                 scan_errors,
                 scan_time,
                 scan_result,
                 scan_timings) = next(scans)

                if TRACE_DEEP:
                    logger_debug(
                    'scan_codebase: location:', location, 'results:', scan_result)

                resource = get_resource(rid)

                if not resource:
                    # this should never happen
                    msg = (
                        'ERROR: Internal error in scan_codebase: Resource '
                        'at %(rid)r is missing from codebase.\n'
                        'Scan result not saved:\n%(scan_result)r.' % locals()
                    )
                    codebase.errors.append(msg)
                    success = False
                    continue

                if scan_errors:
                    success = False
                    resource.scan_errors.extend(scan_errors)

                if TRACE: logger_debug('scan_codebase: scan_timings:', scan_timings)
                if with_timing and scan_timings:
                    if scan_timings:
                        resource.scan_timings.update(scan_timings)

                # NOTE: here we effectively single threaded the saving a
                # Resource to the cache! .... not sure this is a good or bad
                # thing for scale. Likely not
                # FIXME: should we instead store these in the Plugin resource_attributes?
                # these should be matched
                for key, value in scan_result.items():
                    if not value:
                        # the scan attribute will have a default value
                        continue
                    if key.startswith('extra_data.'):
                        key = key.replace('extra_data.', '')
                        resource.extra_data[key] = value
                    else:
                        setattr(resource, key, value)
                codebase.save_resource(resource)

            except StopIteration:
                break
            except KeyboardInterrupt:
                echo_func('\nAborted with Ctrl+C!', fg='red')
                success = False
                terminate_pool(pool)
                break

    finally:
        # ensure the pool is really dead to work around a Python 2.7.3 bug:
        # http://bugs.python.org/issue15101
        terminate_pool(pool)

        if scans and hasattr(scans, 'render_finish'):
            # hack to avoid using a context manager
            scans.render_finish()
    return success


def terminate_pool(pool):
    """
    Invoke terminate() on a process pool and deal with possible Windows issues.
    """
    if not pool:
        return
    if on_windows:
        terminate_pool_with_backoff(pool)
    else:
        pool.terminate()


def terminate_pool_with_backoff(pool, number_of_trials=3):
    # try a few times to terminate,
    for trial in range(number_of_trials, 1):
        try:
            pool.terminate()
            break
        except WindowsError:
            sleep(trial)


def scan_resource(
    location_rid,
    scanners,
    timeout=DEFAULT_TIMEOUT,
    with_timing=False,
    with_threading=True,
):
    """
    Return a tuple of:
        (location, rid, scan_errors, scan_time, scan_results, timings)
    by running the `scanners` Scanner objects for the file or directory resource
    with id `rid` at `location` provided as a `location_rid` tuple of (location,
    rid) for up to `timeout` seconds. If `with_threading` is False, threading is
    disabled.

    The returned tuple has these values:
    - `location` and `rid` are the original arguments.
    - `scan_errors` is a list of error strings.
    - `scan_results` is a mapping of scan results from all scanners.
    - `scan_time` is the duration in seconds to run all scans for this resource.
    - `timings` is a mapping of scan {scanner.name: execution time in seconds}
      tracking the execution duration each each scan individually.
      `timings` is empty unless `with_timing` is True.

    All these values MUST be serializable/pickable because of the way multi-
    processing/threading works.
    """
    scan_time = time()
    location, rid = location_rid
    results = {}
    scan_errors = []
    timings = {} if with_timing else None

    if not with_threading:
        interruptor = fake_interruptible
    else:
        interruptor = interruptible

    # The timeout is a soft deadline for a scanner to stop processing
    # and start returning values. The kill timeout is otherwise there
    # as a gatekeeper for runaway processes.

    # run each scanner in sequence in its own interruptible
    for scanner in scanners:
        if with_timing:
            start = time()

        try:
            # pass a deadline that the scanner can opt to honor or not
            if timeout:
                deadline = time() + int(timeout / 2.5)
            else:
                deadline = sys.maxsize

            runner = partial(scanner.function, location, deadline=deadline)
            error, values_mapping = interruptor(runner, timeout=timeout)
            if error:
                msg = 'ERROR: for scanner: ' + scanner.name + ':\n' + error
                scan_errors.append(msg)
            # the return value of a scanner fun MUST be a mapping
            if values_mapping:
                results.update(values_mapping)

        except Exception:
            msg = 'ERROR: for scanner: ' + scanner.name + ':\n' + traceback.format_exc()
            scan_errors.append(msg)
        finally:
            if with_timing:
                timings[scanner.name] = time() - start

    scan_time = time() - scan_time

    return location, rid, scan_errors, scan_time, results, timings


def display_summary(codebase, scan_names, processes, errors, echo_func=echo_stderr):
    """
    Display a scan summary.
    """
    error_messages, summary_messages = get_displayable_summary(
        codebase, scan_names, processes, errors)
    for msg in error_messages:
        echo_func(msg, fg='red')

    for msg in summary_messages:
        echo_func(msg)


def get_displayable_summary(codebase, scan_names, processes, errors):
    """
    Return displayable summary messages
    """
    initial_files_count = codebase.counters.get('initial:files_count', 0)
    initial_dirs_count = codebase.counters.get('initial:dirs_count', 0)
    initial_res_count = initial_files_count + initial_dirs_count
    initial_size_count = codebase.counters.get('initial:size_count', 0)
    if initial_size_count:
        initial_size_count = format_size(initial_size_count)
        initial_size_count = 'for %(initial_size_count)s' % locals()
    else:
        initial_size_count = ''

    ######################################################################
    prescan_scan_time = codebase.timings.get('pre-scan-scan', 0.)

    if prescan_scan_time:
        prescan_scan_files_count = codebase.counters.get('pre-scan-scan:files_count', 0)
        prescan_scan_file_speed = round(float(prescan_scan_files_count) / prescan_scan_time , 2)

        prescan_scan_size_count = codebase.counters.get('pre-scan-scan:size_count', 0)

        if prescan_scan_size_count:
            prescan_scan_size_speed = format_size(prescan_scan_size_count / prescan_scan_time)
            prescan_scan_size_speed = '%(prescan_scan_size_speed)s/sec.' % locals()

            prescan_scan_size_count = format_size(prescan_scan_size_count)
            prescan_scan_size_count = 'for %(prescan_scan_size_count)s' % locals()
        else:
            prescan_scan_size_count = ''
            prescan_scan_size_speed = ''

    ######################################################################
    scan_time = codebase.timings.get('scan', 0.)

    scan_files_count = codebase.counters.get('scan:files_count', 0)

    if scan_time:
        scan_file_speed = round(float(scan_files_count) / scan_time , 2)
    else:
        scan_file_speed = 0

    scan_size_count = codebase.counters.get('scan:size_count', 0)

    if scan_size_count:
        if scan_time:
            scan_size_speed = format_size(scan_size_count / scan_time)
        else:
            scan_size_speed = 0

        scan_size_speed = '%(scan_size_speed)s/sec.' % locals()

        scan_size_count = format_size(scan_size_count)
        scan_size_count = 'for %(scan_size_count)s' % locals()
    else:
        scan_size_count = ''
        scan_size_speed = ''

    ######################################################################
    final_files_count = codebase.counters.get('final:files_count', 0)
    final_dirs_count = codebase.counters.get('final:dirs_count', 0)
    final_res_count = final_files_count + final_dirs_count
    final_size_count = codebase.counters.get('final:size_count', 0)
    if final_size_count:
        final_size_count = format_size(final_size_count)
        final_size_count = 'for %(final_size_count)s' % locals()
    else:
        final_size_count = ''

    ######################################################################

    error_messages = []
    errors_count = len(errors)
    if errors:
        error_messages.append('Some files failed to scan properly:')
        for error in errors:
            for me in error.splitlines(False):
                error_messages.append(me)

    ######################################################################

    summary_messages = []
    summary_messages.append('Summary:        %(scan_names)s with %(processes)d process(es)' % locals())
    summary_messages.append('Errors count:   %(errors_count)d' % locals())
    summary_messages.append('Scan Speed:     %(scan_file_speed).2f files/sec. %(scan_size_speed)s' % locals())

    if prescan_scan_time:
        summary_messages.append(
            'Early Scanners Speed:     %(prescan_scan_file_speed).2f '
            'files/sec. %(prescan_scan_size_speed)s' % locals()
        )

    summary_messages.append(
        'Initial counts: %(initial_res_count)d resource(s): '
        '%(initial_files_count)d file(s) '
        'and %(initial_dirs_count)d directorie(s) '
        '%(initial_size_count)s' % locals()
    )

    summary_messages.append(
        'Final counts:   %(final_res_count)d resource(s): '
        '%(final_files_count)d file(s) '
        'and %(final_dirs_count)d directorie(s) '
        '%(final_size_count)s' % locals()
    )

    summary_messages.append('Timings:')

    cle = codebase.get_or_create_current_header().to_dict()
    summary_messages.append('  scan_start: {start_timestamp}'.format(**cle))
    summary_messages.append('  scan_end:   {end_timestamp}'.format(**cle))

    for name, value, in codebase.timings.items():
        if value > 0.1:
            summary_messages.append('  %(name)s: %(value).2fs' % locals())

    # TODO: if timing was requested display top per-scan/per-file stats?

    return error_messages, summary_messages


def collect_errors(codebase, verbose=False):
    """
    Collect and return a list of error strings for all `codebase`-level and
    Resource-level errors. Include stack trace if `verbose` is True.
    """
    errors = []
    errors.extend(codebase.errors or [])

    path_with_errors = [(r.path, r.scan_errors)
                       for r in codebase.walk() if r.scan_errors]

    for errored_path, path_errors in path_with_errors:
        msg = 'Path: ' + errored_path
        if verbose:
            msg = [msg]
            for path_error in path_errors:
                for emsg in path_error.splitlines(False):
                    msg.append('  ' + emsg)
            msg = '\n'.join(msg)
        errors.append(msg)
    return errors


def format_size(size):
    """
    Return a human-readable value for the `size` int or float.

    For example:
    >>> assert format_size(0) == '0 Byte'
    >>> assert format_size(1) == '1 Byte'
    >>> assert format_size(0.123) == '0.1 Byte'
    >>> assert format_size(123) == '123 Bytes'
    >>> assert format_size(1023) == '1023 Bytes'
    >>> assert format_size(1024) == '1 KB'
    >>> assert format_size(2567) == '2.51 KB'
    >>> assert format_size(2567000) == '2.45 MB'
    >>> assert format_size(1024*1024) == '1 MB'
    >>> assert format_size(1024*1024*1024) == '1 GB'
    >>> assert format_size(1024*1024*1024*12.3) == '12.30 GB'
    """
    if not size:
        return '0 Byte'
    if size < 1:
        return '%(size).1f Byte' % locals()
    if size == 1:
        return '%(size)d Byte' % locals()
    size = float(size)
    for symbol in ('Bytes', 'KB', 'MB', 'GB', 'TB'):
        if size < 1024:
            if int(size) == float(size):
                return '%(size)d %(symbol)s' % locals()
            return '%(size).2f %(symbol)s' % locals()
        size = size / 1024.
    return '%(size).2f %(symbol)s' % locals()


def get_pretty_params(ctx, generic_paths=False):
    """
    Return a sorted mapping of {CLI option: pretty value string} for the `ctx`
    Click.context, putting arguments first then options:

        {"input": ~/some/path, "--license": True}

    Skip options that are not set or hidden.
    If `generic_paths` is True, click.File and click.Path parameters are made
    "generic" replacing their value with a placeholder. This is used mostly for
    testing.
    """

    if TRACE:
        logger_debug('get_pretty_params: generic_paths', generic_paths)
    args = []
    options = []

    param_values = ctx.params
    for param in ctx.command.params:
        name = param.name
        value = param_values.get(name)

        if param.is_eager:
            continue
        # This attribute is not yet in Click 6.7 but in head
        if getattr(param, 'hidden', False):
            continue

        if value == param.default:
            continue
        if value is None:
            continue
        if value in (tuple(), [],):
            # option with multiple values, the value is a tuple
            continue

        if isinstance(param.type, click.Path) and generic_paths:
            value = '<path>'

        if isinstance(param.type, click.File):
            if generic_paths:
                value = '<file>'
            else:
                # the value cannot be displayed as-is as this may be an opened file-
                # like object
                vname = getattr(value, 'name', None)
                if vname:
                    value = vname
                else:
                    value = '<file>'

        # coerce to string for non-basic supported types
        if not (value in (True, False, None)
            or isinstance(value, (str, str, bytes, tuple, list, dict, dict))):
            value = repr(value)

        # opts is a list of CLI options as in "--strip-root": the last opt is
        # the CLI option long form by convention
        cli_opt = param.opts[-1]

        if isinstance(param, click.Argument):
            args.append((cli_opt, value))
        else:
            options.append((cli_opt, value))

    return dict(sorted(args) + sorted(options))
