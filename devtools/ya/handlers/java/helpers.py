from __future__ import absolute_import
from __future__ import print_function
import time
import sys

import core.yarg
import build.build_opts as build_opts
import build.graph as build_graph
import build.ya_make as ya_make
from build.build_facade import gen_managed_dep_tree, gen_targets_classpath
from exts.tmp import temp_dir
import yalibrary.formatter as yaformatter

from jbuild.gen import base
from jbuild.gen import consts
from six.moves import map


def fix_windows(path):
    return path.replace('\\', '/')


def get_java_ctx_with_tests(opts):
    jopts = core.yarg.merge_opts(build_opts.ya_make_options(free_build_targets=True)).params()
    jopts.dump_sources = True
    jopts.create_symlinks = False
    jopts.__dict__.update(opts.__dict__)
    jopts.debug_options.append('x')
    jopts.flags["YA_IDE_IDEA"] = "yes"

    import app_ctx  # XXX: via args

    _, tests, _, ctx, _ = build_graph.build_graph_and_tests(
        jopts, check=True, ev_listener=ya_make.get_print_listener(jopts, app_ctx.display), display=app_ctx.display
    )

    return ctx, tests


def get_java_ctx(opts):
    ctx, _ = get_java_ctx_with_tests(opts)
    return ctx


NORMAL = '[[imp]]{path}[[rst]]'
EXCLUDED = '[[unimp]]{path} (omitted because of [[c:red]]EXCLUDE[[unimp]])[[rst]]'
CONFLICT = '[[unimp]]{path} (omitted because of [[c:yellow]]conflict with {conflict_resolution}[[unimp]])[[rst]]'
MANAGED = '[[imp]]{path}[[unimp]] (replaced from [[c:blue]]{orig}[[unimp]] because of [[c:blue]]DEPENDENCY_MANAGEMENT[[unimp]])[[rst]]'
IGNORED = '[[unimp]]{path} (omitted as contrib proxy library)[[rst]]'
DUPLICATE = '[[unimp]]{path} (*)[[rst]]'
DIRECT_MANAGED = '[[imp]]{path}[[unimp]] (replaced from [[c:blue]]unversioned[[unimp]] because of [[c:blue]]DEPENDENCY_MANAGEMENT[[unimp]])[[rst]]'
DIRECT_DEFAULT = (
    '[[imp]]{path}[[unimp]] (replaced from [[c:magenta]]unversioned[[unimp]] to [[c:magenta]]default[[unimp]])[[rst]]'
)


def arrow_str(length):
    s = ''

    if length:
        s += '|   ' * (length - 1) + '|-->'

    return '[[unimp]]' + s + '[[rst]]'


def node_str(dep, excluded=False, expanded_above=False):
    if expanded_above:
        return DUPLICATE.format(path=dep.path)

    elif excluded:
        return EXCLUDED.format(path=dep.path)

    elif dep.omitted_for_conflict:
        return CONFLICT.format(path=dep.path, conflict_resolution=base.basename_unix_path(dep.conflict_resolution_path))

    elif dep.replaced_for_dependency_management:
        return MANAGED.format(path=dep.path, orig=base.basename_unix_path(dep.old_path_for_dependency_management))

    elif dep.is_managed:
        return DIRECT_MANAGED.format(path=dep.path)

    elif dep.is_default:
        return DIRECT_DEFAULT.format(path=dep.path)

    else:
        return NORMAL.format(path=dep.path)


def graph_line(depth, dep, excluded=False, expanded_above=False):
    return arrow_str(depth) + node_str(dep, excluded=excluded, expanded_above=expanded_above)


def raise_not_a_java_path(path):
    raise WrongInputException('{} is not a java module'.format(path))


class WrongInputException(Exception):
    mute = True


def print_ymake_dep_tree(opts):
    with temp_dir() as tmp:
        res, evlog = gen_managed_dep_tree(
            build_root=tmp,
            build_type=opts.build_type,
            build_targets=opts.abs_targets,
            debug_options=opts.debug_options,
            flags=opts.flags,
            ymake_bin=opts.ymake_bin,
        )

    import app_ctx

    ev_listener = ya_make.get_print_listener(opts, app_ctx.display)
    for ev in evlog:
        ev_listener(ev)

    formatter = yaformatter.new_formatter(is_tty=sys.stdout.isatty())
    print(formatter.format_message(res.stdout))
    return any('Type' in ev and ev['Type'] == 'Error' for ev in evlog)


def print_classpath(opts):
    opts.flags['TRAVERSE_RECURSE'] = 'no'
    opts.flags['TRAVERSE_RECURS_FOR_TEST'] = 'no'
    with temp_dir() as tmp:
        res, evlog = gen_targets_classpath(
            build_root=tmp,
            build_type=opts.build_type,
            build_targets=opts.abs_targets,
            debug_options=opts.debug_options,
            flags=opts.flags,
            ymake_bin=opts.ymake_bin,
        )

    import app_ctx

    ev_listener = ya_make.get_print_listener(opts, app_ctx.display)
    for ev in evlog:
        ev_listener(ev)

    formatter = yaformatter.new_formatter(is_tty=sys.stdout.isatty())
    print(formatter.format_message(res.stdout))
    return any('Type' in ev and ev['Type'] == 'Error' for ev in evlog)


def print_test_classpath(opts):
    opts.flags['IGNORE_JAVA_DEPENDENCIES_CONFIGURATION'] = 'yes'
    opts.run_tests = 3
    ctx, tests = get_java_ctx_with_tests(opts)
    remaining_roots = set(map(fix_windows, opts.rel_targets))

    formatter = yaformatter.new_formatter(is_tty=sys.stdout.isatty())
    for test in tests:
        if test.project_path not in remaining_roots:
            continue
        classpath = test.get_classpath()
        if classpath is not None:
            classpath = [item[len(consts.BUILD_ROOT) + 1 :] for item in classpath]
            print(formatter.format_message("[[imp]]{}[[rst]]:\n\t{}".format(test.project_path, '\n\t'.join(classpath))))
            remaining_roots.discard(test.project_path)

    for path in remaining_roots:
        print(formatter.format_message("[[imp]]{}[[rst]] is not a Java test".format(path)))


def iter_all_routes(ctx, src_path, dest_path, route_callback, max_dist=None):
    achievable = {}

    def dest_achievable_from(path):
        if path not in achievable:
            achievable[path] = path == dest_path or any(dest_achievable_from(d.path) for d in ctx.by_path[path].deps)

        return achievable[path]

    stack = []

    def visit(path):
        stack.append(path)

        if path == dest_path:
            route_callback(stack)
            stack.pop()
            return  # no loops

        if max_dist is not None and len(stack) - 1 >= max_dist:
            stack.pop()
            return

        for dep in ctx.by_path[path].deps:
            dep_path = dep.path

            if dest_achievable_from(dep_path):
                visit(dep_path)

        stack.pop()

    if dest_achievable_from(src_path):
        visit(src_path)


def print_route(route):
    dist = len(route) - 1

    if dist < 4:
        s = '[[c:green]]{}[[rst]]: '.format(dist)
    elif dist < 7:
        s = '[[c:yellow]]{}[[rst]]: '.format(dist)
    else:
        s = '[[c:red]]{}[[rst]]: '.format(dist)

    s += '\n[[unimp]]-->'.join(['[[imp]]' + p for p in route])

    return s


def find_all_paths(opts):
    start_time = time.time()
    opts.flags['IGNORE_JAVA_DEPENDENCIES_CONFIGURATION'] = 'yes'
    ctx = get_java_ctx(opts)

    if len(opts.rel_targets) != 2:
        raise WrongInputException(
            'Expected: <from> <to>\n' + 'Got: {}'.format(' '.join(['<' + p + '>' for p in opts.rel_targets]))
        )

    src_path = fix_windows(opts.rel_targets[0])
    dest_path = fix_windows(opts.rel_targets[1])

    if src_path not in ctx.by_path:
        raise_not_a_java_path(src_path)

    if dest_path not in ctx.by_path:
        raise_not_a_java_path(dest_path)

    count = [0]

    formatter = yaformatter.new_formatter(is_tty=sys.stdout.isatty())

    def route_callback(route):
        print(formatter.format_message(print_route(route)))
        count[0] += 1

    try:
        iter_all_routes(ctx, src_path, dest_path, route_callback, max_dist=opts.max_dist)
    finally:
        report = 'Found [[c:{}]]{}[[rst]] paths in [[c:yellow]]{}[[rst]] seconds'.format(
            'green' if count[0] else 'red', count[0], time.time() - start_time
        )
        print(formatter.format_message(report))
