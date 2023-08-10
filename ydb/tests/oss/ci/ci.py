import contextlib
import os

import pytest


def pytest_sessionstart(session):
    def noop(*args, **kwargs):
        pass

    assert session.config.pluginmanager
    session.config.pluginmanager._check_non_top_pytest_plugins = noop


# noinspection PyProtectedMember
@contextlib.contextmanager
def patch_project_path(item):
    from ya import pytest_config

    ctx = pytest_config.ya._context

    old_path = ctx["project_path"]

    project_path = os.path.relpath(
        os.path.dirname(item.fspath),
        pytest_config.ya._source_root,
    )

    ctx["project_path"] = project_path

    try:
        yield
    finally:
        ctx["project_path"] = old_path


@pytest.hookimpl(hookwrapper=True)
def pytest_make_collect_report(collector):
    with patch_project_path(collector):
        yield


def make_github_link(repo, ref, filepath, lineno):
    result = [f"https://github.com/{repo}/blob/{ref}/{filepath}"]

    if lineno:
        result.append(f"#L{lineno + 1}")

    return "".join(result)


# noinspection PyProtectedMember
@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    from ya import pytest_config
    import library.python.pytest.yatest_tools as tools

    if "PYTEST_XDIST_WORKER" in os.environ:
        out_path = os.path.join(pytest_config.option.output_dir, os.environ["PYTEST_XDIST_WORKER"])
    else:
        out_path = pytest_config.option.output_dir

    rel_filepath = os.path.relpath(item.path, pytest_config.ya._source_root)

    test_dir = tools.normalize_filename(rel_filepath)

    out_path = os.path.join(out_path, test_dir)

    if not os.path.exists(out_path):
        os.makedirs(out_path)

    pytest_config.ya._output_dir = out_path

    junit_props = {
        "filename": rel_filepath,
    }

    if pytest_config.option.github_repo and pytest_config.option.github_ref:
        lineno = item.reportinfo()[1]
        junit_props["url:testcase"] = make_github_link(
            pytest_config.option.github_repo,
            pytest_config.option.github_ref,
            rel_filepath,
            lineno,
        )
    else:
        junit_props["testcase"] = item.nodeid

    for k, v in junit_props.items():
        item.user_properties.append((k, v))


# @pytest.hookimpl(tryfirst=True)
# def pytest_runtest_teardown(item, nextitem):
#     import tarfile
#     from ya import pytest_config
#     import library.python.pytest.yatest_tools as tools
#
#     artifacts_dir = pytest_config.option.artifacts_dir
#     artifacts_url = pytest_config.option.artifacts_url
#
#     if not artifacts_dir:
#         return
#
#     if not os.path.exists(artifacts_dir):
#         os.makedirs(artifacts_dir)
#
#     class_name, test_name = tools.split_node_id(item.nodeid)
#     basename = f"{tools.normalize_filename(class_name)}_{tools.normalize_filename(test_name)}"
#     fn = f"{basename}.tar.gz"
#
#     tar_fn = os.path.join(artifacts_dir, fn)
#
#     with tarfile.open(tar_fn, "w:gz") as tar:
#         tar.add(pytest_config.ya._output_dir, basename)
#
#     if artifacts_url:
#         item.user_properties.append(("url:artifacts", f"{artifacts_url}{fn}"))


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    with patch_project_path(item):
        yield


def pytest_addoption(parser):
    # parser.addoption("--artifacts-dir", help="path to store test artifacts")
    # parser.addoption("--artifacts-url", help="url prefix for artifacts")
    parser.addoption("--github-repo", help="junit: link files to specific github repo")
    parser.addoption("--github-ref", help="junit: link files to specific changeset")
