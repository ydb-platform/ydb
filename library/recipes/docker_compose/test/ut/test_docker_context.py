import os
import yaml
import yatest.common

import library.recipes.docker_compose.lib as lib


def test_create_context():
    root = yatest.common.work_path("context_root")
    with open(yatest.common.test_source_path("context.yml")) as f:
        ctx = yaml.safe_load(f)
    context = lib._create_context(ctx, yatest.common.test_source_path("init_dir"), root)
    assert "context1" in context
    expected_context_paths = {
        "context1": [
            "init.txt",
            "dir1/file3.txt",
            "dir1/dir2/file4.txt",
            "file1.txt",
            "dir1/file2.txt",
            "dir1/hello",
        ],
        "context2": [
            "init.txt",
            "file1.txt",
        ]
    }
    for c, expected_paths in expected_context_paths.iteritems():
        assert c in context
        for p in expected_paths:
            assert os.path.exists(os.path.join(root, c, p))
