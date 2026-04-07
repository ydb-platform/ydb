import yatest.common
import json
import os

DATA_PATH = yatest.common.source_path('yql/essentials/data/language')
TOOL_PATH = yatest.common.binary_path('yql/essentials/tools/langver_dump/langver_dump')


def test_langver_dump():
    with open(os.path.join(DATA_PATH, "langver.json")) as f:
        langver_from_file = json.load(f)
    res = yatest.common.execute([TOOL_PATH], check_exit_code=True, wait=True)
    langver_from_tool = json.loads(res.stdout)
    assert langver_from_file == langver_from_tool, (
        'JSON_DIFFER\n' 'File:\n %(langver_from_file)s\n\n' 'Tool:\n %(langver_from_tool)s\n' % locals()
    )
