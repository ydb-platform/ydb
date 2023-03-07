from argparse import ArgumentParser
import os
import json

test_context = {
    'runtime': {
        'build_root': '',
        'output_path': '',
        'project_path': 'ydb/tests/functional/ydb_cli',
        'source_root': '',
        'work_path': ''
    }
}

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--build-root', required=True, help='YDB build directory')
    parser.add_argument('--source-root', type=str, required=True, help='YDB root directory')
    args = parser.parse_args()
    test_context['runtime']['build_root'] = args.build_root
    test_context['runtime']['output_path'] = os.path.join(
        args.source_root,
        'ydb/tests/functional/ydb_cli/test-results/py3test/testing_out_stuff'
    )
    test_context['runtime']['source_root'] = args.source_root
    test_context['runtime']['work_path'] = os.path.join(
        args.source_root,
        'ydb/tests/functional/ydb_cli/test-results/py3test'
    )
    strdata = json.dumps(test_context, indent=4)
    with open("test.context", "w") as outfile:
        outfile.write(strdata)
