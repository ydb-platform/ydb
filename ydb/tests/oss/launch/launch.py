from argparse import ArgumentParser
import xml.dom.minidom
import pytest
import os


disabled_suites = {
    'clickbench', 'dynumber', 'large_serializable', 'postgresql', 'serializable', 'rename'
}

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--test-dir', required=True, help='tests source dir')
    parser.add_argument('--xml-dir', required=True, help='XML dir')
    parser.add_argument('--suite', help='suite to run')
    args = parser.parse_args()
    xml_res_file = os.path.join(args.xml_dir, 'res.xml')
    suites = []
    if args.suite:
        suites = [args.suite]
    else:
        suites = [name for name in os.listdir(args.test_dir) if os.path.isdir(os.path.join(args.test_dir, name))]
        suites.sort()

    for suite in suites:
        if suite not in disabled_suites:
            print('Running suite: ', suite)
            xml_path = os.path.join(args.xml_dir, suite)
            pytest_args = "-o junit_logging=log -o junit_log_passing_tests=False -v --junit-xml={} {}".format(
                xml_path,
                os.path.join(args.test_dir, suite)
            )
            pytest.main(pytest_args.split(' '))
            dom = xml.dom.minidom.parse(xml_path)
            pretty_xml = dom.toprettyxml()
            with open(xml_res_file, 'a') as xml_res:
                xml_res.write(pretty_xml)
