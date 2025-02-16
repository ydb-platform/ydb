#!/usr/bin/env python3
import json
import os.path
import argparse
import xml.etree.ElementTree as ET
from xml.dom import minidom


def find_file(root_directory, target_filename):

    for dirpath, _, filenames in os.walk(root_directory):
        if target_filename in filenames:
            return os.path.abspath(os.path.join(dirpath, target_filename))

    return None


def add_properties_to_testcases(xml_file, properties_dict, out_file):

    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Iterate over every testsuite tag
    for testsuite in root.findall('testsuite'):
        directory_name = testsuite.attrib.get('name')

        # Iterate over every testcase tag
        for testcase in testsuite.findall('testcase'):
            testcase_full_name = testcase.attrib.get('name')

            # Parse file name and test name from testcase file name
            if "." in testcase_full_name:
                file_name = ".".join(testcase_full_name.split(".")[:2])
                test_name = ".".join(testcase_full_name.split(".")[2:])

                full_test_path = os.path.join(directory_name, file_name)

                # Check if test exists in properties_dict
                if full_test_path in properties_dict:
                    if test_name in properties_dict[full_test_path]:

                        properties_to_add = properties_dict[full_test_path][test_name]

                        for prop_name, prop_value in properties_to_add.items():
                            ET.SubElement(testcase, 'property', name=prop_name, value=prop_value)

    xml_str = ET.tostring(root, 'utf-8')

    parsed_str = minidom.parseString(xml_str)
    pretty_xml_str = parsed_str.toprettyxml()

    with open(out_file, 'w', encoding='utf-8') as f:
        f.write(pretty_xml_str)



def update_junit(test_dir, junit_file, out_file):
    user_properties_file_name = "pytest_user_properties.json"
    user_properties_file_name = find_file(test_dir, user_properties_file_name)
    if not user_properties_file_name:
        return

    print(user_properties_file_name)

    user_properties = {}
    if os.path.isfile(user_properties_file_name):
        with open(user_properties_file_name, "r") as upf:
            user_properties = json.load(upf)

    add_properties_to_testcases(junit_file, user_properties, out_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test_dir')
    parser.add_argument("--in_file", type=str)
    parser.add_argument("--out_file", type=str)

    args = parser.parse_args()

    update_junit(args.test_dir, args.in_file, args.out_file)


if __name__ == "__main__":
    main()
