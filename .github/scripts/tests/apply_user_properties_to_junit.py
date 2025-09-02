#!/usr/bin/env python3
import json
import os
import argparse
import xml.etree.ElementTree as ET
from xml.dom import minidom


def add_properties_to_testcases(root, all_properties):
    # Iterate over every testsuite tag
    for testsuite in root.findall('testsuite'):
        directory_name = testsuite.attrib.get('name')

        # Iterate over every testcase tag
        for testcase in testsuite.findall('testcase'):
            testcase_full_name = testcase.attrib.get('name')

            # Attempt to split the test name  into components
            parts = testcase_full_name.split(".", 2)
            if len(parts) == 3:
                file_name = ".".join(parts[:2])
                test_name = ".".join(parts[2:])

                full_test_path = os.path.join(directory_name, file_name)

                # Check if the full test path and test name exist in all_properties
                if full_test_path in all_properties and test_name in all_properties[full_test_path]:
                    properties_to_add = all_properties[full_test_path][test_name]

                    # Find or create <properties>
                    properties_elem = testcase.find('properties')
                    if properties_elem is None:
                        properties_elem = ET.SubElement(testcase, 'properties')

                    # Add properties if not already present
                    for prop_name, prop_value in properties_to_add.items():
                        exists = False
                        for prop in properties_elem.findall('property'):
                            if prop.attrib.get('name') == prop_name:
                                exists = True
                                break
                        if not exists:
                            ET.SubElement(properties_elem, 'property', name=prop_name, value=prop_value)

def load_all_properties(test_dir):
    all_properties = {}

    for dirpath, _, filenames in os.walk(test_dir):
        for filename in filenames:
            properties_file_path = os.path.abspath(os.path.join(dirpath, filename))

            if os.path.isfile(properties_file_path):
                with open(properties_file_path, "r") as upf:
                    properties = json.load(upf)

                # Merge properties into all_properties
                for key, value in properties.items():
                    if key not in all_properties:
                        all_properties[key] = value
                    else:
                        all_properties[key].update(value)

    return all_properties

def update_junit(test_dir, junit_file, out_file):
    tree = ET.parse(junit_file)
    root = tree.getroot()

    all_properties = load_all_properties(test_dir)
    add_properties_to_testcases(root, all_properties)

    xml_str = ET.tostring(root, 'utf-8')

    parsed_str = minidom.parseString(xml_str)
    pretty_xml_str = parsed_str.toprettyxml()

    with open(out_file, 'w', encoding='utf-8') as f:
        f.write(pretty_xml_str)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test_dir')
    parser.add_argument("--in_file", type=str)
    parser.add_argument("--out_file", type=str)

    args = parser.parse_args()

    update_junit(args.test_dir, args.in_file, args.out_file)

if __name__ == "__main__":
    main()
