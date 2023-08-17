import os
import glob
from xml.etree import ElementTree as ET


def get_or_create_properties(testcase):
    props = testcase.find("properties")
    if props is None:
        props = ET.Element("properties")
        testcase.append(props)
    return props


def add_junit_link_property(testcase, name, url):
    add_junit_property(testcase, f"url:{name}", url)


def add_junit_property(testcase, name, value):
    props = get_or_create_properties(testcase)
    props.append(ET.Element("property", dict(name=name, value=value)))


def add_junit_log_property(testcase, url):
    add_junit_link_property(testcase, "Log", url)


def get_property_value(testcase, name):
    props = testcase.find("properties")
    if props is None:
        return None

    for prop in props.findall("property"):
        if prop.attrib["name"] == name:
            return prop.attrib["value"]


def create_error_testsuite(testcases):
    n = str(len(testcases))
    suite = ET.Element("testsuite", dict(tests=n, errors=n))
    suite.extend(testcases)

    root = ET.Element("testsuites", dict(tests=n, errors=n))
    root.append(suite)
    return ET.ElementTree(root)


def create_error_testcase(shardname, classname, name, log_fn=None, log_url=None):
    testcase = ET.Element("testcase", dict(classname=classname, name=name))
    add_junit_property(testcase, "shard", shardname)
    if log_url:
        add_junit_log_property(testcase, log_url)

    err = ET.Element("error", dict(type="error"))

    if log_fn:
        with open(log_fn, "rt") as fp:
            err.text = fp.read(4096)
    testcase.append(err)

    return testcase


def suite_case_iterator(root):
    for suite in root.findall("testsuite"):
        for case in suite.findall("testcase"):
            cls, method = case.attrib["classname"], case.attrib["name"]
            yield suite, case, cls, method


def iter_xml_files(folder_or_file):
    if os.path.isfile(folder_or_file):
        files = [folder_or_file]
    else:
        files = glob.glob(os.path.join(folder_or_file, "*.xml"))

    for fn in files:
        try:
            tree = ET.parse(fn)
        except ET.ParseError as e:
            print(f"Unable to parse {fn}: {e}")
            continue

        root = tree.getroot()

        if root.tag == "testsuite":
            suites = [root]
        elif root.tag == "testsuites":
            suites = root.findall("testsuite")
        else:
            raise ValueError(f"Invalid root tag {root.tag}")
        for suite in suites:
            for case in suite.findall("testcase"):
                yield fn, suite, case
