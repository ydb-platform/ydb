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
