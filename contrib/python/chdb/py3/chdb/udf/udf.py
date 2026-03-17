import functools
import inspect
import os
import sys
import tempfile
import atexit
import shutil
import textwrap
from xml.etree import ElementTree as ET
import chdb


def generate_udf(func_name, args, return_type, udf_body):
    # generate python script
    with open(f"{chdb.g_udf_path}/{func_name}.py", "w") as f:
        f.write(f"#!{sys.executable}\n")
        f.write("import sys\n")
        f.write("\n")
        for line in udf_body.split("\n"):
            f.write(f"{line}\n")
        f.write("\n")
        f.write("if __name__ == '__main__':\n")
        f.write("    for line in sys.stdin:\n")
        f.write("        args = line.strip().split('\t')\n")
        for i, arg in enumerate(args):
            f.write(f"        {arg} = args[{i}]\n")
        f.write(f"        print({func_name}({', '.join(args)}))\n")
        f.write("        sys.stdout.flush()\n")
    os.chmod(f"{chdb.g_udf_path}/{func_name}.py", 0o755)
    # generate xml file
    xml_file = f"{chdb.g_udf_path}/udf_config.xml"
    root = ET.Element("functions")
    if os.path.exists(xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()
    function = ET.SubElement(root, "function")
    ET.SubElement(function, "type").text = "executable"
    ET.SubElement(function, "name").text = func_name
    ET.SubElement(function, "return_type").text = return_type
    ET.SubElement(function, "format").text = "TabSeparated"
    ET.SubElement(function, "command").text = f"{func_name}.py"
    for arg in args:
        argument = ET.SubElement(function, "argument")
        # We use TabSeparated format, so assume all arguments are strings
        ET.SubElement(argument, "type").text = "String"
        ET.SubElement(argument, "name").text = arg
    tree = ET.ElementTree(root)
    tree.write(xml_file)


def chdb_udf(return_type="String"):
    """
    Decorator for chDB Python UDF(User Defined Function).
    1. The function should be stateless. So, only UDFs are supported, not UDAFs(User Defined Aggregation Function).
    2. Default return type is String. If you want to change the return type, you can pass in the return type as an argument.
        The return type should be one of the following: https://clickhouse.com/docs/en/sql-reference/data-types
    3. The function should take in arguments of type String. As the input is TabSeparated, all arguments are strings.
    4. The function will be called for each line of input. Something like this:
        ```
        def sum_udf(lhs, rhs):
            return int(lhs) + int(rhs)

        for line in sys.stdin:
            args = line.strip().split('\t')
            lhs = args[0]
            rhs = args[1]
            print(sum_udf(lhs, rhs))
            sys.stdout.flush()
        ```
    5. The function should be pure python function. You SHOULD import all python modules used IN THE FUNCTION.
        ```
        def func_use_json(arg):
            import json
            ...
        ```
    6. Python interpertor used is the same as the one used to run the script. Get from `sys.executable`
    """

    def decorator(func):
        func_name = func.__name__
        sig = inspect.signature(func)
        args = list(sig.parameters.keys())
        src = inspect.getsource(func)
        src = textwrap.dedent(src)
        udf_body = src.split("\n", 1)[1]  # remove the first line "@chdb_udf()"
        # create tmp dir and make sure the dir is deleted when the process exits
        if chdb.g_udf_path == "":
            chdb.g_udf_path = tempfile.mkdtemp()

        # clean up the tmp dir on exit
        @atexit.register
        def _cleanup():
            try:
                shutil.rmtree(chdb.g_udf_path)
            except:  # noqa
                pass

        generate_udf(func_name, args, return_type, udf_body)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator
