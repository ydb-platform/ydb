"""Wrapper script for testing the performance of the html5lib HTML 5 parser.

The input data is the spec document for HTML 5, written in HTML 5.
The spec was pulled from http://svn.whatwg.org/webapps/index.
"""
import io
import os.path

import html5lib
import pyperf


__author__ = "collinwinter@google.com (Collin Winter)"


def bench_html5lib(html_file):
    html_file.seek(0)
    html5lib.parse(html_file)


if __name__ == "__main__":
    runner = pyperf.Runner()
    runner.metadata['description'] = (
        "Test the performance of the html5lib parser.")
    runner.metadata['html5lib_version'] = html5lib.__version__

    # Get all our IO over with early.
    filename = os.path.join(os.path.dirname(__file__),
                            "data", "w3_tr_html5.html")
    with open(filename, "rb") as fp:
        html_file = io.BytesIO(fp.read())

    runner.bench_func('html5lib', bench_html5lib, html_file)
