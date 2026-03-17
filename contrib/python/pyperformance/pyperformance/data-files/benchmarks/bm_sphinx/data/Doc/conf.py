import os
import sys

sys.path.append(os.path.abspath('tools/extensions'))

extensions = [
    'pyspecific',
    'sphinx.ext.extlinks',
]

manpages_url = 'https://manpages.debian.org/{path}'

# General substitutions.
project = 'Python'
copyright = f"2001, Python Software Foundation"

version = release = sys.version.split(" ", 1)[0]

rst_epilog = f"""
.. |python_version_literal| replace:: ``Python {version}``
.. |python_x_dot_y_literal| replace:: ``python{version}``
.. |usr_local_bin_python_x_dot_y_literal| replace:: ``/usr/local/bin/python{version}``
"""

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
today = ''
# Else, today_fmt is used as the format for a strftime call.
today_fmt = '%B %d, %Y'

# By default, highlight as Python 3.
highlight_language = 'python3'

# Minimum version of sphinx required
needs_sphinx = '6.2.1'

# Create table of contents entries for domain objects (e.g. functions, classes,
# attributes, etc.). Default is True.
toc_object_entries = False

# Disable Docutils smartquotes for several translations
smartquotes_excludes = {
    'languages': ['ja', 'fr', 'zh_TW', 'zh_CN'],
    'builders': ['man', 'text'],
}

# Avoid a warning with Sphinx >= 4.0
root_doc = 'contents'

extlinks = {
    "cve": ("https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-%s", "CVE-%s"),
    "cwe": ("https://cwe.mitre.org/data/definitions/%s.html", "CWE-%s"),
    "pypi": ("https://pypi.org/project/%s/", "%s"),
    "source": ('https://github.com/python/cpython/tree/3.13/%s', "%s"),
}
extlinks_detect_hardcoded_links = True
