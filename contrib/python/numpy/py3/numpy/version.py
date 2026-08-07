
"""
Module to expose more detailed version info for the installed `numpy`
"""
version = "2.1.3"
__version__ = version
full_version = version

git_revision = "98464cc0cbc1f211482a0756ded305bed1599f18"
release = 'dev' not in version and '+' not in version
short_version = version.split("+")[0]
