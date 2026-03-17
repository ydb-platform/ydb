# See: https://packaging.python.org/guides/packaging-namespace-packages/#pkgutil-style-namespace-packages
#
# This file must only contain the following line, or other packages in the databricks.* namespace
# may not be importable. The contents of this file must be byte-for-byte equivalent across all packages.
# If they are not, parallel package installation may lead to clobbered and invalid files.
# Also see https://github.com/databricks/databricks-sdk-py/issues/343.
__path__ = __import__("pkgutil").extend_path(__path__, __name__)
