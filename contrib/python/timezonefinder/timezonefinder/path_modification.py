"""modify PYTHONPATH to make parent package discoverable."""

import sys
from os.path import abspath, join, pardir

package_path = abspath(join(__file__, pardir, pardir))
sys.path.insert(0, package_path)
