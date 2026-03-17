"""This module offers the DeepDiff, DeepSearch, grep and DeepHash classes."""
import logging

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)8s %(message)s')

from .diff import DeepDiff
from .search import DeepSearch, grep
from .contenthash import DeepHash
from .helper import py3
