import pytest

import dominate
from dominate import tags
from dominate import util

def test_version():
  import dominate
  version = '2.8.0'
  assert dominate.version == version
  assert dominate.__version__ == version


def test_context():
  id1 = dominate.dom_tag._get_thread_context()
  id2 = dominate.dom_tag._get_thread_context()
  assert id1 == id2

