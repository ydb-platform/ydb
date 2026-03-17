
def test_version():
  import dominate
  version = '2.9.1'
  assert dominate.version == version
  assert dominate.__version__ == version
