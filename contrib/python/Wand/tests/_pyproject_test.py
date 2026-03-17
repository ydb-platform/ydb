from pathlib import Path
from pytest import mark
from wand.version import VERSION
try:
    import tomllib
except ImportError:
    tomllib = None

CFG_ROOT = Path(__file__).parent.parent
CFG_PATH = CFG_ROOT / "pyproject.toml"

def test_pyproject_exists():
    assert CFG_PATH.exists()

@mark.skipif(tomllib is None, reason="tomllib not available")
def test_pyproject_valid():
    with open(CFG_PATH, 'rb') as fd:
        cfg = tomllib.load(fd)
    assert 'project' in cfg
    project = cfg.get('project', {})
    assert project.get('name') in ['wand', 'Wand']
    assert project.get('version') == VERSION