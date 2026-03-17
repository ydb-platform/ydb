from unittest.mock import patch
from pyct.report import report

class TestModule:
    __version__ = "1.9.3"
    __file__ = "/mock/opt/anaconda3/envs/pyct/lib/python3.7/site-packages/param/__init__.py"

@patch("importlib.import_module")
@patch("builtins.print")
def test_report_gives_package_version(mock_print, mock_import_module):
    module = TestModule()
    mock_import_module.return_value = module

    report("param")

    mock_print.assert_called_with('param=1.9.3                    # /mock/opt/anaconda3/envs/pyct/lib/python3.7/site-packages/param')

@patch("builtins.print")
@patch("subprocess.check_output")
def test_report_gives_conda_version(mock_check_output, mock_print):
    mock_check_output.side_effect = [b'/mock/opt/anaconda3/condabin/conda\n', b'conda 4.8.3\n']

    report("conda")

    mock_print.assert_called_with("conda=4.8.3                    # /mock/opt/anaconda3/condabin/conda")


@patch("builtins.print")
@patch("subprocess.check_output")
def test_report_gives_python_version(mock_check_output, mock_print):
    mock_check_output.side_effect = [b'/mock/opt/anaconda3/envs/pyct/bin/python\n', b'Python 3.7.7\n']

    report("python")

    mock_print.assert_called_with("python=3.7.7                   # /mock/opt/anaconda3/envs/pyct/bin/python")

@patch("builtins.print")
@patch("platform.platform")
def test_report_gives_system_version(mock_platform, mock_print):
    mock_platform.side_effect = ["Darwin-19.2.0", "Darwin-19.2.0-x86_64-i386-64bit"]

    report("system")

    mock_print.assert_called_with("system=Darwin-19.2.0           # OS: Darwin-19.2.0-x86_64-i386-64bit")

@patch("builtins.print")
def test_unknown_package_output(mock_print):
    report("fake_package")

    mock_print.assert_called_with("fake_package=unknown           # not installed in this environment")
