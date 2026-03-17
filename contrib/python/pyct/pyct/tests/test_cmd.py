import os
import pytest
import pyct.cmd
from pyct.cmd import fetch_data, clean_data, copy_examples, examples

# Same as in pyct/examples/datasets.yml
DATASETS_CONTENT = u"""
data:
  - url: this_should_never_be_used
    title: 'Test Data'
    files:
      - test_data.csv
"""

# Same as in pyct/examples/data/.data_stubs/test_data.csv
TEST_FILE_CONTENT = u"""
name,score,rank
Alice,100.5,1
Bob,50.3,2
Charlie,25,3
"""

REAL_FILE_CONTENT = u"""
name,score,rank
Alice,100.5,1
Bob,50.3,2
Charlie,25,3
Dave,28,4
Eve,25,3
Frank,75,9
"""

EXAMPLE_CONTENT = u"""{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**NOTE:** This is a temporary notebook that gets created for tests."
   ]
  },
 ],
 "metadata": {
  "language_info": {
   "name": "python",
   "pygments_lexer": "ipython3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
"""


@pytest.fixture(autouse=True)
def tmp_module(tmp_path):
    """This sets up a temporary directory structure meant to mimic a module
    """
    project = tmp_path / "static_module"
    project.mkdir()
    examples = project / "examples"
    examples.mkdir()
    (examples / "Test_Example_Notebook.ipynb").write_text(EXAMPLE_CONTENT)
    (examples / "datasets.yml").write_text(DATASETS_CONTENT)
    (examples / "data").mkdir()
    (examples / "data" / ".data_stubs").mkdir()
    (examples / "data" / ".data_stubs" / "test_data.csv").write_text(TEST_FILE_CONTENT)
    return project

@pytest.fixture(autouse=True)
def monkeypatch_find_examples(monkeypatch, tmp_module):
    """Monkeypatching find examples to use a tmp examples.
    """
    def _find_examples(name):
        return os.path.join(str(tmp_module), "examples")
    monkeypatch.setattr(pyct.cmd, '_find_examples', _find_examples)

@pytest.fixture(scope='function')
def tmp_project(tmp_path):
    project = tmp_path / "test_project"
    project.mkdir()
    return project

@pytest.fixture(scope='function')
def tmp_project_with_examples(tmp_path):
    project = tmp_path
    examples = project / "examples"
    examples.mkdir()
    datasets = examples / "datasets.yml"
    datasets.write_text(DATASETS_CONTENT)
    (examples / "data").mkdir()
    example = examples / "Test_Example_Notebook.ipynb"
    example.write_text(u"Fake notebook contents")
    return project

@pytest.fixture(scope='function')
def tmp_project_with_stubs(tmp_project_with_examples):
    project = tmp_project_with_examples
    data_stubs = project / "examples" / "data" / ".data_stubs"
    data_stubs.mkdir()
    return project

@pytest.fixture(scope='function')
def tmp_project_with_test_file(tmp_project_with_stubs):
    project = tmp_project_with_stubs
    data_stub = project /  "examples" / "data" / ".data_stubs" / "test_data.csv"
    data_stub.write_text(TEST_FILE_CONTENT)
    return project


def test_examples_with_use_test_data(tmp_project):
    project = tmp_project
    path = str(project / "examples")
    examples(name="pyct", path=path, use_test_data=True)
    assert (project / "examples" / "data" / "test_data.csv").is_file()
    assert (project / "examples" / "Test_Example_Notebook.ipynb").is_file()

def test_examples_with_prexisting_content_in_target_raises_error(tmp_project_with_examples):
    project = tmp_project_with_examples
    path = str(project / "examples")
    data = project / "examples" / "data" / "test_data.csv"
    data.write_text(REAL_FILE_CONTENT)
    with pytest.raises(ValueError):
        examples(name="pyct", path=path, use_test_data=True)
    assert (project / "examples" / "Test_Example_Notebook.ipynb").is_file()
    assert (project / "examples" / "Test_Example_Notebook.ipynb").read_text() != EXAMPLE_CONTENT
    assert (project / "examples" / "data" / "test_data.csv").is_file()
    assert (project / "examples" / "data" / "test_data.csv").read_text() == REAL_FILE_CONTENT

def test_examples_using_test_data_and_force_with_prexisting_content_in_target(tmp_project_with_examples):
    project = tmp_project_with_examples
    path = str(project / "examples")
    data = project / "examples" / "data" / "test_data.csv"
    data.write_text(REAL_FILE_CONTENT)
    examples(name="pyct", path=path, use_test_data=True, force=True)
    assert (project / "examples" / "Test_Example_Notebook.ipynb").is_file()
    assert (project / "examples" / "Test_Example_Notebook.ipynb").read_text() == EXAMPLE_CONTENT
    assert (project / "examples" / "data" / "test_data.csv").is_file()
    assert (project / "examples" / "data" / "test_data.csv").read_text() == TEST_FILE_CONTENT

def test_copy_examples(tmp_project):
    project = tmp_project
    path = str(project / "examples")
    copy_examples(name="pyct", path=path)
    assert (project / "examples" / "Test_Example_Notebook.ipynb").is_file()

def test_copy_examples_with_prexisting_content_in_target_raises_error(tmp_project_with_examples):
    project = tmp_project_with_examples
    path = str(project / "examples")
    with pytest.raises(ValueError):
        copy_examples(name="pyct", path=path)
    assert (project / "examples" / "Test_Example_Notebook.ipynb").is_file()
    assert (project / "examples" / "Test_Example_Notebook.ipynb").read_text() != EXAMPLE_CONTENT

def test_copy_examples_using_force_with_prexisting_content_in_target(tmp_project_with_examples):
    project = tmp_project_with_examples
    path = str(project / "examples")
    copy_examples(name="pyct", path=path, force=True)
    assert (project / "examples" / "Test_Example_Notebook.ipynb").is_file()
    assert (project / "examples" / "Test_Example_Notebook.ipynb").read_text() == EXAMPLE_CONTENT

def test_fetch_data_using_test_data_with_no_file_in_data_copies_from_stubs(tmp_project_with_test_file):
    project = tmp_project_with_test_file
    path = str(project / "examples")
    fetch_data(name="pyct", path=path, use_test_data=True)
    assert (project / "examples" / "data" / "test_data.csv").is_file()
    assert (project / "examples" / "data" / "test_data.csv").read_text() == TEST_FILE_CONTENT

def test_fetch_data_using_test_data_with_file_in_data_skips(tmp_project_with_test_file):
    project = tmp_project_with_test_file
    path = str(project / "examples")
    data = project / "examples" / "data" / "test_data.csv"
    data.write_text(REAL_FILE_CONTENT)
    fetch_data(name="pyct", path=path, use_test_data=True)
    assert (project / "examples" / "data" / "test_data.csv").is_file()
    assert (project / "examples" / "data" / "test_data.csv").read_text() == REAL_FILE_CONTENT

def test_fetch_data_using_test_data_and_force_with_file_in_data_over_writes(tmp_project_with_test_file):
    project = tmp_project_with_test_file
    path = str(project / "examples")
    data = project / "examples" / "data" / "test_data.csv"
    data.write_text(REAL_FILE_CONTENT)
    fetch_data(name="pyct", path=path, use_test_data=True, force=True)
    assert (project / "examples" / "data" / "test_data.csv").is_file()
    assert (project / "examples" / "data" / "test_data.csv").read_text() == TEST_FILE_CONTENT

def test_clean_data_when_data_file_is_real_does_nothing(tmp_project_with_test_file):
    project = tmp_project_with_test_file
    path = str(project / "examples")
    data = project / "examples" / "data" / "test_data.csv"
    data.write_text(REAL_FILE_CONTENT)
    clean_data(name="pyct", path=path)
    assert (project / "examples" / "data" / "test_data.csv").is_file()
    assert (project / "examples" / "data" / "test_data.csv").read_text() == REAL_FILE_CONTENT

def test_clean_data_when_data_file_is_from_stubs_removes_file_from_data(tmp_project_with_test_file):
    project = tmp_project_with_test_file
    path = str(project / "examples")
    data = project / "examples" / "data" / "test_data.csv"
    data.write_text(TEST_FILE_CONTENT)
    clean_data(name="pyct", path=path)
    assert not (project / "examples" / "data" / "test_data.csv").is_file()
    assert (project / "examples" / "data" / ".data_stubs" / "test_data.csv").is_file()
    assert (project / "examples" / "data" / ".data_stubs" / "test_data.csv").read_text() == TEST_FILE_CONTENT

def test_clean_data_when_file_not_in_data_does_nothing(tmp_project_with_test_file):
    project = tmp_project_with_test_file
    path = str(project / "examples")
    clean_data(name="pyct", path=path)
    assert not (project / "examples" / "data" / "test_data.csv").is_file()
    assert (project / "examples" / "data" / ".data_stubs" / "test_data.csv").is_file()
    assert (project / "examples" / "data" / ".data_stubs" / "test_data.csv").read_text() == TEST_FILE_CONTENT

def test_clean_data_when_stubs_is_empty_does_nothing(tmp_project_with_stubs):
    project = tmp_project_with_stubs
    path = str(project / "examples")
    data = project / "examples" / "data" / "test_data.csv"
    data.write_text(REAL_FILE_CONTENT)
    clean_data(name="pyct", path=path)
    assert (project / "examples" / "data" / "test_data.csv").is_file()
    assert not (project / "examples" / "data" / ".data_stubs" / "test_data.csv").is_file()

def test_clean_data_when_no_stubs_dir_does_nothing(tmp_project_with_examples):
    project = tmp_project_with_examples
    path = str(project / "examples")
    data = project / "examples" / "data" / "test_data.csv"
    data.write_text(REAL_FILE_CONTENT)
    clean_data(name="pyct", path=path)
    assert (project / "examples" / "data" / "test_data.csv").is_file()
