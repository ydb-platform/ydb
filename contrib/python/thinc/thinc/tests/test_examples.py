import os
from pathlib import Path

import pytest


@pytest.fixture
def test_files(nb_file):
    pytest.importorskip("nbconvert")
    pytest.importorskip("nbformat")
    import nbconvert
    import nbformat
    from nbconvert.preprocessors import ExecutePreprocessor

    if not Path(nb_file).exists():
        return
    kernel_name = os.environ.get("NOTEBOOK_KERNEL", "python3")
    with open(nb_file) as f:
        nb = nbformat.read(f, as_version=4)
    proc = ExecutePreprocessor(timeout=600, kernel_name=kernel_name)
    proc.allow_errors = True
    proc.preprocess(nb, {"metadata": {"path": "/"}})
    cells_with_outputs = [c for c in nb.cells if "outputs" in c]
    for cell in cells_with_outputs:
        for output in cell["outputs"]:
            if output.output_type == "error":
                for l in output.traceback:
                    print(l)
                raise Exception(f"{output.ename}: {output.evalue}")


@pytest.mark.skip(reason="Notebooks should be updated")
@pytest.mark.parametrize(
    "nb_file",
    (
        "examples/01_intro_model_definition_methods.ipynb",
        "examples/05_benchmarking_layers.ipynb",
    ),
)
def test_ipython_notebooks(test_files: None):
    ...


@pytest.mark.skip(reason="these notebooks need special software or hardware")
@pytest.mark.parametrize(
    "nb_file",
    (
        "examples/00_intro_to_thinc.ipynb",
        "examples/02_transformers_tagger_bert.ipynb",
        "examples/03_pos_tagger_basic_cnn.ipynb",
        "examples/03_textcat_basic_neural_bow.ipynb",
        "examples/04_configure_gpu_memory.ipynb",
        "examples/04_parallel_training_ray.ipynb",
        "examples/05_visualizing_models.ipynb",
        "examples/06_predicting_like_terms.ipynb",
    ),
)
def test_ipython_notebooks_slow(test_files: None):
    ...
