# Crowd-Kit: Computational Quality Control for Crowdsourcing

[![Crowd-Kit](https://tlk.s3.yandex.net/crowd-kit/Crowd-Kit-GitHub.png)](https://github.com/Toloka/crowd-kit)

[![PyPI Version][pypi_badge]][pypi_link]
[![GitHub Tests][github_tests_badge]][github_tests_link]
[![Codecov][codecov_badge]][codecov_link]
[![Documentation][docs_badge]][docs_link]
[![Paper][paper_badge]][paper_link]

[pypi_badge]: https://badge.fury.io/py/crowd-kit.svg
[pypi_link]: https://pypi.python.org/pypi/crowd-kit
[github_tests_badge]: https://github.com/Toloka/crowd-kit/actions/workflows/tests.yml/badge.svg?branch=main
[github_tests_link]: https://github.com/Toloka/crowd-kit/actions/workflows/tests.yml
[codecov_badge]: https://codecov.io/gh/Toloka/crowd-kit/branch/main/graph/badge.svg
[codecov_link]: https://codecov.io/gh/Toloka/crowd-kit
[docs_badge]: https://readthedocs.org/projects/crowd-kit/badge/
[docs_link]: https://crowd-kit.readthedocs.io/
[paper_badge]: https://joss.theoj.org/papers/10.21105/joss.06227/status.svg
[paper_link]: https://doi.org/10.21105/joss.06227

**Crowd-Kit** is a powerful Python library that implements commonly-used aggregation methods for crowdsourced annotation and offers the relevant metrics and datasets. We strive to implement functionality that simplifies working with crowdsourced data.

Currently, Crowd-Kit contains:

* implementations of commonly-used aggregation methods for categorical, pairwise, textual, and segmentation responses;
* metrics of uncertainty, consistency, and agreement with aggregate;
* loaders for popular crowdsourced datasets.

Also, the `learning` subpackage contains PyTorch implementations of deep learning from crowds methods and advanced aggregation algorithms.

## Installing

To install Crowd-Kit, run the following command: `pip install crowd-kit`. If you also want to use the `learning` subpackage, type `pip install crowd-kit[learning]`.

If you are interested in contributing to Crowd-Kit, use [uv](https://github.com/astral-sh/uv) to manage the dependencies:

```shell
uv venv
uv pip install -e '.[dev,docs,learning]'
uv tool run pre-commit install
```

We use [pytest](https://pytest.org/) for testing and a variety of linters, including [pre-commit](https://pre-commit.com/), [Black](https://github.com/psf/black), [isort](https://github.com/pycqa/isort), [Flake8](https://github.com/pycqa/flake8), [pyupgrade](https://github.com/asottile/pyupgrade), and [nbQA](https://github.com/nbQA-dev/nbQA), to simplify code maintenance.

## Getting Started

This example shows how to use Crowd-Kit for categorical aggregation using the classical Dawid-Skene algorithm.

First, let us do all the necessary imports.

````python
from crowdkit.aggregation import DawidSkene
from crowdkit.datasets import load_dataset

import pandas as pd
````

Then, you need to read your annotations into Pandas DataFrame with columns `task`, `worker`, `label`. Alternatively, you can download an example dataset:

````python
df = pd.read_csv('results.csv')  # should contain columns: task, worker, label
# df, ground_truth = load_dataset('relevance-2')  # or download an example dataset
````

Then, you can aggregate the workers' responses using the `fit_predict` method from the **scikit-learn** library:

````python
aggregated_labels = DawidSkene(n_iter=100).fit_predict(df)
````

[More usage examples](https://github.com/Toloka/crowd-kit/tree/main/examples)

## Implemented Aggregation Methods

Below is the list of currently implemented methods, including the already available (✅) and in progress (🟡).

### Categorical Responses

| Method | Status |
| ------------- | :-------------: |
| [Majority Vote](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.classification.majority_vote.MajorityVote) | ✅ |
| [One-coin Dawid-Skene](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.classification.dawid_skene.OneCoinDawidSkene) | ✅ |
| [Dawid-Skene](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.classification.dawid_skene.DawidSkene) | ✅ |
| [Gold Majority Vote](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.classification.gold_majority_vote.GoldMajorityVote) | ✅ |
| [M-MSR](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.classification.m_msr.MMSR) | ✅ |
| [Wawa](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.classification.wawa.Wawa) | ✅ |
| [Zero-Based Skill](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.classification.zero_based_skill.ZeroBasedSkill) | ✅ |
| [GLAD](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.classification.glad.GLAD) | ✅ |
| [KOS](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.classification.kos.KOS) | ✅ |
| [MACE](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.classification.mace.MACE) | ✅ |

### Multi-Label Responses

|Method|Status|
|-|:-:|
|[Binary Relevance](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.multilabel.binary_relevance.BinaryRelevance)|✅|

### Textual Responses

| Method | Status |
| ------------- | :-------------: |
| [RASA](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.embeddings.rasa.RASA) | ✅ |
| [HRRASA](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.embeddings.hrrasa.HRRASA) | ✅ |
| [ROVER](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.texts.rover.ROVER) | ✅ |

### Image Segmentation

| Method | Status |
| ------------------ | :------------------: |
| [Segmentation MV](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.image_segmentation.segmentation_majority_vote.SegmentationMajorityVote) | ✅ |
| [Segmentation RASA](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.image_segmentation.segmentation_rasa.SegmentationRASA) | ✅ |
| [Segmentation EM](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.image_segmentation.segmentation_em.SegmentationEM) | ✅ |

### Pairwise Comparisons

| Method | Status |
| -------------- | :---------------------: |
| [Bradley-Terry](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.pairwise.bradley_terry.BradleyTerry) | ✅ |
| [Noisy Bradley-Terry](https://toloka.ai/docs/crowd-kit/reference/crowdkit.aggregation.pairwise.noisy_bt.NoisyBradleyTerry) | ✅ |

> [!TIP]
> Consider using the more modern [Evalica](https://github.com/dustalov/evalica) library to aggregate pairwise comparisons.

### Learning from Crowds

|Method|Status|
|-|:-:|
|[CrowdLayer](https://toloka.ai/docs/crowd-kit/reference/crowdkit.learning.crowd_layer.CrowdLayer)|✅|
|[CoNAL](https://toloka.ai/docs/crowd-kit/reference/crowdkit.learning.conal.CoNAL)|✅|

## Citation

* Ustalov D., Pavlichenko N., Tseitlin B. (2024). [Learning from Crowds with Crowd-Kit](https://doi.org/10.21105/joss.06227). Journal of Open Source Software, 9(96), 6227

```bibtex
@article{CrowdKit,
  author    = {Ustalov, Dmitry and Pavlichenko, Nikita and Tseitlin, Boris},
  title     = {{Learning from Crowds with Crowd-Kit}},
  year      = {2024},
  journal   = {Journal of Open Source Software},
  volume    = {9},
  number    = {96},
  pages     = {6227},
  publisher = {The Open Journal},
  doi       = {10.21105/joss.06227},
  issn      = {2475-9066},
  eprint    = {2109.08584},
  eprinttype = {arxiv},
  eprintclass = {cs.HC},
  language  = {english},
}
```

## Support and Contributions

Please use [GitHub Issues](https://github.com/Toloka/crowd-kit/issues) to seek support and submit feature requests. We accept contributions to Crowd-Kit via GitHub as according to our guidelines in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

&copy; Crowd-Kit team authors, 2020&ndash;2025. Licensed under the Apache License, Version 2.0. See LICENSE file for more details.
