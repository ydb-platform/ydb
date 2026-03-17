<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://huggingface.co/datasets/huggingface/documentation-images/raw/main/huggingface_hub-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://huggingface.co/datasets/huggingface/documentation-images/raw/main/huggingface_hub.svg">
    <img alt="huggingface_hub library logo" src="https://huggingface.co/datasets/huggingface/documentation-images/raw/main/huggingface_hub.svg" width="352" height="59" style="max-width: 100%">
  </picture>
  <br/>
  <br/>
</p> 

<p align="center">
    <i>The official Python client for the Huggingface Hub.</i>
</p>

<p align="center">
    <a href="https://huggingface.co/docs/huggingface_hub/en/index"><img alt="Documentation" src="https://img.shields.io/website/http/huggingface.co/docs/huggingface_hub/index.svg?down_color=red&down_message=offline&up_message=online&label=doc"></a>
    <a href="https://github.com/huggingface/huggingface_hub/releases"><img alt="GitHub release" src="https://img.shields.io/github/release/huggingface/huggingface_hub.svg"></a>
    <a href="https://github.com/huggingface/huggingface_hub"><img alt="PyPi version" src="https://img.shields.io/pypi/pyversions/huggingface_hub.svg"></a>
    <a href="https://pypi.org/project/huggingface-hub"><img alt="PyPI - Downloads" src="https://img.shields.io/pypi/dm/huggingface_hub"></a>
    <a href="https://codecov.io/gh/huggingface/huggingface_hub"><img alt="Code coverage" src="https://codecov.io/gh/huggingface/huggingface_hub/branch/main/graph/badge.svg?token=RXP95LE2XL"></a>
</p>

<h4 align="center">
    <p>
        <b>English</b> |
        <a href="https://github.com/huggingface/huggingface_hub/blob/main/i18n/README_de.md">Deutsch</a> |
        <a href="https://github.com/huggingface/huggingface_hub/blob/main/i18n/README_fr.md">Français</a> |
        <a href="https://github.com/huggingface/huggingface_hub/blob/main/i18n/README_hi.md">हिंदी</a> |
        <a href="https://github.com/huggingface/huggingface_hub/blob/main/i18n/README_ko.md">한국어</a> |
        <a href="https://github.com/huggingface/huggingface_hub/blob/main/i18n/README_cn.md">中文 (简体)</a>
    <p>
</h4>

---

**Documentation**: <a href="https://hf.co/docs/huggingface_hub" target="_blank">https://hf.co/docs/huggingface_hub</a>

**Source Code**: <a href="https://github.com/huggingface/huggingface_hub" target="_blank">https://github.com/huggingface/huggingface_hub</a>

---

## Welcome to the huggingface_hub library

The `huggingface_hub` library allows you to interact with the [Hugging Face Hub](https://huggingface.co/), a platform democratizing open-source Machine Learning for creators and collaborators. Discover pre-trained models and datasets for your projects or play with the thousands of machine learning apps hosted on the Hub. You can also create and share your own models, datasets and demos with the community. The `huggingface_hub` library provides a simple way to do all these things with Python.

## Key features

- [Download files](https://huggingface.co/docs/huggingface_hub/en/guides/download) from the Hub.
- [Upload files](https://huggingface.co/docs/huggingface_hub/en/guides/upload) to the Hub.
- [Manage your repositories](https://huggingface.co/docs/huggingface_hub/en/guides/repository).
- [Run Inference](https://huggingface.co/docs/huggingface_hub/en/guides/inference) on deployed models.
- [Search](https://huggingface.co/docs/huggingface_hub/en/guides/search) for models, datasets and Spaces.
- [Share Model Cards](https://huggingface.co/docs/huggingface_hub/en/guides/model-cards) to document your models.
- [Engage with the community](https://huggingface.co/docs/huggingface_hub/en/guides/community) through PRs and comments.

## Installation

Install the `huggingface_hub` package with [pip](https://pypi.org/project/huggingface-hub/):

```bash
pip install huggingface_hub
```

If you prefer, you can also install it with [conda](https://huggingface.co/docs/huggingface_hub/en/installation#install-with-conda).

In order to keep the package minimal by default, `huggingface_hub` comes with optional dependencies useful for some use cases. For example, if you want to use the MCP module, run:

```bash
pip install "huggingface_hub[mcp]"
```

To learn more installation and optional dependencies, check out the [installation guide](https://huggingface.co/docs/huggingface_hub/en/installation).

## Quick start

### Download files

Download a single file

```py
from huggingface_hub import hf_hub_download

hf_hub_download(repo_id="tiiuae/falcon-7b-instruct", filename="config.json")
```

Or an entire repository

```py
from huggingface_hub import snapshot_download

snapshot_download("stabilityai/stable-diffusion-2-1")
```

Files will be downloaded in a local cache folder. More details in [this guide](https://huggingface.co/docs/huggingface_hub/en/guides/manage-cache).

### Login

The Hugging Face Hub uses tokens to authenticate applications (see [docs](https://huggingface.co/docs/hub/security-tokens)). To log in your machine, run the following CLI:

```bash
hf auth login
# or using an environment variable
hf auth login --token $HUGGINGFACE_TOKEN
```

### Create a repository

```py
from huggingface_hub import create_repo

create_repo(repo_id="super-cool-model")
```

### Upload files

Upload a single file

```py
from huggingface_hub import upload_file

upload_file(
    path_or_fileobj="/home/lysandre/dummy-test/README.md",
    path_in_repo="README.md",
    repo_id="lysandre/test-model",
)
```

Or an entire folder

```py
from huggingface_hub import upload_folder

upload_folder(
    folder_path="/path/to/local/space",
    repo_id="username/my-cool-space",
    repo_type="space",
)
```

For details in the [upload guide](https://huggingface.co/docs/huggingface_hub/en/guides/upload).

## Integrating to the Hub.

We're partnering with cool open source ML libraries to provide free model hosting and versioning. You can find the existing integrations [here](https://huggingface.co/docs/hub/libraries).

The advantages are:

- Free model or dataset hosting for libraries and their users.
- Built-in file versioning, even with very large files, thanks to a git-based approach.
- In-browser widgets to play with the uploaded models.
- Anyone can upload a new model for your library, they just need to add the corresponding tag for the model to be discoverable.
- Fast downloads! We use Cloudfront (a CDN) to geo-replicate downloads so they're blazing fast from anywhere on the globe.
- Usage stats and more features to come.

If you would like to integrate your library, feel free to open an issue to begin the discussion. We wrote a [step-by-step guide](https://huggingface.co/docs/hub/adding-a-library) with ❤️ showing how to do this integration.

## Contributions (feature requests, bugs, etc.) are super welcome 💙💚💛💜🧡❤️

Everyone is welcome to contribute, and we value everybody's contribution. Code is not the only way to help the community.
Answering questions, helping others, reaching out and improving the documentations are immensely valuable to the community.
We wrote a [contribution guide](https://github.com/huggingface/huggingface_hub/blob/main/CONTRIBUTING.md) to summarize
how to get started to contribute to this repository.
