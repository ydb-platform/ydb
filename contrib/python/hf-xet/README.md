<!---
Copyright 2024 The HuggingFace Team. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
<p align="center">
    <a href="https://github.com/huggingface/xet-core/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/huggingface/xet-core.svg?color=blue"></a>
    <a href="https://github.com/huggingface/xet-core/releases"><img alt="GitHub release" src="https://img.shields.io/github/release/huggingface/xet-core.svg"></a>
    <a href="https://github.com/huggingface/xet-core/blob/main/CODE_OF_CONDUCT.md"><img alt="Contributor Covenant" src="https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg"></a>
</p>

<h3 align="center">
  <p>🤗 hf-xet - xet client tech, used in <a target="_blank" href="https://github.com/huggingface/huggingface_hub/">huggingface_hub</a></p>
</h3>

## Welcome

`hf-xet` enables `huggingface_hub` to utilize xet storage for uploading and downloading to HF Hub. Xet storage provides chunk-based deduplication, efficient storage/retrieval with local disk caching, and backwards compatibility with Git LFS. This library is not meant to be used directly, and is instead intended to be used from [huggingface_hub](https://pypi.org/project/huggingface-hub).

## Key features

♻ **chunk-based deduplication implementation**: avoid transferring and storing chunks that are shared across binary files (models, datasets, etc).

🤗 **Python bindings**: bindings for [huggingface_hub](https://github.com/huggingface/huggingface_hub/) package.

↔ **network communications**: concurrent communication to HF Hub Xet backend services (CAS).

🔖 **local disk caching**: chunk-based cache that sits alongside the existing [huggingface_hub disk cache](https://huggingface.co/docs/huggingface_hub/guides/manage-cache).

## Installation

Install the `hf_xet` package with [pip](https://pypi.org/project/hf-xet/):

```bash
pip install hf_xet
```

## Quick Start

`hf_xet` is not intended to be run independently as it is expected to be used from `huggingface_hub`, so to get started with `huggingface_hub` check out the documentation [here]("https://hf.co/docs/huggingface_hub").

## Contributions (feature requests, bugs, etc.) are encouraged & appreciated 💙💚💛💜🧡❤️

Please join us in making hf-xet better. We value everyone's contributions. Code is not the only way to help. Answering questions, helping each other, improving documentation, filing issues all help immensely. If you are interested in contributing (please do!), check out the [contribution guide](https://github.com/huggingface/xet-core/blob/main/CONTRIBUTING.md) for this repository.