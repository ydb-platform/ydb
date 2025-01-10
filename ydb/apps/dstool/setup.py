# -*- coding: utf-8 -*-
import setuptools

long_description = """
# What is ydb-dstool

ydb-dstool stands for YDB Distributed Storage Tool. It facilitates administration of YDB storage.

# How to run ydb-dstool

## Install ydb-dstool package

```bash
user@host:~$ pip install ydb-dstool
```

## Set up environment and run

```bash
user@host:~$ export PATH=${PATH}:${HOME}/.local/bin
user@host:~$ ydb-dstool -e ydb.endpoint cluster list
```

# Where to find more info

https://github.com/ydb-platform/ydb/blob/main/ydb/apps/dstool/README.md
"""

setuptools.setup(
    name="ydb-dstool",
    version="0.0.13",
    description="YDB Distributed Storage Administration Tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Yandex LLC",
    author_email="ydb@yandex-team.ru",
    url="https://github.com/ydb-platform/ydb/tree/main/ydb/apps/dstool",
    license="Apache 2.0",
    package_dir={"": "."},
    packages=[
        'ydb/apps/dstool',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    install_requires=(
        "packaging"
    ),
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "ydb-dstool = ydb.apps.dstool.main:main",
        ]
    }
)
