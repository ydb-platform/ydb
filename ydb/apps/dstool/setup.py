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
    version="0.0.9",
    description="YDB Distributed Storage Administration Tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Yandex LLC",
    author_email="ydb@yandex-team.ru",
    url="https://github.com/ydb-platform/ydb/tree/main/ydb/apps/dstool",
    license="Apache 2.0",
    package_dir={"": "."},
    packages=[
        'ydb/library/actors/protos',
        'ydb/core/protos',
        'ydb/core/fq/libs/config/protos',
        'ydb/library/folder_service/proto',
        'ydb/library/login/protos',
        'ydb/library/mkql_proto/protos',
        'ydb/library/yql/dq/actors/protos',
        'ydb/library/yql/dq/proto',
        'ydb/library/yql/protos',
        'ydb/library/yql/providers/common/proto',
        'ydb/library/yql/providers/s3/proto',
        'ydb/library/yql/public/issue/protos',
        'ydb/public/api/protos/annotations',
        'ydb/public/api/protos/draft',
        'ydb/public/api/protos',
        'ydb/apps/dstool/lib',
        'ydb/apps/dstool',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    install_requires=(
        "protobuf>=3.13.0",
        "grpcio>=1.5.0",
        "packaging"
    ),
    python_requires=">=3.6",
    entry_points={
        "console_scripts": [
            "ydb-dstool = ydb.apps.dstool.main:main",
        ]
    }
)
