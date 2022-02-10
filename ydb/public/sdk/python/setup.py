# -*- coding: utf-8 -*-
import setuptools

try:
    from ydb_version import VERSION
except ImportError:
    VERSION = "0.0.1-dev"


setuptools.setup(
    name="ydb",
    version=VERSION,
    description="YDB Python library",
    author="Yandex LLC",
    author_email="ydb@yandex-team.ru",
    url="http://github.com/yandex-cloud/ydb-python-sdk",
    license="Apache 2.0",
    package_dir={"": "."},
    packages=setuptools.find_packages("."),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
    ],
    install_requires=(
        "protobuf>=3.13.0",
        "grpcio>=1.5.0",
        "enum-compat>=0.0.1",
    ),
    options={"bdist_wheel": {"universal": True}},
)
