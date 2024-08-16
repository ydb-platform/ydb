#!/usr/bin/env bash
set -eux
ya make -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON=yes -DOS_SDK=local -DPYTHON_BIN=python3.8 -DPYTHON_CONFIG=python3.8-config python3_8
ya make -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON=yes -DOS_SDK=local -DPYTHON_BIN=python3.9 -DPYTHON_CONFIG=python3.9-config python3_9
ya make -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON=yes -DOS_SDK=local -DPYTHON_BIN=python3.10 -DPYTHON_CONFIG=python3.10-config python3_10
ya make -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON=yes -DOS_SDK=local -DPYTHON_BIN=python3.11 -DPYTHON_CONFIG=python3.11-config python3_11
ya make -DUSE_ARCADIA_PYTHON=no -DUSE_LOCAL_PYTHON=yes -DOS_SDK=local -DPYTHON_BIN=python3.12 -DPYTHON_CONFIG=python3.12-config python3_12
