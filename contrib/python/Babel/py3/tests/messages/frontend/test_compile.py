#
# Copyright (C) 2007-2011 Edgewall Software, 2013-2025 the Babel team
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution. The terms
# are also available at https://github.com/python-babel/babel/blob/master/LICENSE.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at https://github.com/python-babel/babel/commits/master/.

from __future__ import annotations

import pytest

from babel.messages import frontend
from babel.messages.frontend import OptionError
from tests.messages.consts import TEST_PROJECT_DISTRIBUTION_DATA, data_dir
from tests.messages.utils import Distribution


@pytest.fixture
def compile_catalog_cmd(monkeypatch):
    pytest.skip()
    monkeypatch.chdir(data_dir)
    dist = Distribution(TEST_PROJECT_DISTRIBUTION_DATA)
    cmd = frontend.CompileCatalog(dist)
    cmd.initialize_options()
    return cmd


def test_no_directory_or_output_file_specified(compile_catalog_cmd):
    compile_catalog_cmd.locale = 'en_US'
    compile_catalog_cmd.input_file = 'dummy'
    with pytest.raises(OptionError):
        compile_catalog_cmd.finalize_options()


def test_no_directory_or_input_file_specified(compile_catalog_cmd):
    compile_catalog_cmd.locale = 'en_US'
    compile_catalog_cmd.output_file = 'dummy'
    with pytest.raises(OptionError):
        compile_catalog_cmd.finalize_options()
