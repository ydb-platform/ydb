from typing import Sequence, Mapping

from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings

import test_case


class Collection(object):
    _test_cases: Mapping[str, Sequence]

    def __init__(self, ss: Settings):
        self._test_cases = {
            'join': test_case.Factory().make_test_cases(),
        }

    def get(self, key: str) -> Sequence:
        if key not in self._test_cases:
            raise ValueError(f'no such test: {key}')

        return self._test_cases[key]

    def ids(self, key: str) -> Sequence[str]:
        if key not in self._test_cases:
            raise ValueError(f'no such test: {key}')

        return map(lambda tc: tc.name, self._test_cases[key])
