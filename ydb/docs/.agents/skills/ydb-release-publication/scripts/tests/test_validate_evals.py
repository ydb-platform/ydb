import sys
import unittest
from pathlib import Path


SCRIPTS = Path(__file__).parents[1]
sys.path.insert(0, str(SCRIPTS))

import validate_evals


class ValidateEvalsTest(unittest.TestCase):
    def test_committed_evals_are_valid(self) -> None:
        self.assertEqual(validate_evals.main(), 0)


if __name__ == "__main__":
    unittest.main()
