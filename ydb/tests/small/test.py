import random
import pytest
import time


def test_flacky():
    assert random.randint(0, 2) == 1

def test_skiped():
    pytest.skip("becourse I want it")

def test_timeout():
    time.sleep(60)
