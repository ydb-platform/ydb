from nanoid.algorithm import algorithm_generate


def test_generates_random_buffers():
    numbers = {}
    random_bytes = algorithm_generate(10000)
    assert len(random_bytes) == 10000
    for i in range(len(random_bytes)):
        if not numbers.get(random_bytes[i]):
            numbers[random_bytes[i]] = 0
        numbers[random_bytes[i]] += 1
        assert type(random_bytes[i]) == int
        assert random_bytes[i] <= 255
        assert random_bytes[i] >= 0


def test_generates_small_random_buffers():
    assert len(algorithm_generate(10)) == 10
