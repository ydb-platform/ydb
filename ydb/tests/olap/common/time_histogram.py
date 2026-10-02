from timeit import default_timer as timer
import numpy as np


class TimeHistogram:
    def __init__(self, name):
        self.results = []
        self.name = name

    def timeit(self, func):
        start = timer()
        func()
        end = timer()
        self.results.append(end - start)

    def _percentile_ms(self, percent):
        return int(np.percentile(np.array(self.results), percent) * 1000)

    def __str__(self):
        return f"{self.name}: 10% {self._percentile_ms(10)} 30% {self._percentile_ms(30)} 50% {self._percentile_ms(50)} 90% {self._percentile_ms(90)} 99% {self._percentile_ms(99)} ms"

    def __repr__(self):
        return self.str()
