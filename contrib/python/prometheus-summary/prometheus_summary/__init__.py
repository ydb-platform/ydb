from threading import Lock
from typing import Iterable, Sequence, Optional, Tuple

import prometheus_client
from prometheus_client.samples import Sample
from quantile_estimator import TimeWindowEstimator

from .version import __version__


class Summary(prometheus_client.Summary):
    # pairs of (quantile, allowed error (inaccuracy))
    DEFAULT_INVARIANTS = ((0.50, 0.05), (0.90, 0.01), (0.99, 0.001))

    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Iterable[str] = (),
        namespace: str = "",
        subsystem: str = "",
        unit: str = "",
        registry: Optional[prometheus_client.CollectorRegistry] = prometheus_client.REGISTRY,
        _labelvalues: Optional[Sequence[str]] = None,
        invariants: Sequence[Tuple[float, float]] = DEFAULT_INVARIANTS,
        max_age_seconds: int = 10 * 60,
        age_buckets: int = 5,
    ) -> None:
        self._invariants = invariants
        self._max_age_seconds = max_age_seconds
        self._age_buckets = age_buckets
        self._lock = Lock()
        super().__init__(
            name,
            documentation,
            labelnames=labelnames,
            namespace=namespace,
            subsystem=subsystem,
            unit=unit,
            registry=registry,
            _labelvalues=_labelvalues,
        )
        self._kwargs["invariants"] = invariants
        self._kwargs["max_age_seconds"] = max_age_seconds
        self._kwargs["age_buckets"] = age_buckets

    def _metric_init(self):
        super()._metric_init()
        self._estimator = TimeWindowEstimator(
            *self._invariants,
            max_age_seconds=self._max_age_seconds,
            age_buckets=self._age_buckets,
        )

    def observe(self, amount):
        if not isinstance(amount, (float, int)):
            raise TypeError("Summary only works with int or float")

        amount = float(amount)
        super().observe(amount)
        with self._lock:
            self._estimator.observe(amount)

    def _child_samples(self):
        with self._lock:
            samples = [
                Sample("", {"quantile": str(quantile)}, self._estimator.query(quantile), None, None)
                for quantile, _ in self._invariants
            ]

        samples.extend(super()._child_samples())
        return samples
