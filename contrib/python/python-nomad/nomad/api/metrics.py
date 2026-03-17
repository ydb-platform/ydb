"""Nomad Metrics: https://developer.hashicorp.com/nomad/api-docs/metrics"""

from nomad.api.base import Requester


class Metrics(Requester):
    """
    The /metrics endpoint returns metrics for the current Nomad process.

    https://www.nomadproject.io/api/metrics.html

    Key metrics delivered via the endpoint could be found in Telemetry section:

    https://www.nomadproject.io/docs/agent/telemetry.html
    """

    ENDPOINT = "metrics"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        msg = f"{item} does not exist"
        raise AttributeError(msg)

    def get_metrics(self):
        """Get the metrics

        https://www.nomadproject.io/api/metrics.html

        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(method="get").json()
