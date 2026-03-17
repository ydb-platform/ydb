import json
import pkgutil
from random import Random

from .sampling_rule import SamplingRule
from ...exceptions.exceptions import InvalidSamplingManifestError

# `.decode('utf-8')` needed for Python 3.4, 3.5.
local_sampling_rule = json.loads(pkgutil.get_data(__name__, 'sampling_rule.json').decode('utf-8'))

SUPPORTED_RULE_VERSION = (1, 2)


class LocalSampler:
    """
    The local sampler that holds either custom sampling rules
    or default sampling rules defined locally. The X-Ray recorder
    use it to calculate if this segment should be sampled or not
    when local rules are neccessary.
    """
    def __init__(self, rules=local_sampling_rule):
        """
        :param dict rules: a dict that defines custom sampling rules.
        An example configuration:
        {
            "version": 2,
            "rules": [
                {
                    "description": "Player moves.",
                    "host": "*",
                    "http_method": "*",
                    "url_path": "/api/move/*",
                    "fixed_target": 0,
                    "rate": 0.05
                }
            ],
            "default": {
                "fixed_target": 1,
                "rate": 0.1
            }
        }
        This example defines one custom rule and a default rule.
        The custom rule applies a five-percent sampling rate with no minimum
        number of requests to trace for paths under /api/move/. The default
        rule traces the first request each second and 10 percent of additional requests.
        The SDK applies custom rules in the order in which they are defined.
        If a request matches multiple custom rules, the SDK applies only the first rule.
        """
        self.load_local_rules(rules)
        self._random = Random()

    def should_trace(self, sampling_req=None):
        """
        Return True if the sampler decide to sample based on input
        information and sampling rules. It will first check if any
        custom rule should be applied, if not it falls back to the
        default sampling rule.

        All optional arugments are extracted from incoming requests by
        X-Ray middleware to perform path based sampling.
        """
        if sampling_req is None:
            return self._should_trace(self._default_rule)

        host = sampling_req.get('host', None)
        method = sampling_req.get('method', None)
        path = sampling_req.get('path', None)

        for rule in self._rules:
            if rule.applies(host, method, path):
                return self._should_trace(rule)

        return self._should_trace(self._default_rule)

    def load_local_rules(self, rules):
        version = rules.get('version', None)
        if version not in SUPPORTED_RULE_VERSION:
            raise InvalidSamplingManifestError('Manifest version: %s is not supported.', version)

        if 'default' not in rules:
            raise InvalidSamplingManifestError('A default rule must be provided.')

        self._default_rule = SamplingRule(rule_dict=rules['default'],
                                          version=version,
                                          default=True)

        self._rules = []
        if 'rules' in rules:
            for rule in rules['rules']:
                self._rules.append(SamplingRule(rule, version))

    def _should_trace(self, sampling_rule):

        if sampling_rule.reservoir.take():
            return True
        else:
            return self._random.random() < sampling_rule.rate
