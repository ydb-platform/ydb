import logging
from random import Random
import time
import threading

from .local.sampler import LocalSampler
from .rule_cache import RuleCache
from .rule_poller import RulePoller
from .target_poller import TargetPoller
from .connector import ServiceConnector
from .reservoir import ReservoirDecision
from aws_xray_sdk import global_sdk_config

log = logging.getLogger(__name__)


class DefaultSampler:
    """Making sampling decisions based on centralized sampling rules defined
    by X-Ray control plane APIs. It will fall back to local sampler if
    centralized sampling rules are not available.
    """
    def __init__(self):
        self._local_sampler = LocalSampler()
        self._cache = RuleCache()
        self._connector = ServiceConnector()
        self._rule_poller = RulePoller(self._cache, self._connector)
        self._target_poller = TargetPoller(self._cache,
                                           self._rule_poller, self._connector)

        self._xray_client = None
        self._random = Random()
        self._started = False
        self._origin = None
        self._lock = threading.Lock()

    def start(self):
        """
        Start rule poller and target poller once X-Ray daemon address
        and context manager is in place.
        """
        if not global_sdk_config.sdk_enabled():
            return

        with self._lock:
            if not self._started:
                self._rule_poller.start()
                self._target_poller.start()
                self._started = True

    def should_trace(self, sampling_req=None):
        """
        Return the matched sampling rule name if the sampler finds one
        and decide to sample. If no sampling rule matched, it falls back
        to the local sampler's ``should_trace`` implementation.
        All optional arguments are extracted from incoming requests by
        X-Ray middleware to perform path based sampling.
        """
        if not global_sdk_config.sdk_enabled():
            return False

        if not self._started:
            self.start() # only front-end that actually uses the sampler spawns poller threads

        now = int(time.time())
        if sampling_req and not sampling_req.get('service_type', None):
            sampling_req['service_type'] = self._origin
        elif sampling_req is None:
            sampling_req = {'service_type': self._origin}
        matched_rule = self._cache.get_matched_rule(sampling_req, now)
        if matched_rule:
            log.debug('Rule %s is selected to make a sampling decision.', matched_rule.name)
            return self._process_matched_rule(matched_rule, now)
        else:
            log.info('No effective centralized sampling rule match. Fallback to local rules.')
            return self._local_sampler.should_trace(sampling_req)

    def load_local_rules(self, rules):
        """
        Load specified local rules to local fallback sampler.
        """
        self._local_sampler.load_local_rules(rules)

    def load_settings(self, daemon_config, context, origin=None):
        """
        The pollers have dependency on the context manager
        of the X-Ray recorder. They will respect the customer
        specified xray client to poll sampling rules/targets.
        Otherwise they falls back to use the same X-Ray daemon
        as the emitter.
        """
        self._connector.setup_xray_client(ip=daemon_config.tcp_ip,
                                          port=daemon_config.tcp_port,
                                          client=self.xray_client)

        self._connector.context = context
        self._origin = origin

    def _process_matched_rule(self, rule, now):
        # As long as a rule is matched we increment request counter.
        rule.increment_request_count()
        reservoir = rule.reservoir
        sample = True
        # We check if we can borrow or take from reservoir first.
        decision = reservoir.borrow_or_take(now, rule.can_borrow)
        if(decision == ReservoirDecision.BORROW):
            rule.increment_borrow_count()
        elif (decision == ReservoirDecision.TAKE):
            rule.increment_sampled_count()
        # Otherwise we compute based on fixed rate of this sampling rule.
        elif (self._random.random() <= rule.rate):
            rule.increment_sampled_count()
        else:
            sample = False

        if sample:
            return rule.name
        else:
            return False

    @property
    def xray_client(self):
        return self._xray_client

    @xray_client.setter
    def xray_client(self, v):
        self._xray_client = v
