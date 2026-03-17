import binascii
import os
import time
from datetime import datetime

import botocore.session
from botocore import UNSIGNED
from botocore.client import Config

from .sampling_rule import SamplingRule
from aws_xray_sdk.core.models.dummy_entities import DummySegment
from aws_xray_sdk.core.context import Context


class ServiceConnector:
    """
    Connector class that translates Centralized Sampling poller functions to
    actual X-Ray back-end APIs and communicates with X-Ray daemon as the
    signing proxy.
    """
    def __init__(self):
        self._xray_client = self._create_xray_client()
        self._client_id = binascii.b2a_hex(os.urandom(12)).decode('utf-8')
        self._context = Context()

    def _context_wrapped(func):
        """
        Wrapping boto calls with dummy segment. This is because botocore
        has two dependencies (requests and httplib) that might be
        monkey-patched in user code to capture subsegments. The wrapper
        makes sure there is always a non-sampled segment present when
        the connector makes an  AWS API call using botocore.
        This context wrapper doesn't work with asyncio based context
        as event loop is not thread-safe.
        """
        def wrapper(self, *args, **kargs):
            if type(self.context).__name__ == 'AsyncContext':
                return func(self, *args, **kargs)
            segment = DummySegment()
            self.context.set_trace_entity(segment)
            result = func(self, *args, **kargs)
            self.context.clear_trace_entities()
            return result

        return wrapper

    @_context_wrapped
    def fetch_sampling_rules(self):
        """
        Use X-Ray botocore client to get the centralized sampling rules
        from X-Ray service. The call is proxied and signed by X-Ray Daemon.
        """
        new_rules = []

        resp = self._xray_client.get_sampling_rules()
        records = resp['SamplingRuleRecords']

        for record in records:
            rule_def = record['SamplingRule']
            if self._is_rule_valid(rule_def):
                rule = SamplingRule(name=rule_def['RuleName'],
                                    priority=rule_def['Priority'],
                                    rate=rule_def['FixedRate'],
                                    reservoir_size=rule_def['ReservoirSize'],
                                    host=rule_def['Host'],
                                    service=rule_def['ServiceName'],
                                    method=rule_def['HTTPMethod'],
                                    path=rule_def['URLPath'],
                                    service_type=rule_def['ServiceType'])
                new_rules.append(rule)

        return new_rules

    @_context_wrapped
    def fetch_sampling_target(self, rules):
        """
        Report the current statistics of sampling rules and
        get back the new assgiend quota/TTL froom the X-Ray service.
        The call is proxied and signed via X-Ray Daemon.
        """
        now = int(time.time())
        report_docs = self._generate_reporting_docs(rules, now)
        resp = self._xray_client.get_sampling_targets(
            SamplingStatisticsDocuments=report_docs
        )
        new_docs = resp['SamplingTargetDocuments']

        targets_mapping = {}
        for doc in new_docs:
            TTL = self._dt_to_epoch(doc['ReservoirQuotaTTL']) if doc.get('ReservoirQuotaTTL', None) else None
            target = {
                'rate': doc['FixedRate'],
                'quota': doc.get('ReservoirQuota', None),
                'TTL': TTL,
                'interval': doc.get('Interval', None),
            }
            targets_mapping[doc['RuleName']] = target

        return targets_mapping, self._dt_to_epoch(resp['LastRuleModification'])

    def setup_xray_client(self, ip, port, client):
        """
        Setup the xray client based on ip and port.
        If a preset client is specified, ip and port
        will be ignored.
        """
        if not client:
            client = self._create_xray_client(ip, port)
        self._xray_client = client

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, v):
        self._context = v

    def _generate_reporting_docs(self, rules, now):
        report_docs = []

        for rule in rules:
            statistics = rule.snapshot_statistics()
            doc = {
                'RuleName': rule.name,
                'ClientID': self._client_id,
                'RequestCount': statistics['request_count'],
                'BorrowCount': statistics['borrow_count'],
                'SampledCount': statistics['sampled_count'],
                'Timestamp': now,
            }
            report_docs.append(doc)
        return report_docs

    def _dt_to_epoch(self, dt):
        """
        Convert a offset-aware datetime to POSIX time.
        """
        # Added in python 3.3+ and directly returns POSIX time.
        return int(dt.timestamp())

    def _is_rule_valid(self, record):
        # We currently only handle v1 sampling rules.
        return record.get('Version', None) == 1 and \
            record.get('ResourceARN', None) == '*' and \
            record.get('ServiceType', None) and \
            not record.get('Attributes', None)

    def _create_xray_client(self, ip='127.0.0.1', port='2000'):
        session = botocore.session.get_session()
        url = 'http://%s:%s' % (ip, port)
        return session.create_client('xray', endpoint_url=url,
                                     region_name='us-west-2',
                                     config=Config(signature_version=UNSIGNED),
                                     aws_access_key_id='', aws_secret_access_key=''
                                     )
