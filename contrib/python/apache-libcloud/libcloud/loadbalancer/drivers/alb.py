# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__all__ = ["ApplicationLBDriver"]

from libcloud.utils.xml import findall, findtext
from libcloud.common.aws import AWSGenericResponse, SignedAWSConnection
from libcloud.loadbalancer.base import Driver, Member, LoadBalancer
from libcloud.loadbalancer.types import State

VERSION = "2015-12-01"
HOST = "elasticloadbalancing.%s.amazonaws.com"
ROOT = "/%s/" % (VERSION)
NS = "http://elasticloadbalancing.amazonaws.com/doc/{}/".format(VERSION)


class ALBResponse(AWSGenericResponse):
    """
    Amazon ALB response class.
    """

    namespace = NS
    exceptions = {}
    xpath = "Error"


class ALBConnection(SignedAWSConnection):
    version = VERSION
    host = HOST
    responseCls = ALBResponse
    service_name = "elasticloadbalancing"


class ALBTargetGroup:
    """
    AWS ALB target group class
    http://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-target-groups.html
    """

    def __init__(
        self,
        target_group_id,
        name,
        protocol,
        port,
        vpc,
        driver,
        health_check_timeout=5,
        health_check_port="traffic-port",
        health_check_path="/",
        health_check_proto="HTTP",
        health_check_matcher="200",
        health_check_interval=30,
        healthy_threshold=5,
        unhealthy_threshold=2,
        balancers=[],
        members=[],
    ):
        self.id = target_group_id
        self.name = name
        self.protocol = protocol
        self.port = port
        self.vpc = vpc
        self.health_check_timeout = health_check_timeout
        self.health_check_port = health_check_port
        self.health_check_path = health_check_path
        self.health_check_proto = health_check_proto
        self.health_check_matcher = health_check_matcher
        self.health_check_interval = health_check_interval
        self.healthy_threshold = healthy_threshold
        self.unhealthy_threshold = unhealthy_threshold

        self._balancers = balancers
        self._balancers_arns = [lb.id for lb in balancers] if balancers else []
        self._members = members
        self._members_ids = [mb.id for mb in members] if members else []
        self._driver = driver

    @property
    def balancers(self):
        if not self._balancers and self._balancers_arns:
            self._balancers = []
            for balancer_arn in self._balancers_arns:
                self._balancers.append(self._driver.get_balancer(balancer_arn))
        return self._balancers

    @balancers.setter
    def balancers(self, val):
        self._balancers = val
        self._balancers_arns = [lb.id for lb in val] if val else []

    @property
    def members(self):
        if not self._members:
            mbrs = self._driver._ex_get_target_group_members(self)
            self._members = mbrs
            self._members_ids = [mb.id for mb in mbrs] if mbrs else []

        return self._members

    @members.setter
    def members(self, val):
        self._members = val
        self._members_ids = [mb.id for mb in val] if val else []


class ALBListener:
    """
    AWS ALB listener class
    http://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-listeners.html
    """

    def __init__(
        self,
        listener_id,
        protocol,
        port,
        balancer,
        driver,
        action="",
        ssl_policy="",
        ssl_certificate="",
        rules=[],
    ):
        self.id = listener_id
        self.protocol = protocol
        self.port = port
        self.action = action
        self.ssl_policy = ssl_policy
        self.ssl_certificate = ssl_certificate

        self._balancer = balancer
        self._balancer_arn = balancer.id if balancer else None
        self._rules = rules
        self._driver = driver

    @property
    def balancer(self):
        if not self._balancer and self._balancer_arn:
            self._balancer = self._driver.get_balancer(self._balancer_arn)
        return self._balancer

    @balancer.setter
    def balancer(self, val):
        self._balancer = val
        self._balancer_arn = val.id

    @property
    def rules(self):
        if not self._rules:
            self._rules = self._driver._ex_get_rules_for_listener(self)
        return self._rules

    @rules.setter
    def rules(self, val):
        self._rules = val


class ALBRule:
    """
    AWS ALB listener rule class
    http://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-listeners.html#listener-rules
    """

    def __init__(
        self,
        rule_id,
        is_default,
        priority,
        target_group,
        driver,
        conditions={},
        listener=None,
    ):
        self.id = rule_id
        self.is_default = is_default
        self.priority = priority
        self.conditions = conditions

        self._listener = listener
        self._listener_arn = listener.id if listener else None
        self._target_group = target_group
        self._target_group_arn = target_group.id if target_group else None
        self._driver = driver

    @property
    def target_group(self):
        if not self._target_group and self._target_group_arn:
            self._target_group = self._driver.ex_get_target_group(self._target_group_arn)
        return self._target_group

    @target_group.setter
    def target_group(self, val):
        self._target_group = val
        self._target_group_arn = val.id

    @property
    def listener(self):
        if not self._listener and self._listener_arn:
            self._listener = self.driver.ex_get_listener(self._listener_arn)
        return self._listener

    @listener.setter
    def listener(self, val):
        self._listener = val
        self._listener_arn = val.id


class ApplicationLBDriver(Driver):
    name = "Amazon Application Load Balancing"
    website = "http://aws.amazon.com/elasticloadbalancing/"
    connectionCls = ALBConnection
    signature_version = "4"

    def __init__(self, access_id, secret, region, token=None):
        self.token = token
        self.region = region
        self.region_name = region
        super().__init__(access_id, secret, token=token, host=HOST % region, region=region)

    def list_protocols(self):
        """
        Return list of protocols supported by driver

        :rtype: ``list`` of ``strings``
        """
        return ["http", "https"]

    def list_balancers(self):
        """
        List all load balancers

        :rtype: ``list`` of :class:`LoadBalancer`
        """
        params = {"Action": "DescribeLoadBalancers"}
        data = self.connection.request(ROOT, params=params).object
        return self._to_balancers(data)

    def get_balancer(self, balancer_id):
        """
        Get a load balancer object by ARN

        :param  balancer_id: ARN of load balancer you wish to fetch.
        :type  balancer_id: ``str``

        :rtype: :class:`LoadBalancer`
        """
        params = {
            "Action": "DescribeLoadBalancers",
            "LoadBalancerArns.member.1": balancer_id,
        }
        data = self.connection.request(ROOT, params=params).object
        return self._to_balancers(data)[0]

    def create_balancer(
        self,
        name,
        port,
        protocol,
        algorithm,
        members,
        ex_scheme=None,
        ex_security_groups=None,
        ex_subnets=None,
        ex_tags=None,
        ex_ssl_cert_arn=None,
    ):
        """
        Create a new load balancer instance.

        AWS ALB balancer creation consists of 5 steps:
        http://docs.aws.amazon.com/elasticloadbalancing/latest/APIReference/Welcome.html

        create_balancer() is a standard API method so, it's made as a wrapper
        here to preserve compatibility with other drivers where LB creation
        is one-step process. It calls respective ALB methods to assemble
        ready-to-use load balancer.

        :param name: Name of the new load balancer
        :type name: ``str``

        :param port: Port number to setup load balancer listener
        :type port: ``int``

        :param protocol: Load balancer protocol, should be 'HTTP' or 'HTTPS'.
        :type protocol: ``str``

        :param algorithm: Load balancing algorithm. Ignored for AWS ALB.
        :type algorithm: :class:`Algorithm` or ``None``

        :param members: List of Members to attach to the balancer. If 'port'
                        attribute is set for the member - load balancer will
                        send traffic there. Otherwise - load balancer port is
                        used on the memeber's side. 'ip' attribute is ignored.
        :type members: ``list`` of :class:`Member`

        :param ex_scheme: Scheme of load balancer. Can be 'internet-facing' or
                          'internal'.
        :type ex_scheme: ``str``

        :param ex_security_groups: List of load balancer security group ids.
        :type ex_security_groups: ``list`` of ``str``

        :param ex_subnets: List of load balancer subnet ids.
        :type ex_subnets: ``list`` of ``str``

        :param ex_tags: Tags to assign to the load balancer.
        :type ex_tags: ``dict``

        :param ex_ssl_cert_arn: SSL certificate ARN to use when load balancer
                protocol is 'HTTPS'.
        :type ex_ssl_cert_arn: ``str``

        :return: LoadBalancer object
        :rtype: :class:`LoadBalancer`
        """

        ex_scheme = ex_scheme or ""
        ex_security_groups = ex_security_groups or []
        ex_subnets = ex_subnets or []
        ex_tags = ex_tags or {}
        ex_ssl_cert_arn = ex_ssl_cert_arn or ""

        balancer = self.ex_create_balancer(
            name,
            scheme=ex_scheme,
            security_groups=ex_security_groups,
            subnets=ex_subnets,
            tags=ex_tags,
        )

        target_group = self.ex_create_target_group(
            name + "-tg",
            port,
            protocol,
            balancer.extra.get("vpc"),
            health_check_proto=protocol,
        )
        self.ex_register_targets(target_group, members)
        listener = self.ex_create_listener(
            balancer, port, protocol, target_group, ssl_cert_arn=ex_ssl_cert_arn
        )

        balancer.extra["listener"] = listener

        return balancer

    def ex_create_balancer(
        self,
        name,
        addr_type="ipv4",
        scheme="internet-facing",
        security_groups=None,
        subnets=None,
        tags=None,
    ):
        """
        AWS-specific method to create a new load balancer. Since ALB is a
        composite object (load balancer, target group, listener etc) - extra
        methods must be called to assemble ready-to-use balancer.

        :param name: Name of the new load balancer
        :type name: ``str``

        :param addr_type: Load balancer address type. Can be 'ipv4' or 'ipv6'.
        :type addr_type: ``str``

        :param scheme: Scheme of load balancer. Can be 'internet-facing' or
                      'internal'.
        :type scheme: ``str``

        :param security_groups: List of load balancer security group ids.
        :type security_groups: ``list`` of ``str``

        :param subnets: List of load balancer subnet ids.
        :type subnets: ``list`` of ``str``

        :param tags: Tags to assign to the load balancer.
        :type tags: ``dict``

        :return: LoadBalancer object
        :rtype: :class:`LoadBalancer`
        """

        security_groups = security_groups or []
        subnets = subnets or []
        tags = tags or {}

        # mandatory params
        params = {"Action": "CreateLoadBalancer", "Name": name}

        idx = 0
        for subnet in subnets:
            idx += 1
            params["Subnets.member." + str(idx)] = subnet

        # optional params
        params.update(
            {
                "IpAddressType": addr_type,  # Valid Values: ipv4 | dualstack
                "Scheme": scheme,  # Valid Values: internet-facing | internal
            }
        )

        idx = 0
        for sg in security_groups:
            idx += 1
            params["SecurityGroups.member." + str(idx)] = sg

        idx = 0
        for k, v in tags.items():
            idx += 1
            params["Tags.member." + str(idx) + ".Key"] = k
            params["Tags.member." + str(idx) + ".Value"] = v

        data = self.connection.request(ROOT, params=params).object

        xpath = "CreateLoadBalancerResult/LoadBalancers/member"
        for el in findall(element=data, xpath=xpath, namespace=NS):
            balancer = self._to_balancer(el)

        return balancer

    def ex_create_target_group(
        self,
        name,
        port,
        proto,
        vpc,
        health_check_interval=30,
        health_check_path="/",
        health_check_port="traffic-port",
        health_check_proto="HTTP",
        health_check_timeout=5,
        health_check_matcher="200",
        healthy_threshold=5,
        unhealthy_threshold=2,
    ):
        """
        Create a target group for AWS ALB load balancer.

        :param name: Name of target group
        :type name: ``str``

        :param port: The port on which the targets receive traffic.
                    This port is used unless you specify a port override when
                    registering the target.
        :type port: ``int``

        :param proto: The protocol to use for routing traffic to the targets.
                    Can be 'HTTP' or 'HTTPS'.
        :type proto: ``str``

        :param vpc: The identifier of the virtual private cloud (VPC).
        :type vpc: ``str``

        :param health_check_interval: The approximate amount of time, in
                                    seconds, between health checks of an
                                    individual target. The default is
                                    30 seconds.
        :type health_check_interval: ``int``

        :param health_check_path: The ping path that is the destination on
                                the targets for health checks. The default is /
        :type health_check_path: ``str``

        :param health_check_port: The port the load balancer uses when
                                performing health checks on targets.
                                The default is traffic-port, which indicates
                                the port on which each target receives traffic
                                from the load balancer.
        :type health_check_port: ``str``

        :param health_check_proto: The protocol the load balancer uses when
                                performing health checks on targets.
                                Can be 'HTTP' (default) or 'HTTPS'.
        :type health_check_proto: ``str``

        :param health_check_timeout: The amount of time, in seconds, during
                                    which no response from a target means
                                    a failed health check. The default is 5s.
        :type health_check_timeout: ``int``

        :param health_check_matcher: The HTTP codes to use when checking for
                                    a successful response from a target.
                                    Valid values: "200", "200,202", "200-299".
        :type health_check_matcher: ``str``

        :param healthy_threshold: The number of consecutive health checks
                                  successes required before considering
                                  an unhealthy target healthy. The default is 5
        :type healthy_threshold: ``int``

        :param unhealthy_threshold: The number of consecutive health check
                                    failures required before considering
                                    a target unhealthy. The default is 2.
        :type unhealthy_threshold: ``int``

        :return: Target group object.
        :rtype: :class:`ALBTargetGroup`
        """

        # mandatory params
        params = {
            "Action": "CreateTargetGroup",
            "Name": name,
            "Protocol": proto,
            "Port": port,
            "VpcId": vpc,
        }

        # optional params
        params.update(
            {
                # Valid Values: Min value of 5. Max value of 300.
                "HealthCheckIntervalSeconds": health_check_interval,
                "HealthCheckPath": health_check_path,
                "HealthCheckPort": health_check_port,
                # Valid Values: HTTP | HTTPS
                "HealthCheckProtocol": health_check_proto,
                # Valid Range: Min value of 2. Max value of 60.
                "HealthCheckTimeoutSeconds": health_check_timeout,
                # Valid Range: Minimum value of 2. Maximum value of 10.
                "HealthyThresholdCount": healthy_threshold,
                # Valid Range: Minimum value of 2. Maximum value of 10.
                "UnhealthyThresholdCount": unhealthy_threshold,
                # Valid values: "200", "200,202", "200-299"
                "Matcher.HttpCode": health_check_matcher,
            }
        )

        data = self.connection.request(ROOT, params=params).object

        xpath = "CreateTargetGroupResult/TargetGroups/member"
        for el in findall(element=data, xpath=xpath, namespace=NS):
            target_group = self._to_target_group(el)

        return target_group

    def ex_register_targets(self, target_group, members=None):
        """
        Register members as targets at target group

        :param target_group: Target group dict where register members.
        :type target_group: ``dict``

        :param members: List of Members to attach to the balancer. If 'port'
                        attribute is set for the member - load balancer will
                        send traffic there. Otherwise - load balancer port is
                        used on the memeber's side. 'ip' attribute is ignored.
        :type members: ``list`` of :class:`Member`

        :return: True on success, False if no members provided.
        :rtype: ``bool``
        """

        members = members or []

        # mandatory params
        params = {"Action": "RegisterTargets", "TargetGroupArn": target_group.id}

        if not members:
            return False

        idx = 0
        for member in members:
            idx += 1
            params["Targets.member." + str(idx) + ".Id"] = member.id
            if member.port:
                params["Targets.member." + str(idx) + ".Port"] = member.port

        # RegisterTargets doesn't return any useful data
        self.connection.request(ROOT, params=params)

        target_group.members = members

        return True

    def ex_create_listener(
        self,
        balancer,
        port,
        proto,
        target_group,
        action="forward",
        ssl_cert_arn=None,
        ssl_policy=None,
    ):
        """
        Create a listener for application load balancer

        :param balancer: LoadBalancer to create listener for
        :type  balancer: :class:`LoadBalancer`

        :param port: Port number to setup load balancer listener
        :type port: ``int``

        :param proto: Load balancer protocol, should be 'HTTP' or 'HTTPS'.
        :type proto: ``str``

        :param target_group: Target group associated with the listener.
        :type target_group: :class:`ALBTargetGroup`

        :param action: Default action for the listener,
                        valid value is 'forward'
        :type action: ``str``

        :param ssl_cert_arn: SSL certificate ARN to use when listener protocol
                            is 'HTTPS'.
        :type ssl_cert_arn: ``str``

        :param ssl_policy: The security policy that defines which ciphers and
                        protocols are supported. The default is the current
                        predefined security policy.
                        Example: 'ELBSecurityPolicy-2016-08'
        :type ssl_policy: ``str``

        :return: Listener object
        :rtype: :class:`ALBListener`
        """

        ssl_cert_arn = ssl_cert_arn or ""
        ssl_policy = ssl_policy or ""

        # mandatory params
        params = {
            "Action": "CreateListener",
            "LoadBalancerArn": balancer.id,
            "Protocol": proto,  # Valid Values: HTTP | HTTPS
            "Port": port,  # Valid Range: Min value of 1. Max value of 65535.
            "DefaultActions.member.1.Type": action,
            "DefaultActions.member.1.TargetGroupArn": target_group.id,
        }

        # optional params
        if proto == "HTTPS":
            params["Certificates.member.1.CertificateArn"] = ssl_cert_arn
            if ssl_policy:
                params["SslPolicy"] = ssl_policy

        data = self.connection.request(ROOT, params=params).object

        xpath = "CreateListenerResult/Listeners/member"
        for el in findall(element=data, xpath=xpath, namespace=NS):
            listener = self._to_listener(el)
            listener.balancer = balancer

        return listener

    def ex_create_listener_rule(
        self,
        listener,
        priority,
        target_group,
        action="forward",
        condition_field=None,
        condition_value=None,
    ):
        """
        Create a rule for listener.

        :param listener: Listener object where to create rule
        :type listener: :class:`ALBListener`

        :param priority: The priority for the rule. A listener can't have
                        multiple rules with the same priority.
        :type priority: ``str``

        :param target_group: Target group object to associate with rule
        :type target_group: :class:`ALBTargetGroup`

        :param action: Action for the rule, valid value is 'forward'
        :type action: ``str``

        :param condition_field: Rule condition field name. The possible values
                                are 'host-header' and 'path-pattern'.
        :type condition_field: ``str``

        :param condition_value: Value to match. Wildcards are supported, for
                                example: '/img/*'

        :return: Rule object
        :rtype: :class:`ALBRule`
        """

        condition_field = condition_field or ""
        condition_value = condition_value or ""

        # mandatory params
        params = {
            "Action": "CreateRule",
            "ListenerArn": listener.id,
            "Priority": priority,  # Valid Range: Min value of 1. Max: 99999.
            "Actions.member.1.Type": action,
            "Actions.member.1.TargetGroupArn": target_group.id,
            # Valid values are host-header and path-pattern.
            "Conditions.member.1.Field": condition_field,
            "Conditions.member.1.Values.member.1": condition_value,
        }

        data = self.connection.request(ROOT, params=params).object

        xpath = "CreateRuleResult/Rules/member"
        for el in findall(element=data, xpath=xpath, namespace=NS):
            rule = self._to_rule(el)
            rule.listener = listener

        return rule

    def ex_get_target_group(self, target_group_id):
        """
        Get target group object by ARN

        :param target_group_id: ARN of target group
        :type target_group_id: ``str``

        :return: Target group object
        :rtype: :class:`ALBTargetGroup`
        """

        # mandatory params
        params = {
            "Action": "DescribeTargetGroups",
            "TargetGroupArns.member.1": target_group_id,
        }

        data = self.connection.request(ROOT, params=params).object

        return self._to_target_groups(data)[0]

    def ex_get_listener(self, listener_id):
        """
        Get listener object by ARN

        :param listener_id: ARN of listener object to get
        :type listener_id: ``str``

        :return: Listener object
        :rtype: :class:`ALBListener`
        """

        # mandatory params
        params = {"Action": "DescribeListeners", "ListenerArns.member.1": listener_id}

        data = self.connection.request(ROOT, params=params).object
        return self._to_listeners(data)[0]

    def ex_get_rule(self, rule_id):
        """
        Get rule by ARN.

        :param rule_id: ARN of rule
        :type rule_id: ``str``

        :return: Rule object
        :rtype: :class:`ALBRule`
        """

        params = {"Action": "DescribeRules", "RuleArns.member.1": rule_id}

        data = self.connection.request(ROOT, params=params).object
        return self._to_rules(data)[0]

    def _to_listeners(self, data):
        xpath = "DescribeListenersResult/Listeners/member"
        return [self._to_listener(el) for el in findall(element=data, xpath=xpath, namespace=NS)]

    def _to_listener(self, el):
        listener = ALBListener(
            listener_id=findtext(element=el, xpath="ListenerArn", namespace=NS),
            protocol=findtext(element=el, xpath="Protocol", namespace=NS),
            port=int(findtext(element=el, xpath="Port", namespace=NS)),
            balancer=None,
            driver=self.connection.driver,
            action=findtext(element=el, xpath="DefaultActions/member/Type", namespace=NS),
            ssl_policy=findtext(element=el, xpath="SslPolicy", namespace=NS),
            ssl_certificate=findtext(
                element=el, xpath="Certificates/member/CertificateArn", namespace=NS
            ),
        )

        listener._balancer_arn = findtext(element=el, xpath="LoadBalancerArn", namespace=NS)

        return listener

    def _to_balancer(self, el):
        balancer = LoadBalancer(
            id=findtext(element=el, xpath="LoadBalancerArn", namespace=NS),
            name=findtext(element=el, xpath="LoadBalancerName", namespace=NS),
            state=State.UNKNOWN,
            ip=findtext(el, xpath="DNSName", namespace=NS),
            port=None,
            driver=self.connection.driver,
        )

        balancer.extra = {
            "listeners": self._ex_get_balancer_listeners(balancer),
            "tags": self._ex_get_balancer_tags(balancer),
            "vpc": findtext(el, xpath="VpcId", namespace=NS),
        }

        if len(balancer.extra["listeners"]) > 0:
            balancer.port = balancer.extra["listeners"][0].port
        else:
            balancer.port = None

        return balancer

    def _to_balancers(self, data):
        xpath = "DescribeLoadBalancersResult/LoadBalancers/member"
        return [self._to_balancer(el) for el in findall(element=data, xpath=xpath, namespace=NS)]

    def _to_tags(self, data):
        """
        return tags dict
        """
        tags = {}
        xpath = "DescribeTagsResult/TagDescriptions/member/Tags/member"

        for el in findall(element=data, xpath=xpath, namespace=NS):
            key = findtext(element=el, xpath="Key", namespace=NS)
            value = findtext(element=el, xpath="Value", namespace=NS)
            if key:
                tags[key] = value

        return tags

    def _to_rule(self, el):
        def __to_bool__(val):
            return val.lower() in ("yes", "true", "t", "1")

        conditions = {}
        cond_members = findall(element=el, xpath="Conditions/member", namespace=NS)
        for cond_member in cond_members:
            field = findtext(element=cond_member, xpath="Field", namespace=NS)
            conditions[field] = []
            value_members = findall(element=cond_member, xpath="Values/member", namespace=NS)
            for value_member in value_members:
                conditions[field].append(value_member.text)

        rule = ALBRule(
            rule_id=findtext(element=el, xpath="RuleArn", namespace=NS),
            is_default=__to_bool__(findtext(element=el, xpath="IsDefault", namespace=NS)),
            # CreateRule API method accepts only int for priority, however
            # DescribeRules method returns 'default' string for default
            # listener rule. So leaving it as string.
            priority=findtext(element=el, xpath="Priority", namespace=NS),
            target_group=None,
            driver=self.connection.driver,
            conditions=conditions,
        )

        rule._target_group_arn = findtext(
            element=el, xpath="Actions/member/TargetGroupArn", namespace=NS
        )

        return rule

    def _to_rules(self, data):
        xpath = "DescribeRulesResult/Rules/member"
        return [self._to_rule(el) for el in findall(element=data, xpath=xpath, namespace=NS)]

    def _to_target_groups(self, data):
        xpath = "DescribeTargetGroupsResult/TargetGroups/member"
        return [
            self._to_target_group(el) for el in findall(element=data, xpath=xpath, namespace=NS)
        ]

    def _to_target_group(self, el):
        target_group = ALBTargetGroup(
            target_group_id=findtext(element=el, xpath="TargetGroupArn", namespace=NS),
            name=findtext(element=el, xpath="TargetGroupName", namespace=NS),
            protocol=findtext(element=el, xpath="Protocol", namespace=NS),
            port=int(findtext(element=el, xpath="Port", namespace=NS)),
            vpc=findtext(element=el, xpath="VpcId", namespace=NS),
            driver=self.connection.driver,
            health_check_timeout=int(
                findtext(element=el, xpath="HealthCheckTimeoutSeconds", namespace=NS)
            ),
            health_check_port=findtext(element=el, xpath="HealthCheckPort", namespace=NS),
            health_check_path=findtext(element=el, xpath="HealthCheckPath", namespace=NS),
            health_check_proto=findtext(element=el, xpath="HealthCheckProtocol", namespace=NS),
            health_check_interval=int(
                findtext(element=el, xpath="HealthCheckIntervalSeconds", namespace=NS)
            ),
            healthy_threshold=int(
                findtext(element=el, xpath="HealthyThresholdCount", namespace=NS)
            ),
            unhealthy_threshold=int(
                findtext(element=el, xpath="UnhealthyThresholdCount", namespace=NS)
            ),
            health_check_matcher=findtext(element=el, xpath="Matcher/HttpCode", namespace=NS),
        )

        lbs = findall(element=el, xpath="LoadBalancerArns/member", namespace=NS)
        target_group._balancers_arns = [lb_arn.text for lb_arn in lbs]

        return target_group

    def _to_target_group_members(self, data):
        xpath = "DescribeTargetHealthResult/TargetHealthDescriptions/member"
        return [
            self._to_target_group_member(el)
            for el in findall(element=data, xpath=xpath, namespace=NS)
        ]

    def _to_target_group_member(self, el):
        member = Member(
            id=findtext(element=el, xpath="Target/Id", namespace=NS),
            ip=None,
            port=findtext(element=el, xpath="Target/Port", namespace=NS),
            balancer=None,
            extra={"health": findtext(element=el, xpath="TargetHealth/State", namespace=NS)},
        )
        return member

    def _ex_get_target_group_members(self, target_group):
        """
        Return a list of target group member dicts.

        :param target_group: target group to fetch members for
        :type target_group: :class:`ALBTargetGroup`

        :return: list of target group members
        :rtype: ``list`` of :class:`Member`
        """

        params = {"Action": "DescribeTargetHealth", "TargetGroupArn": target_group.id}

        data = self.connection.request(ROOT, params=params).object
        target_group_members = []
        for tg_member in self._to_target_group_members(data):
            tg_member.extra["target_group"] = target_group
            target_group_members.append(tg_member)

        return target_group_members

    def _ex_get_balancer_listeners(self, balancer):
        """
        Return a list of listeners associated with load balancer.

        :param balancer: Load balancer to fetch listeners for
        :type balancer: :class:`LoadBalancer`

        :return: list of listener objects
        :rtype: ``list`` of :class:`ALBListener`
        """
        params = {"Action": "DescribeListeners", "LoadBalancerArn": balancer.id}

        data = self.connection.request(ROOT, params=params).object

        return self._to_listeners(data)

    def _ex_get_rules_for_listener(self, listener):
        """
        Get list of rules associated with listener.

        :param listener: Listener object to fetch rules for
        :type listener: :class:`ALBListener`

        :return: List of rules
        :rtype: ``list`` of :class:`ALBListener`
        """

        params = {"Action": "DescribeRules", "ListenerArn": listener.id}

        data = self.connection.request(ROOT, params=params).object
        rules = self._to_rules(data)
        for rule in rules:
            rule.listener = listener

        return rules

    def _ex_get_balancer_tags(self, balancer):
        """
        Get a dict of load balancer tags.

        :param balancer: Load balancer to fetch tags for
        :type balancer: :class:`LoadBalancer`

        :return: Dictionary of tags (name/value) for load balancer
        :rtype: ``dict``
        """
        params = {"Action": "DescribeTags", "ResourceArns.member.1": balancer.id}
        data = self.connection.request(ROOT, params=params).object
        return self._to_tags(data)

    def _ex_connection_class_kwargs(self):
        pdriver = super()
        kwargs = pdriver._ex_connection_class_kwargs()
        if hasattr(self, "token") and self.token is not None:
            kwargs["token"] = self.token
            kwargs["signature_version"] = "4"
        else:
            kwargs["signature_version"] = self.signature_version

        return kwargs


# Commented out to avoid confusion. In AWS ALB relation between load balancer
# and target group/members is indirect. So it's better to go through full chain
# to obtain required object(s).
# Chain is: balancer->listener->rule->target group->member
#
#    def balancer_list_members(self, balancer):
#         """
#         List members of load balancer
#
#         :param balancer: LoadBalancer to list members for
#         :type  balancer: :class:`LoadBalancer`
#
#         :rtype: ``list`` of :class:`Member`
#         """
#         return balancer.extra.get('members', [])
#
#    def _ex_get_balancer_members(self, balancer):
#        """
#        Fetch members across all listeners/rules/target groups
#
#         :param balancer: load balancer to fetch members for
#         :type balancer: :class:`LoadBalancer`
#
#         :return: list of load balancer members across all target groups
#         :rtype: ``list`` of :class:`Member`
#         """
#         balancer_members = []
#         for listener in balancer.extra.get('listeners', []):
#             for rule in listener.rules:
#                 for tg_member in rule.target_group.members:
#                     tg_member.balancer = balancer
#                     tg_member.extra['target_group'] = rule.target_group
#                     balancer_members.append(tg_member)
#
#         return balancer_members
#
#    def _ex_get_balancer_target_groups(self, balancer):
#        """
#        Return a list of load balancer target groups.
#
#        :param balancer: load balancer to fetch target groups for
#        :type balancer: :class:`LoadBalancer`
#
#        :rtype: ``list`` of :class:`ALBTargetGroup`
#        """
#         params = {
#             'Action': 'DescribeTargetGroups',
#             'LoadBalancerArn': balancer.id
#         }
#
#         data = self.connection.request(ROOT, params=params).object
#
#         return self._to_target_groups(data)
