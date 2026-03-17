# SPDX-Copyright: Copyright (c) Capital One Services, LLC
# SPDX-License-Identifier: Apache-2.0
# Copyright 2020 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

"""
Functions for C7N features when evaluating CEL expressions.

These functions provide a mapping between C7N features and CEL.

These functions are exposed by the global ``FUNCTIONS`` dictionary that is provided
to the CEL evaluation run-time to provide necessary C7N features.

The functions rely on implementation details in the ``CELFilter`` class.

The API
=======

C7N uses CEL and the :py:mod:`c7nlib` module as follows::

    class CELFilter(c7n.filters.core.Filter):  # See below for the long list of mixins.
        decls = {
            "resource": celpy.celtypes.MapType,
            "now": celpy.celtypes.TimestampType,
            "event": celpy.celtypes.MapType,
        }
        decls.update(celpy.c7nlib.DECLARATIONS)

        def __init__(self, expr: str) -> None:
            self.expr = expr

        def validate(self) -> None:
            cel_env = celpy.Environment(
                annotations=self.decls,
                runner_class=c7nlib.C7N_Interpreted_Runner)
            cel_ast = cel_env.compile(self.expr)
            self.pgm = cel_env.program(cel_ast, functions=celpy.c7nlib.FUNCTIONS)

        def process(self,
            resources: Iterable[celpy.celtypes.MapType]) -> Iterator[celpy.celtypes.MapType]:
            now = datetime.datetime.utcnow()
            for resource in resources:
                with C7NContext(filter=the_filter):
                    cel_activation = {
                        "resource": celpy.json_to_cel(resource),
                        "now": celpy.celtypes.TimestampType(now),
                    }
                    if self.pgm.evaluate(cel_activation):
                        yield resource

This isn't the whole story, this is the starting point.

This library of functions is bound into the program that's built from the AST.

Several objects are required in activation for use by the CEL expression

-   ``resource``. The JSON document describing the cloud resource.

-   ``now.`` The current timestamp.

-   Optionally, ``event`` may have an AWS CloudWatch Event.


The type: value Features
========================

The core value features of C7N require a number of CEL extensions.

-   :func:`glob(string, pattern)` uses Python fnmatch rules. This implements ``op: glob``.

-   :func:`difference(list, list)` creates intermediate sets and computes the difference
    as a boolean value. Any difference is True.  This implements ``op: difference``.

-   :func:`intersect(list, list)` creats intermediate sets and computes the intersection
    as a boolean value. Any interection is True.  This implements ``op: intersect``.

-   :func:`normalize(string)` supports normalized comparison between strings.
    In this case, it means lower cased and trimmed. This implements ``value_type: normalize``.

-   :func:`net.cidr_contains` checks to see if a given CIDR block contains a specific
    address.  See https://www.openpolicyagent.org/docs/latest/policy-reference/#net.

-   :func:`net.cidr_size` extracts the prefix length of a parsed CIDR block.

-   :func:`version` uses ``disutils.version.LooseVersion`` to compare version strings.

-   :func:`resource_count` function. This is TBD.

The type: value_from features
==============================

This relies on  ``value_from()`` and ``jmes_path_map()`` functions

In context, it looks like this::

    value_from("s3://c7n-resources/exemptions.json", "json")
    .jmes_path_map('exemptions.ec2.rehydration.["IamInstanceProfile.Arn"][].*[].*[]')
    .contains(resource["IamInstanceProfile"]["Arn"])

The ``value_from()`` function reads values from a given URI.

-   A full URI for an S3 bucket.

-   A full URI for a server that supports HTTPS GET requests.

If a format is given, this is used, otherwise it's based on the
suffix of the path.

The ``jmes_path_map()`` function compiles and applies a JMESPath
expression against each item in the collection to create a
new collection.  To an extent, this repeats functionality
from the ``map()`` macro.

Additional Functions
====================

A number of C7N subclasses of ``Filter`` provide additional features. There are
at least 70-odd functions that are expressed or implied by these filters.

Because the CEL expressions are always part of a ``CELFilter``, all of these
additional C7N features need to be transformed into "mixins" that are implemented
in two places. The function is part of the legacy subclass of ``Filter``,
and the function is also part of ``CELFilter``.

::

    class InstanceImageMixin:
        # from :py:class:`InstanceImageBase` refactoring
        def get_instance_image(self):
            pass

    class RelatedResourceMixin:
        # from :py:class:`RelatedResourceFilter` mixin
        def get_related_ids(self):
            pass

        def get_related(self):
            pass

    class CredentialReportMixin:
        # from :py:class:`c7n.resources.iam.CredentialReport` filter.
        def get_credential_report(self):
            pass

    class ResourceKmsKeyAliasMixin:
        # from :py:class:`c7n.resources.kms.ResourceKmsKeyAlias`
        def get_matching_aliases(self, resource):
            pass

    class CrossAccountAccessMixin:
        # from :py:class:`c7n.filters.iamaccessfilter.CrossAccountAccessFilter`
        def get_accounts(self, resource):
            pass
        def get_vpcs(self, resource):
            pass
        def get_vpces(self, resource):
            pass
        def get_orgids(self, resource):
            pass
        # from :py:class:`c7n.resources.secretsmanager.CrossAccountAccessFilter`
        def get_resource_policy(self, resource):
            pass

    class SNSCrossAccountMixin:
        # from :py:class:`c7n.resources.sns.SNSCrossAccount`
        def get_endpoints(self, resource):
            pass
        def get_protocols(self, resource):
            pass

    class ImagesUnusedMixin:
        # from :py:class:`c7n.resources.ami.ImageUnusedFilter`
        def _pull_ec2_images(self, resource):
            pass
        def _pull_asg_images(self, resource):
            pass

    class SnapshotUnusedMixin:
        # from :py:class:`c7n.resources.ebs.SnapshotUnusedFilter`
        def _pull_asg_snapshots(self, resource):
            pass
        def _pull_ami_snapshots(self, resource):
            pass

    class IamRoleUsageMixin:
        # from :py:class:`c7n.resources.iam.IamRoleUsage`
        def service_role_usage(self, resource):
            pass
        def instance_profile_usage(self, resource):
            pass

    class SGUsageMixin:
        # from :py:class:`c7n.resources.vpc.SGUsage`
        def scan_groups(self, resource):
            pass

    class IsShieldProtectedMixin:
        # from :py:mod:`c7n.resources.shield`
        def get_type_protections(self, resource):
            pass

    class ShieldEnabledMixin:
        # from :py:class:`c7n.resources.account.ShieldEnabled`
        def account_shield_subscriptions(self, resource):
            pass

    class CELFilter(
        InstanceImageMixin, RelatedResourceMixin, CredentialReportMixin,
        ResourceKmsKeyAliasMixin, CrossAccountAccessMixin, SNSCrossAccountMixin,
        ImagesUnusedMixin, SnapshotUnusedMixin, IamRoleUsageMixin, SGUsageMixin,
        Filter,
    ):
        '''Container for functions used by c7nlib to expose data to CEL'''
        def __init__(self, data, manager) -> None:
            super().__init__(data, manager)
            assert data["type"].lower() == "cel"
            self.expr = data["expr"]
            self.parser = c7n.filters.offhours.ScheduleParser()

        def validate(self):
            pass  # See above example

        def process(self, resources):
            pass  # See above example

This is not the complete list. See the ``tests/test_c7nlib.py`` for the ``celfilter_instance``
fixture which contains **all** of the functions required.

C7N Context Object
==================

A number of the functions require access to C7N features that are not simply part
of the resource being filtered. There are two alternative ways to handle this dependency:

-   A global C7N context object that has the current ``CELFilter`` providing
    access to C7N internals.

-   A ``C7N`` argument to the functions that need C7N access.
    This would be provided in the activation context for CEL.

To keep the library functions looking simple, the module global ``C7N`` is used.
This avoids introducing a non-CEL parameter to the c7nlib functions.

The ``C7N`` context object contains the following attributes:

-   ``filter``. The original C7N ``Filter`` object. This provides access to the
    resource manager. It can be used to manage supplemental
    queries using C7N caches and other resource management.

This is set by the :py:class:`C7NContext` prior to CEL evaluation.

Name Resolution
===============

Note that names are **not** resolved via a lookup in the program object,
an instance of the :py:class:`celpy.Runner` class. To keep these functions
simple, the runner is not part of the run-time, and name resolution
will appear to be "hard-wrired" among these functions.

This is rarely an issue, since most of these functions are independent.
The :func:`value_from` function relies on :func:`text_from` and :func:`parse_text`.
Changing either of these functions with an override won't modify the behavior
of :func:`value_from`.
"""

import csv
import fnmatch
import io
import ipaddress
import json
import logging
import os.path
import sys
import urllib.request
import zlib
from contextlib import closing
from packaging.version import Version
from types import TracebackType
from typing import Any, Callable, Dict, Iterator, List, Optional, Type, Union, cast

# import dateutil
from pendulum import parse as parse_date
import jmespath  # type: ignore [import-untyped]

from celpy import InterpretedRunner, celtypes
from celpy.adapter import json_to_cel
from celpy.evaluation import Annotation, Context, Evaluator

logger = logging.getLogger(f"celpy.{__name__}")


class C7NContext:
    """
    Saves current C7N filter for use by functions in this module.

    This is essential for making C7N filter available to *some* of these functions.

    ::

        with C7NContext(filter):
            cel_prgm.evaluate(cel_activation)
    """

    def __init__(self, filter: Any) -> None:
        self.filter = filter

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}(filter={self.filter!r})"

    def __enter__(self) -> None:
        global C7N
        C7N = self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        global C7N
        C7N = cast("C7NContext", None)
        return


# An object used for access to the C7N filter.
# A module global makes the interface functions much simpler.
# They can rely on `C7N.filter` providing the current `CELFilter` instance.
C7N = cast("C7NContext", None)


def key(source: celtypes.ListType, target: celtypes.StringType) -> celtypes.Value:
    """
    The C7N shorthand ``tag:Name`` doesn't translate well to CEL. It extracts a single value
    from a sequence of objects with a ``{"Key": x, "Value": y}`` structure; specifically,
    the value for ``y`` when ``x == "Name"``.

    This function locate a particular "Key": target within a list of {"Key": x, "Value", y} items,
    returning the y value if one is found, null otherwise.

    In effect, the ``key()``    function::

        resource["Tags"].key("Name")["Value"]

    is somewhat like::

        resource["Tags"].filter(x, x["Key"] == "Name")[0]

    But the ``key()`` function doesn't raise an exception if the key is not found,
    instead it returns None.

    We might want to generalize this into a ``first()`` reduction macro.
    ``resource["Tags"].first(x, x["Key"] == "Name" ? x["Value"] : null, null)``
    This macro returns the first non-null value or the default (which can be ``null``.)
    """
    key = celtypes.StringType("Key")
    value = celtypes.StringType("Value")
    matches: Iterator[celtypes.Value] = (
        item
        for item in source
        if cast(celtypes.StringType, cast(celtypes.MapType, item).get(key)) == target  # noqa: W503
    )
    try:
        return cast(celtypes.MapType, next(matches)).get(value)
    except StopIteration:
        return None


def glob(text: celtypes.StringType, pattern: celtypes.StringType) -> celtypes.BoolType:
    """Compare a string with a pattern.

    While ``"*.py".glob(some_string)`` seems logical because the pattern the more persistent object,
    this seems to cause confusion.

    We use ``some_string.glob("*.py")`` to express a regex-like rule. This parallels the CEL
    `.matches()` method.

    We also support ``glob(some_string, "*.py")``.
    """
    return celtypes.BoolType(fnmatch.fnmatch(text, pattern))


def difference(left: celtypes.ListType, right: celtypes.ListType) -> celtypes.BoolType:
    """
    Compute the difference between two lists. This is ordered set difference: left - right.
    It's true if the result is non-empty: there is an item in the left, not present in the right.
    It's false if the result is empty: the lists are the same.
    """
    return celtypes.BoolType(bool(set(left) - set(right)))


def intersect(left: celtypes.ListType, right: celtypes.ListType) -> celtypes.BoolType:
    """
    Compute the intersection between two lists.
    It's true if the result is non-empty: there is an item in both lists.
    It's false if the result is empty: there is no common item between the lists.
    """
    return celtypes.BoolType(bool(set(left) & set(right)))


def normalize(string: celtypes.StringType) -> celtypes.StringType:
    """
    Normalize a string.
    """
    return celtypes.StringType(string.lower().strip())


def unique_size(collection: celtypes.ListType) -> celtypes.IntType:
    """
    Unique size of a list
    """
    return celtypes.IntType(len(set(collection)))


class IPv4Network(ipaddress.IPv4Network):
    # Override for net 2 net containment comparison
    def __contains__(self, other):  # type: ignore[no-untyped-def]
        if other is None:
            return False
        if isinstance(other, ipaddress._BaseNetwork):
            return self.supernet_of(other)  # type: ignore[no-untyped-call]
        return super(IPv4Network, self).__contains__(other)

    if sys.version_info.major == 3 and sys.version_info.minor <= 6:  # pragma: no cover

        @staticmethod
        def _is_subnet_of(a, b):  # type: ignore[no-untyped-def]
            try:
                # Always false if one is v4 and the other is v6.
                if a._version != b._version:
                    raise TypeError(f"{a} and {b} are not of the same version")
                return (
                    b.network_address <= a.network_address
                    and b.broadcast_address >= a.broadcast_address  # noqa: W503
                )
            except AttributeError:
                raise TypeError(
                    f"Unable to test subnet containment between {a} and {b}"
                )

        def supernet_of(self, other):  # type: ignore[no-untyped-def]
            """Return True if this network is a supernet of other."""
            return self._is_subnet_of(other, self)  # type: ignore[no-untyped-call]


CIDR = Union[None, IPv4Network, ipaddress.IPv4Address]
CIDR_Class = Union[Type[IPv4Network], Callable[..., ipaddress.IPv4Address]]


def parse_cidr(value: str) -> CIDR:
    """
    Process cidr ranges.

    This is a union of types outside CEL.

    It appears to be Union[None, IPv4Network, ipaddress.IPv4Address]
    """
    klass: CIDR_Class = IPv4Network
    if "/" not in value:
        klass = ipaddress.ip_address  # type: ignore[assignment]
    v: CIDR
    try:
        v = klass(value)
    except (ipaddress.AddressValueError, ValueError):
        v = None
    return v


def size_parse_cidr(
    value: celtypes.StringType,
) -> Optional[celtypes.IntType]:
    """CIDR prefixlen value"""
    cidr = parse_cidr(value)
    if cidr and isinstance(cidr, IPv4Network):
        return celtypes.IntType(cidr.prefixlen)
    else:
        return None


class ComparableVersion(Version):
    """
    The old LooseVersion could fail on comparing present strings, used
    in the value as shorthand for certain options.

    The new Version doesn't fail as easily.
    """

    def __eq__(self, other: object) -> bool:
        try:
            return super(ComparableVersion, self).__eq__(other)
        except TypeError:  # pragma: no cover
            return False


def version(
    value: celtypes.StringType,
) -> celtypes.Value:  # actually, a ComparableVersion
    return cast(celtypes.Value, ComparableVersion(value))


def present(
    value: celtypes.StringType,
) -> celtypes.Value:
    return cast(celtypes.Value, bool(value))


def absent(
    value: celtypes.StringType,
) -> celtypes.Value:
    return cast(celtypes.Value, not bool(value))


def text_from(
    url: celtypes.StringType,
) -> celtypes.Value:
    """
    Read raw text from a URL. This can be expanded to accept S3 or other URL's.
    """
    req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
    raw_data: str
    with closing(urllib.request.urlopen(req)) as response:
        if response.info().get("Content-Encoding") == "gzip":
            raw_data = zlib.decompress(response.read(), zlib.MAX_WBITS | 32).decode(
                "utf8"
            )
        else:
            raw_data = response.read().decode("utf-8")
    return celtypes.StringType(raw_data)


def parse_text(
    source_text: celtypes.StringType, format: celtypes.StringType
) -> celtypes.Value:
    """
    Parse raw text using a given format.
    """
    if format == "json":
        return json_to_cel(json.loads(source_text))
    elif format == "txt":
        return celtypes.ListType(
            [celtypes.StringType(s.rstrip()) for s in source_text.splitlines()]
        )
    elif format in ("ldjson", "ndjson", "jsonl"):
        return celtypes.ListType(
            [json_to_cel(json.loads(s)) for s in source_text.splitlines()]
        )
    elif format == "csv":
        return celtypes.ListType(
            [json_to_cel(row) for row in csv.reader(io.StringIO(source_text))]
        )
    elif format == "csv2dict":
        return celtypes.ListType(
            [json_to_cel(row) for row in csv.DictReader(io.StringIO(source_text))]
        )
    else:
        raise ValueError(f"Unsupported format: {format!r}")  # pragma: no cover


def value_from(
    url: celtypes.StringType,
    format: Optional[celtypes.StringType] = None,
) -> celtypes.Value:
    """
    Read values from a URL.

    First, do :func:`text_from` to read the source.
    Then, do :func:`parse_text` to parse the source, if needed.

    This makes the format optional, and deduces it from the URL's path information.

    C7N will generally replace this with a function
    that leverages a more sophisticated :class:`c7n.resolver.ValuesFrom`.
    """
    supported_formats = ("json", "ndjson", "ldjson", "jsonl", "txt", "csv", "csv2dict")

    # 1. get format either from arg or URL
    if not format:
        _, suffix = os.path.splitext(url)
        format = celtypes.StringType(suffix[1:])
    if format not in supported_formats:
        raise ValueError(f"Unsupported format: {format!r}")

    # 2. read raw data
    # Note this is directly bound to text_from() and does not go though the environment
    # or other CEL indirection.
    raw_data = cast(celtypes.StringType, text_from(url))

    # 3. parse physical format (json, ldjson, ndjson, jsonl, txt, csv, csv2dict)
    return parse_text(raw_data, format)


def jmes_path(
    source_data: celtypes.Value, path_source: celtypes.StringType
) -> celtypes.Value:
    """
    Apply JMESPath to an object read from from a URL.
    """
    expression = jmespath.compile(path_source)
    return json_to_cel(expression.search(source_data))


def jmes_path_map(
    source_data: celtypes.ListType, path_source: celtypes.StringType
) -> celtypes.ListType:
    """
    Apply JMESPath to a each object read from from a URL.
    This is for ndjson, nljson and jsonl files.
    """
    expression = jmespath.compile(path_source)
    return celtypes.ListType(
        [json_to_cel(expression.search(row)) for row in source_data]
    )


def marked_key(
    source: celtypes.ListType, target: celtypes.StringType
) -> celtypes.Value:
    """
    Examines a list of {"Key": text, "Value": text} mappings
    looking for the given Key value.

    Parses a ``message:action@action_date`` value into a mapping
    {"message": message, "action": action, "action_date": action_date}

    If no Key or no Value or the Value isn't the right structure,
    the result is a null.
    """
    value = key(source, target)
    if value is None:
        return None
    try:
        msg, tgt = cast(celtypes.StringType, value).rsplit(":", 1)
        action, action_date_str = tgt.strip().split("@", 1)
    except ValueError:
        return None
    return celtypes.MapType(
        {
            celtypes.StringType("message"): celtypes.StringType(msg),
            celtypes.StringType("action"): celtypes.StringType(action),
            celtypes.StringType("action_date"): celtypes.TimestampType(action_date_str),
        }
    )


def image(resource: celtypes.MapType) -> celtypes.Value:
    """
    Reach into C7N to get the image details for this EC2 or ASG resource.

    Minimally, the creation date is transformed into a CEL timestamp.
    We may want to slightly generalize this to json_to_cell() the entire Image object.

    The following may be usable, but it seems too complex:

    ::

        C7N.filter.prefetch_instance_images(C7N.policy.resources)
        image = C7N.filter.get_instance_image(resource["ImageId"])
        return json_to_cel(image)

    ..  todo:: Refactor C7N

        Provide the :py:class:`InstanceImageBase` mixin in a :py:class:`CELFilter` class.
        We want to have the image details in the new :py:class:`CELFilter` instance.
    """

    # Assuming the :py:class:`CELFilter` class has this method extracted from the legacy filter.
    # Requies the policy already did this: C7N.filter.prefetch_instance_images([resource]) to
    # populate cache.
    image = C7N.filter.get_instance_image(resource)

    if image:
        creation_date = image["CreationDate"]
        image_name = image["Name"]
    else:
        creation_date = "2000-01-01T01:01:01.000Z"
        image_name = ""

    return json_to_cel(
        # {"CreationDate": dateutil.parser.isoparse(creation_date), "Name": image_name}
        {"CreationDate": parse_date(creation_date), "Name": image_name}
    )


def get_raw_metrics(request: celtypes.MapType) -> celtypes.Value:
    """
    Reach into C7N and make a statistics request using the current C7N filter object.

    The ``request`` parameter is the request object that is passed through to AWS via
    the current C7N filter's manager. The request is a Mapping with the following keys and values:

    ::

        get_raw_metrics({
            "Namespace": "AWS/EC2",
            "MetricName": "CPUUtilization",
            "Dimensions": {"Name": "InstanceId", "Value": resource.InstanceId},
            "Statistics": ["Average"],
            "StartTime": now - duration("4d"),
            "EndTime": now,
            "Period": duration("86400s")
        })

    The request is passed through to AWS more-or-less directly. The result is a CEL
    list of values for then requested statistic. A ``.map()`` macro
    can be used to compute additional details. An ``.exists()`` macro can filter the
    data to look for actionable values.

    We would prefer to refactor C7N and implement this with code something like this:

    ::

        C7N.filter.prepare_query(C7N.policy.resources)
        data = C7N.filter.get_resource_statistics(client, resource)
        return json_to_cel(data)

    ..  todo:: Refactor C7N

        Provide a :py:class:`MetricsAccess` mixin in a :py:class:`CELFilter` class.
        We want to have the metrics processing in the new :py:class:`CELFilter` instance.

    """
    client = C7N.filter.manager.session_factory().client("cloudwatch")
    data = client.get_metric_statistics(
        Namespace=request["Namespace"],
        MetricName=request["MetricName"],
        Statistics=request["Statistics"],
        StartTime=request["StartTime"],
        EndTime=request["EndTime"],
        Period=request["Period"],
        Dimensions=request["Dimensions"],
    )["Datapoints"]

    return json_to_cel(data)


def get_metrics(
    resource: celtypes.MapType, request: celtypes.MapType
) -> celtypes.Value:
    """
    Reach into C7N and make a statistics request using the current C7N filter.

    This builds a request object that is passed through to AWS via the :func:`get_raw_metrics`
    function.

    The ``request`` parameter is a Mapping with the following keys and values:

    ::

        resource.get_metrics({"MetricName": "CPUUtilization", "Statistic": "Average",
            "StartTime": now - duration("4d"), "EndTime": now, "Period": duration("86400s")}
            ).exists(m, m < 30)

    The namespace is derived from the ``C7N.policy``. The dimensions are derived from
    the ``C7N.fiter.model``.

    ..  todo:: Refactor C7N

        Provide a :py:class:`MetricsAccess` mixin in a :py:class:`CELFilter` class.
        We want to have the metrics processing in the new :py:class:`CELFilter` instance.

    """
    dimension = C7N.filter.manager.get_model().dimension
    namespace = C7N.filter.manager.resource_type
    # TODO: Varies by resource/policy type. Each policy's model may have different dimensions.
    dimensions = [{"Name": dimension, "Value": resource.get(dimension)}]
    raw_metrics = get_raw_metrics(
        cast(
            celtypes.MapType,
            json_to_cel(
                {
                    "Namespace": namespace,
                    "MetricName": request["MetricName"],
                    "Dimensions": dimensions,
                    "Statistics": [request["Statistic"]],
                    "StartTime": request["StartTime"],
                    "EndTime": request["EndTime"],
                    "Period": request["Period"],
                }
            ),
        )
    )
    return json_to_cel(
        [
            cast(Dict[str, celtypes.Value], item).get(request["Statistic"])
            for item in cast(List[celtypes.Value], raw_metrics)
        ]
    )


def get_raw_health_events(request: celtypes.MapType) -> celtypes.Value:
    """
    Reach into C7N and make a health-events request using the current C7N filter.

    The ``request`` parameter is the filter object that is passed through to AWS via
    the current C7N filter's manager. The request is a List of AWS health events.

    ::

        get_raw_health_events({
            "services": ["ELASTICFILESYSTEM"],
            "regions": ["us-east-1", "global"],
            "eventStatusCodes": ['open', 'upcoming'],
        })
    """
    client = C7N.filter.manager.session_factory().client(
        "health", region_name="us-east-1"
    )
    data = client.describe_events(filter=request)["events"]
    return json_to_cel(data)


def get_health_events(
    resource: celtypes.MapType, statuses: Optional[List[celtypes.Value]] = None
) -> celtypes.Value:
    """
    Reach into C7N and make a health-event request using the current C7N filter.

    This builds a request object that is passed through to AWS via the :func:`get_raw_health_events`
    function.

    ..  todo:: Handle optional list of event types.
    """
    if not statuses:
        statuses = [celtypes.StringType("open"), celtypes.StringType("upcoming")]
    phd_svc_name_map = {
        "app-elb": "ELASTICLOADBALANCING",
        "ebs": "EBS",
        "efs": "ELASTICFILESYSTEM",
        "elb": "ELASTICLOADBALANCING",
        "emr": "ELASTICMAPREDUCE",
    }
    m = C7N.filter.manager
    service = phd_svc_name_map.get(m.data["resource"], m.get_model().service.upper())
    raw_events = get_raw_health_events(
        cast(
            celtypes.MapType,
            json_to_cel(
                {
                    "services": [service],
                    "regions": [m.config.region, "global"],
                    "eventStatusCodes": statuses,
                }
            ),
        )
    )
    return raw_events


def get_related_ids(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N and make a get_related_ids() request using the current C7N filter.

    ..  todo:: Refactor C7N

        Provide the :py:class:`RelatedResourceFilter` mixin in a :py:class:`CELFilter` class.
        We want to have the related id's details in the new :py:class:`CELFilter` instance.
    """

    # Assuming the :py:class:`CELFilter` class has this method extracted from the legacy filter.
    related_ids = C7N.filter.get_related_ids(resource)
    return json_to_cel(related_ids)


def get_related_sgs(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N and make a get_related_sgs() request using the current C7N filter.
    """
    security_groups = C7N.filter.get_related_sgs(resource)
    return json_to_cel(security_groups)


def get_related_subnets(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N and make a get_related_subnets() request using the current C7N filter.
    """
    subnets = C7N.filter.get_related_subnets(resource)
    return json_to_cel(subnets)


def get_related_nat_gateways(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N and make a get_related_nat_gateways() request using the current C7N filter.
    """
    nat_gateways = C7N.filter.get_related_nat_gateways(resource)
    return json_to_cel(nat_gateways)


def get_related_igws(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N and make a get_related_igws() request using the current C7N filter.
    """
    igws = C7N.filter.get_related_igws(resource)
    return json_to_cel(igws)


def get_related_security_configs(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N and make a get_related_security_configs() request using the current C7N filter.
    """
    security_configs = C7N.filter.get_related_security_configs(resource)
    return json_to_cel(security_configs)


def get_related_vpc(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N and make a get_related_vpc() request using the current C7N filter.
    """
    vpc = C7N.filter.get_related_vpc(resource)
    return json_to_cel(vpc)


def get_related_kms_keys(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N and make a get_related_kms_keys() request using the current C7N filter.
    """
    vpc = C7N.filter.get_related_kms_keys(resource)
    return json_to_cel(vpc)


def security_group(
    security_group_id: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N and make a get_related() request using the current C7N filter to get
    the security group.

    ..  todo:: Refactor C7N

        Provide the :py:class:`RelatedResourceFilter` mixin in a :py:class:`CELFilter` class.
        We want to have the related id's details in the new :py:class:`CELFilter` instance.
        See :py:class:`VpcSecurityGroupFilter` subclass of :py:class:`RelatedResourceFilter`.
    """

    # Assuming the :py:class:`CELFilter` class has this method extracted from the legacy filter.
    security_groups = C7N.filter.get_related([security_group_id])
    return json_to_cel(security_groups)


def subnet(
    subnet_id: celtypes.Value,
) -> celtypes.Value:
    """
    Reach into C7N and make a get_related() request using the current C7N filter to get
    the subnet.

    ..  todo:: Refactor C7N

        Provide the :py:class:`RelatedResourceFilter` mixin in a :py:class:`CELFilter` class.
        We want to have the related id's details in the new :py:class:`CELFilter` instance.
        See :py:class:`VpcSubnetFilter` subclass of :py:class:`RelatedResourceFilter`.
    """
    # Get related ID's first, then get items for the related ID's.
    subnets = C7N.filter.get_related([subnet_id])
    return json_to_cel(subnets)


def flow_logs(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N and locate the flow logs using the current C7N filter.

    ..  todo:: Refactor C7N

        Provide a separate function to get the flow logs, separate from the
        the filter processing.

    ..  todo:: Refactor :func:`c7nlib.flow_logs` -- it exposes too much implementation detail.

    """
    # TODO: Refactor into a function in ``CELFilter``. Should not be here.
    client = C7N.filter.manager.session_factory().client("ec2")
    logs = client.describe_flow_logs().get("FlowLogs", ())
    m = C7N.filter.manager.get_model()
    resource_map: Dict[str, List[Dict[str, Any]]] = {}
    for fl in logs:
        resource_map.setdefault(fl["ResourceId"], []).append(fl)
    if resource.get(m.id) in resource_map:
        flogs = resource_map[cast(str, resource.get(m.id))]
        return json_to_cel(flogs)
    return json_to_cel([])


def vpc(
    vpc_id: celtypes.Value,
) -> celtypes.Value:
    """
    Reach into C7N and make a ``get_related()`` request using the current C7N filter to get
    the VPC details.

    ..  todo:: Refactor C7N

        Provide the :py:class:`RelatedResourceFilter` mixin in a :py:class:`CELFilter` class.
        We want to have the related id's details in the new :py:class:`CELFilter` instance.
        See :py:class:`VpcFilter` subclass of :py:class:`RelatedResourceFilter`.
    """
    # Assuming the :py:class:`CELFilter` class has this method extracted from the legacy filter.
    vpc = C7N.filter.get_related([vpc_id])
    return json_to_cel(vpc)


def subst(
    jmes_path: celtypes.StringType,
) -> celtypes.StringType:
    """
    Reach into C7N and build a set of substitutions to replace text in a JMES path.

    This is based on how :py:class:`c7n.resolver.ValuesFrom` works. There are
    two possible substitution values:

    -   account_id
    -   region

    :param jmes_path: the source
    :return: A JMES with values replaced.
    """

    config_args = {
        "account_id": C7N.filter.manager.config.account_id,
        "region": C7N.filter.manager.config.region,
    }
    return celtypes.StringType(jmes_path.format(**config_args))


def credentials(resource: celtypes.MapType) -> celtypes.Value:
    """
    Reach into C7N and make a get_related() request using the current C7N filter to get
    the IAM-role credential details.

    See :py:class:`c7n.resources.iam.CredentialReport` filter.
    The `get_credential_report()` function does what we need.

    ..  todo:: Refactor C7N

        Refactor the :py:class:`c7n.resources.iam.CredentialReport` filter into a
        ``CredentialReportMixin`` mixin to the :py:class:`CELFilter` class.
        The ``get_credential_report()`` function does what we need.
    """
    return json_to_cel(C7N.filter.get_credential_report(resource))


def kms_alias(
    vpc_id: celtypes.Value,
) -> celtypes.Value:
    """
    Reach into C7N and make a get_matching_aliases() request using the current C7N filter to get
    the alias.

    See :py:class:`c7n.resources.kms.ResourceKmsKeyAlias`.
    The `get_matching_aliases()` function does what we need.

    ..  todo:: Refactor C7N

        Refactor the :py:class:`c7n.resources.kms.ResourceKmsKeyAlias` filter into a
        ``ResourceKmsKeyAliasMixin`` mixin to the :py:class:`CELFilter` class.
        The ``get_matching_aliases()`` dfunction does what we need.
    """
    return json_to_cel(C7N.filter.get_matching_aliases())


def kms_key(
    key_id: celtypes.Value,
) -> celtypes.Value:
    """
    Reach into C7N and make a ``get_related()`` request using the current C7N filter to get
    the key. We're looking for the c7n.resources.kms.Key resource manager to get the related key.

    ..  todo:: Refactor C7N

        Provide the :py:class:`RelatedResourceFilter` mixin in a :py:class:`CELFilter` class.
    """
    key = C7N.filter.get_related([key_id])
    return json_to_cel(key)


def resource_schedule(
    tag_value: celtypes.Value,
) -> celtypes.Value:
    """
    Reach into C7N and use the the :py:class:`c7n.filters.offhours.ScheduleParser` class
    to examine the tag's value, the current time, and return a True/False.
    This parser is the `parser` value of the :py:class:`c7n.filters.offhours.Time` filter class.
    Using the filter's `parser.parse(value)` provides needed structure.

    The `filter.parser.parse(value)` will transform text of the Tag value
    into a dictionary. This is further transformed to something we can use in CEL.

    From this
    ::

        off=[(M-F,21),(U,18)];on=[(M-F,6),(U,10)];tz=pt

    C7N ScheduleParser produces this
    ::

        {
          off: [
            { days: [1, 2, 3, 4, 5], hour: 21 },
            { days: [0], hour: 18 }
          ],
          on: [
            { days: [1, 2, 3, 4, 5], hour: 6 },
            { days: [0], hour: 10 }
          ],
          tz: "pt"
        }

    For CEL, we need this
    ::

        {
          off: [
            { days: [1, 2, 3, 4, 5], hour: 21, tz: "pt" },
            { days: [0], hour: 18, tz: "pt" }
          ],
          on: [
            { days: [1, 2, 3, 4, 5], hour: 6, tz: "pt" },
            { days: [0], hour: 10, tz: "pt" }
          ],
        }

    This lets a CEL expression use
    ::

        key("maid_offhours").resource_schedule().off.exists(s,
            now.getDayOfWeek(s.tz) in s.days && now.getHour(s.tz) == s.hour)
    """
    c7n_sched_doc = C7N.filter.parser.parse(tag_value)
    tz = c7n_sched_doc.pop("tz", "et")
    cel_sched_doc = {
        state: [
            {"days": time["days"], "hour": time["hour"], "tz": tz} for time in time_list
        ]
        for state, time_list in c7n_sched_doc.items()
    }
    return json_to_cel(cel_sched_doc)


def get_accounts(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N filter and get accounts for a given resource.
    Used by resources like AMI's, log-groups, ebs-snapshot, etc.

    ..  todo:: Refactor C7N

        Provide the :py:class:`c7n.filters.iamaccessfilter.CrossAccountAccessFilter`
        as a mixin to ``CELFilter``.
    """
    return json_to_cel(C7N.filter.get_accounts())


def get_vpcs(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N filter and get vpcs for a given resource.
    Used by resources like AMI's, log-groups, ebs-snapshot, etc.

    ..  todo:: Refactor C7N

        Provide the :py:class:`c7n.filters.iamaccessfilter.CrossAccountAccessFilter`
        as a mixin to ``CELFilter``.
    """
    return json_to_cel(C7N.filter.get_vpcs())


def get_vpces(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N filter and get vpces for a given resource.
    Used by resources like AMI's, log-groups, ebs-snapshot, etc.

    ..  todo:: Refactor C7N

        Provide the :py:class:`c7n.filters.iamaccessfilter.CrossAccountAccessFilter`
        as a mixin to ``CELFilter``.

    """
    return json_to_cel(C7N.filter.get_vpces())


def get_orgids(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N filter and get orgids for a given resource.
    Used by resources like AMI's, log-groups, ebs-snapshot, etc.

    ..  todo:: Refactor C7N

        Provide the :py:class:`c7n.filters.iamaccessfilter.CrossAccountAccessFilter`
        as a mixin to ``CELFilter``.
    """
    return json_to_cel(C7N.filter.get_orgids())


def get_endpoints(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """For sns resources

    ..  todo:: Refactor C7N

        Provide the :py:class:`c7n.filters.iamaccessfilter.CrossAccountAccessFilter`
        as a mixin to ``CELFilter``.
    """
    return json_to_cel(C7N.filter.get_endpoints())


def get_protocols(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """For sns resources

    ..  todo:: Refactor C7N
    """
    return json_to_cel(C7N.filter.get_protocols())


def get_key_policy(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """For kms resources

    ..  todo:: Refactor C7N
    """
    key_id = resource.get(
        celtypes.StringType("TargetKeyId"), resource.get(celtypes.StringType("KeyId"))
    )
    client = C7N.filter.manager.session_factory().client("kms")
    return json_to_cel(
        client.get_key_policy(KeyId=key_id, PolicyName="default")["Policy"]
    )


def get_resource_policy(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    Reach into C7N filter and get the resource policy for a given resource.
    Used by resources like AMI's, log-groups, ebs-snapshot, etc.

    ..  todo:: Refactor C7N
    """
    return json_to_cel(C7N.filter.get_resource_policy())


def describe_subscription_filters(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    For log-groups resources.

    ..  todo:: Refactor C7N

        this should be directly available in CELFilter.
    """
    client = C7N.filter.manager.session_factory().client("logs")
    return json_to_cel(
        C7N.filter.manager.retry(
            client.describe_subscription_filters, logGroupName=resource["logGroupName"]
        ).get("subscriptionFilters", ())
    )


def describe_db_snapshot_attributes(
    resource: celtypes.MapType,
) -> celtypes.Value:
    """
    For rds-snapshot and ebs-snapshot resources

    ..  todo:: Refactor C7N

        this should be directly available in CELFilter.
    """
    client = C7N.filter.manager.session_factory().client("ec2")
    return json_to_cel(
        C7N.filter.manager.retry(
            client.describe_snapshot_attribute,
            SnapshotId=resource["SnapshotId"],
            Attribute="createVolumePermission",
        )
    )


def arn_split(arn: celtypes.StringType, field: celtypes.StringType) -> celtypes.Value:
    """
    Parse an ARN, removing a partivular field.
    The field name must one one of
    "partition", "service", "region", "account-id", "resource-type", "resource-id"
    In the case of a ``resource-type/resource-id`` path, this will be a "resource-id" value,
    and there will be no "resource-type".

    Examples formats

        ``arn:partition:service:region:account-id:resource-id``

        ``arn:partition:service:region:account-id:resource-type/resource-id``

        ``arn:partition:service:region:account-id:resource-type:resource-id``
    """
    field_names = {
        len(names): names
        for names in [
            ("partition", "service", "region", "account-id", "resource-id"),
            (
                "partition",
                "service",
                "region",
                "account-id",
                "resource-type",
                "resource-id",
            ),
        ]
    }
    prefix, *fields = arn.split(":")
    if prefix != "arn":
        raise ValueError(f"Not an ARN: {arn}")
    mapping = dict(zip(field_names[len(fields)], fields))
    return json_to_cel(mapping[field])


def all_images() -> celtypes.Value:
    """
    Depends on :py:meth:`CELFilter._pull_ec2_images` and :py:meth:`CELFilter._pull_asg_images`

    See :py:class:`c7n.resources.ami.ImageUnusedFilter`
    """
    return json_to_cel(
        list(C7N.filter._pull_ec2_images() | C7N.filter._pull_asg_images())
    )


def all_snapshots() -> celtypes.Value:
    """
    Depends on :py:meth:`CELFilter._pull_asg_snapshots`
    and :py:meth:`CELFilter._pull_ami_snapshots`

    See :py:class:`c7n.resources.ebs.SnapshotUnusedFilter`
    """
    return json_to_cel(
        list(C7N.filter._pull_asg_snapshots() | C7N.filter._pull_ami_snapshots())
    )


def all_launch_configuration_names() -> celtypes.Value:
    """
    Depends on :py:meth:`CELFilter.manager.get_launch_configuration_names`

    See :py:class:`c7n.resources.asg.UnusedLaunchConfig`
    """
    asgs = C7N.filter.manager.get_resource_manager("asg").resources()
    used = set(
        [
            a.get("LaunchConfigurationName", a["AutoScalingGroupName"])
            for a in asgs
            if not a.get("LaunchTemplate")
        ]
    )
    return json_to_cel(list(used))


def all_service_roles() -> celtypes.Value:
    """
    Depends on :py:meth:`CELFilter.service_role_usage`

    See :py:class:`c7n.resources.iam.UnusedIamRole`
    """
    return json_to_cel(C7N.filter.service_role_usage())


def all_instance_profiles() -> celtypes.Value:
    """
    Depends on :py:meth:`CELFilter.instance_profile_usage`

    See :py:class:`c7n.resources.iam.UnusedInstanceProfiles`
    """
    return json_to_cel(C7N.filter.instance_profile_usage())


def all_dbsubenet_groups() -> celtypes.Value:
    """
    Depends on :py:meth:`CELFilter.get_dbsubnet_group_used`

    See :py:class:`c7n.resources.rds.UnusedRDSSubnetGroup`
    """
    rds = C7N.filter.manager.get_resource_manager("rds").resources()
    used = set([r.get("DBSubnetGroupName", r["DBInstanceIdentifier"]) for r in rds])
    return json_to_cel(list(used))


def all_scan_groups() -> celtypes.Value:
    """
    Depends on :py:meth:`CELFilter.scan_groups`

    See :py:class:`c7n.resources.vpc.UnusedSecurityGroup`
    """
    return json_to_cel(C7N.filter.scan_groups())


def get_access_log(resource: celtypes.MapType) -> celtypes.Value:
    """
    Depends on :py:meth:`CELFilter.resources`

    See :py:class:`c7n.resources.elb.IsNotLoggingFilter` and
    :py:class:`c7n.resources.elb.IsLoggingFilter`.
    """
    client = C7N.filter.manager.session_factory().client("elb")
    results = client.describe_load_balancer_attributes(
        LoadBalancerName=resource["LoadBalancerName"]
    )
    return json_to_cel(results["LoadBalancerAttributes"])


def get_load_balancer(resource: celtypes.MapType) -> celtypes.Value:
    """
    Depends on :py:meth:`CELFilter.resources`

    See :py:class:`c7n.resources.appelb.IsNotLoggingFilter` and
    :py:class:`c7n.resources.appelb.IsLoggingFilter`.
    """

    def parse_attribute_value(v: str) -> Union[int, bool, str]:
        """Lightweight JSON atomic value convertion to native Python."""
        if v.isdigit():
            return int(v)
        elif v == "true":
            return True
        elif v == "false":
            return False
        return v

    client = C7N.filter.manager.session_factory().client("elbv2")
    results = client.describe_load_balancer_attributes(
        LoadBalancerArn=resource["LoadBalancerArn"]
    )
    print(results)
    return json_to_cel(
        dict(
            (item["Key"], parse_attribute_value(item["Value"]))
            for item in results["Attributes"]
        )
    )


def shield_protection(resource: celtypes.MapType) -> celtypes.Value:
    """
    Depends on the :py:meth:`c7n.resources.shield.IsShieldProtected.process` method.
    This needs to be refactored and renamed to avoid collisions with other ``process()`` variants.

    Applies to most resource types.
    """
    client = C7N.filter.manager.session_factory().client(
        "shield", region_name="us-east-1"
    )
    protections = C7N.filter.get_type_protections(
        client, C7N.filter.manager.get_model()
    )
    protected_resources = [p["ResourceArn"] for p in protections]
    return json_to_cel(protected_resources)


def shield_subscription(resource: celtypes.MapType) -> celtypes.Value:
    """
    Depends on :py:meth:`c7n.resources.account.ShieldEnabled.process` method.
    This needs to be refactored and renamed to avoid collisions with other ``process()`` variants.

    Applies to account resources only.
    """
    subscriptions = C7N.filter.account_shield_subscriptions(resource)
    return json_to_cel(subscriptions)


def web_acls(resource: celtypes.MapType) -> celtypes.Value:
    """
    Depends on :py:meth:`c7n.resources.cloudfront.IsWafEnabled.process` method.
    This needs to be refactored and renamed to avoid collisions with other ``process()`` variants.
    """
    wafs = C7N.filter.manager.get_resource_manager("waf").resources()
    waf_name_id_map = {w["Name"]: w["WebACLId"] for w in wafs}
    return json_to_cel(waf_name_id_map)


DECLARATIONS: Dict[str, Annotation] = {
    "glob": celtypes.FunctionType,
    "difference": celtypes.FunctionType,
    "intersect": celtypes.FunctionType,
    "normalize": celtypes.FunctionType,
    "parse_cidr": celtypes.FunctionType,  # Callable[..., CIDR],
    "size_parse_cidr": celtypes.FunctionType,
    "unique_size": celtypes.FunctionType,
    "version": celtypes.FunctionType,  # Callable[..., ComparableVersion],
    "present": celtypes.FunctionType,
    "absent": celtypes.FunctionType,
    "text_from": celtypes.FunctionType,
    "value_from": celtypes.FunctionType,
    "jmes_path": celtypes.FunctionType,
    "jmes_path_map": celtypes.FunctionType,
    "key": celtypes.FunctionType,
    "marked_key": celtypes.FunctionType,
    "image": celtypes.FunctionType,
    "get_metrics": celtypes.FunctionType,
    "get_related_ids": celtypes.FunctionType,
    "security_group": celtypes.FunctionType,
    "subnet": celtypes.FunctionType,
    "flow_logs": celtypes.FunctionType,
    "vpc": celtypes.FunctionType,
    "subst": celtypes.FunctionType,
    "credentials": celtypes.FunctionType,
    "kms_alias": celtypes.FunctionType,
    "kms_key": celtypes.FunctionType,
    "resource_schedule": celtypes.FunctionType,
    "get_accounts": celtypes.FunctionType,
    "get_related_sgs": celtypes.FunctionType,
    "get_related_subnets": celtypes.FunctionType,
    "get_related_nat_gateways": celtypes.FunctionType,
    "get_related_igws": celtypes.FunctionType,
    "get_related_security_configs": celtypes.FunctionType,
    "get_related_vpc": celtypes.FunctionType,
    "get_related_kms_keys": celtypes.FunctionType,
    "get_vpcs": celtypes.FunctionType,
    "get_vpces": celtypes.FunctionType,
    "get_orgids": celtypes.FunctionType,
    "get_endpoints": celtypes.FunctionType,
    "get_protocols": celtypes.FunctionType,
    "get_key_policy": celtypes.FunctionType,
    "get_resource_policy": celtypes.FunctionType,
    "describe_subscription_filters": celtypes.FunctionType,
    "describe_db_snapshot_attributes": celtypes.FunctionType,
    "arn_split": celtypes.FunctionType,
    "all_images": celtypes.FunctionType,
    "all_snapshots": celtypes.FunctionType,
    "all_launch_configuration_names": celtypes.FunctionType,
    "all_service_roles": celtypes.FunctionType,
    "all_instance_profiles": celtypes.FunctionType,
    "all_dbsubenet_groups": celtypes.FunctionType,
    "all_scan_groups": celtypes.FunctionType,
    "get_access_log": celtypes.FunctionType,
    "get_load_balancer": celtypes.FunctionType,
    "shield_protection": celtypes.FunctionType,
    "shield_subscription": celtypes.FunctionType,
    "web_acls": celtypes.FunctionType,
    # "etc.": celtypes.FunctionType,
}

ExtFunction = Callable[..., celtypes.Value]

FUNCTIONS: Dict[str, ExtFunction] = {
    f.__name__: cast(ExtFunction, f)
    for f in [
        glob,
        difference,
        intersect,
        normalize,
        parse_cidr,
        size_parse_cidr,
        unique_size,
        version,
        present,
        absent,
        text_from,
        value_from,
        jmes_path,
        jmes_path_map,
        key,
        marked_key,
        image,
        get_metrics,
        get_related_ids,
        security_group,
        subnet,
        flow_logs,
        vpc,
        subst,
        credentials,
        kms_alias,
        kms_key,
        resource_schedule,
        get_accounts,
        get_related_sgs,
        get_related_subnets,
        get_related_nat_gateways,
        get_related_igws,
        get_related_security_configs,
        get_related_vpc,
        get_related_kms_keys,
        get_vpcs,
        get_vpces,
        get_orgids,
        get_endpoints,
        get_protocols,
        get_key_policy,
        get_resource_policy,
        describe_subscription_filters,
        describe_db_snapshot_attributes,
        arn_split,
        all_images,
        all_snapshots,
        all_launch_configuration_names,
        all_service_roles,
        all_instance_profiles,
        all_dbsubenet_groups,
        all_scan_groups,
        get_access_log,
        get_load_balancer,
        shield_protection,
        shield_subscription,
        web_acls,
        # etc.
    ]
}


class C7N_Interpreted_Runner(InterpretedRunner):
    """
    Extends the Evaluation to introduce the C7N CELFilter instance into the exvaluation.

    The variable is global to allow the functions to have the simple-looking argument
    values that CEL expects. This allows a function in this module to reach outside CEL for
    access to C7N's caches.

    ..  todo: Refactor to be a mixin to the Runner class hierarchy.
    """

    def evaluate(
        self, context: Context, filter: Optional[Any] = None
    ) -> celtypes.Value:
        e = Evaluator(
            ast=self.ast,
            activation=self.new_activation(context),
            functions=self.functions,
        )
        with C7NContext(filter=filter):
            value = e.evaluate()
        return value
