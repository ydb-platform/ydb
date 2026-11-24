# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

CLOUDEVENTS_EVENT_ID: Final = "cloudevents.event_id"
"""
The [event_id](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#id) uniquely identifies the event.
"""

CLOUDEVENTS_EVENT_SOURCE: Final = "cloudevents.event_source"
"""
The [source](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#source-1) identifies the context in which an event happened.
"""

CLOUDEVENTS_EVENT_SPEC_VERSION: Final = "cloudevents.event_spec_version"
"""
The [version of the CloudEvents specification](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#specversion) which the event uses.
"""

CLOUDEVENTS_EVENT_SUBJECT: Final = "cloudevents.event_subject"
"""
The [subject](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#subject) of the event in the context of the event producer (identified by source).
"""

CLOUDEVENTS_EVENT_TYPE: Final = "cloudevents.event_type"
"""
The [event_type](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#type) contains a value describing the type of event related to the originating occurrence.
"""
