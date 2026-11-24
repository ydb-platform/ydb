# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


from typing import Final

DB_CLIENT_OPERATION_DURATION: Final = "db.client.operation.duration"
"""
Duration of database client operations
Instrument: histogram
Unit: s
Note: Batch operations SHOULD be recorded as a single operation.
"""
