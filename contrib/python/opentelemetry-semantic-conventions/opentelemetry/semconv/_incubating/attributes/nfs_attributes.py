# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

NFS_OPERATION_NAME: Final = "nfs.operation.name"
"""
NFSv4+ operation name.
"""

NFS_SERVER_REPCACHE_STATUS: Final = "nfs.server.repcache.status"
"""
Linux: one of "hit" (NFSD_STATS_RC_HITS), "miss" (NFSD_STATS_RC_MISSES), or "nocache" (NFSD_STATS_RC_NOCACHE -- uncacheable).
"""
