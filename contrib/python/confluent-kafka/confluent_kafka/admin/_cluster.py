# Copyright 2022 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from .._util import ConversionUtil
from ._acl import AclOperation


class DescribeClusterResult:
    """
    Represents cluster description information used in describe cluster operation.
    Used by :meth:`AdminClient.describe_cluster`.

    Parameters
    ----------
    controller : Node
        The current controller in the cluster.
    nodes : list(Node)
        Information about each node in the cluster.
    cluster_id : str
        The current cluster id in the cluster.
    authorized_operations: list(AclOperation)
        AclOperations allowed for the cluster.
    """

    def __init__(self, controller, nodes, cluster_id=None, authorized_operations=None):
        self.cluster_id = cluster_id
        self.controller = controller
        self.nodes = nodes
        self.authorized_operations = None
        if authorized_operations:
            self.authorized_operations = []
            for op in authorized_operations:
                self.authorized_operations.append(ConversionUtil.convert_to_enum(op, AclOperation))
