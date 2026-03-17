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

__all__ = ["Provider", "ContainerState"]


class Type:
    @classmethod
    def tostring(cls, value):
        """Return the string representation of the state object attribute
        :param str value: the state object to turn into string
        :return: the uppercase string that represents the state object
        :rtype: str
        """
        return value.upper()

    @classmethod
    def fromstring(cls, value):
        """Return the state object attribute that matches the string
        :param str value: the string to look up
        :return: the state object attribute that matches the string
        :rtype: str
        """
        return getattr(cls, value.upper(), None)


class Provider:
    """
    Defines for each of the supported providers

    Non-Dummy drivers are sorted in alphabetical order. Please preserve this
    ordering when adding new drivers.
    """

    DUMMY = "dummy"
    DOCKER = "docker"
    ECS = "ecs"
    GKE = "GKE"
    KUBERNETES = "kubernetes"
    LXD = "lxd"
    RANCHER = "rancher"


class ContainerState(Type):
    """
    Standard states for a container

    :cvar RUNNING: Container is running.
    :cvar REBOOTING: Container is rebooting.
    :cvar TERMINATED: Container is terminated.
                This container can't be started later on.
    :cvar STOPPED: Container is stopped.
                This container can be started later on.
    :cvar PENDING: Container is pending.
    :cvar SUSPENDED: Container is suspended.
    :cvar ERROR: Container is an error state.
                Usually no operations can be performed
                on the container once it ends up in the error state.
    :cvar PAUSED: Container is paused.
    :cvar UNKNOWN: Container state is unknown.
    """

    RUNNING = "running"
    REBOOTING = "rebooting"
    TERMINATED = "terminated"
    PENDING = "pending"
    UNKNOWN = "unknown"
    STOPPED = "stopped"
    SUSPENDED = "suspended"
    ERROR = "error"
    PAUSED = "paused"
