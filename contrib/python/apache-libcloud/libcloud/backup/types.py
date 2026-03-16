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

__all__ = ["Provider", "BackupTargetType", "BackupTargetJobStatusType"]


class Provider:
    """
    Defines for each of the supported providers

    Non-Dummy drivers are sorted in alphabetical order. Please preserve this
    ordering when adding new drivers.
    """

    DUMMY = "dummy"
    DIMENSIONDATA = "dimensiondata"
    EBS = "ebs"
    GCE = "gce"


class BackupTargetType:
    """
    Backup Target type.
    """

    VIRTUAL = "Virtual"
    """ Denotes a virtual host """

    PHYSICAL = "Physical"
    """ Denotes a physical host """

    FILESYSTEM = "Filesystem"
    """ Denotes a file system (e.g. NAS) """

    DATABASE = "Database"
    """ Denotes a database target """

    OBJECT = "Object"
    """ Denotes an object based file system """

    VOLUME = "Volume"
    """ Denotes a block storage volume """


class BackupTargetJobStatusType:
    """
    The status of a backup target job
    """

    RUNNING = "Running"
    CANCELLED = "Cancelled"
    FAILED = "Failed"
    COMPLETED = "Completed"
    PENDING = "Pending"
