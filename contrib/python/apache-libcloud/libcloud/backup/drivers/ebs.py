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

__all__ = ["EBSBackupDriver"]


from libcloud.utils.xml import findall, findtext
from libcloud.common.aws import AWSGenericResponse, SignedAWSConnection
from libcloud.backup.base import (
    BackupDriver,
    BackupTarget,
    BackupTargetJob,
    BackupTargetRecoveryPoint,
)
from libcloud.backup.types import BackupTargetType, BackupTargetJobStatusType
from libcloud.utils.iso8601 import parse_date

VERSION = "2015-10-01"
HOST = "ec2.amazonaws.com"
ROOT = "/%s/" % (VERSION)
NS = "http://ec2.amazonaws.com/doc/{}/".format(VERSION)


class EBSResponse(AWSGenericResponse):
    """
    Amazon EBS response class.
    """

    namespace = NS
    exceptions = {}
    xpath = "Error"


class EBSConnection(SignedAWSConnection):
    version = VERSION
    host = HOST
    responseCls = EBSResponse
    service_name = "backup"


class EBSBackupDriver(BackupDriver):
    name = "Amazon EBS Backup Driver"
    website = "http://aws.amazon.com/ebs/"
    connectionCls = EBSConnection

    def __init__(self, access_id, secret, region):
        super().__init__(access_id, secret)
        self.region = region
        self.connection.host = HOST % (region)

    def get_supported_target_types(self):
        """
        Get a list of backup target types this driver supports

        :return: ``list`` of :class:``BackupTargetType``
        """
        return [BackupTargetType.VOLUME]

    def list_targets(self):
        """
        List all backuptargets

        :rtype: ``list`` of :class:`BackupTarget`
        """
        raise NotImplementedError("list_targets not implemented for this driver")

    def create_target(self, name, address, type=BackupTargetType.VOLUME, extra=None):
        """
        Creates a new backup target

        :param name: Name of the target
        :type name: ``str``

        :param address: The volume ID.
        :type address: ``str``

        :param type: Backup target type (Physical, Virtual, ...).
        :type type: :class:`BackupTargetType`

        :param extra: (optional) Extra attributes (driver specific).
        :type extra: ``dict``

        :rtype: Instance of :class:`BackupTarget`
        """
        # Does nothing since any volume can be snapped at anytime.
        return self.ex_get_target_by_volume_id(address)

    def create_target_from_node(self, node, type=BackupTargetType.VIRTUAL, extra=None):
        """
        Creates a new backup target from an existing node

        :param node: The Node to backup
        :type  node: ``Node``

        :param type: Backup target type (Physical, Virtual, ...).
        :type type: :class:`BackupTargetType`

        :param extra: (optional) Extra attributes (driver specific).
        :type extra: ``dict``

        :rtype: Instance of :class:`BackupTarget`
        """
        # Get the first EBS volume.
        device_mapping = node.extra["block_device_mapping"]
        if device_mapping is not None:
            return self.create_target(
                name=node.name,
                address=device_mapping["ebs"][0]["volume_id"],
                type=BackupTargetType.VOLUME,
                extra=None,
            )
        else:
            raise RuntimeError("Node does not have any block devices")

    def create_target_from_container(self, container, type=BackupTargetType.OBJECT, extra=None):
        """
        Creates a new backup target from an existing storage container

        :param node: The Container to backup
        :type  node: ``Container``

        :param type: Backup target type (Physical, Virtual, ...).
        :type type: :class:`BackupTargetType`

        :param extra: (optional) Extra attributes (driver specific).
        :type extra: ``dict``

        :rtype: Instance of :class:`BackupTarget`
        """
        raise NotImplementedError("create_target_from_container not implemented for this driver")

    def update_target(self, target, name, address, extra):
        """
        Update the properties of a backup target

        :param target: Backup target to update
        :type  target: Instance of :class:`BackupTarget`

        :param name: Name of the target
        :type name: ``str``

        :param address: Hostname, FQDN, IP, file path etc.
        :type address: ``str``

        :param extra: (optional) Extra attributes (driver specific).
        :type extra: ``dict``

        :rtype: Instance of :class:`BackupTarget`
        """
        # Does nothing since any volume can be snapped at anytime.
        return self.ex_get_target_by_volume_id(address)

    def delete_target(self, target):
        """
        Delete a backup target

        :param target: Backup target to delete
        :type  target: Instance of :class:`BackupTarget`
        """
        raise NotImplementedError("delete_target not implemented for this driver")

    def list_recovery_points(self, target, start_date=None, end_date=None):
        """
        List the recovery points available for a target

        :param target: Backup target to delete
        :type  target: Instance of :class:`BackupTarget`

        :param start_date: The start date to show jobs between (optional)
        :type  start_date: :class:`datetime.datetime`

        :param end_date: The end date to show jobs between (optional)
        :type  end_date: :class:`datetime.datetime``

        :rtype: ``list`` of :class:`BackupTargetRecoveryPoint`
        """
        params = {
            "Action": "DescribeSnapshots",
            "Filter.1.Name": "volume-id",
            "Filter.1.Value": target.extra["volume-id"],
        }
        data = self.connection.request(ROOT, params=params).object
        return self._to_recovery_points(data, target)

    def recover_target(self, target, recovery_point, path=None):
        """
        Recover a backup target to a recovery point

        :param target: Backup target to delete
        :type  target: Instance of :class:`BackupTarget`

        :param recovery_point: Backup target with the backup data
        :type  recovery_point: Instance of :class:`BackupTarget`

        :param path: The part of the recovery point to recover (optional)
        :type  path: ``str``

        :rtype: Instance of :class:`BackupTargetJob`
        """
        raise NotImplementedError("delete_target not implemented for this driver")

    def recover_target_out_of_place(self, target, recovery_point, recovery_target, path=None):
        """
        Recover a backup target to a recovery point out-of-place

        :param target: Backup target with the backup data
        :type  target: Instance of :class:`BackupTarget`

        :param recovery_point: Backup target with the backup data
        :type  recovery_point: Instance of :class:`BackupTarget`

        :param recovery_target: Backup target with to recover the data to
        :type  recovery_target: Instance of :class:`BackupTarget`

        :param path: The part of the recovery point to recover (optional)
        :type  path: ``str``

        :rtype: Instance of :class:`BackupTargetJob`
        """
        raise NotImplementedError("delete_target not implemented for this driver")

    def get_target_job(self, target, id):
        """
        Get a specific backup job by ID

        :param target: Backup target with the backup data
        :type  target: Instance of :class:`BackupTarget`

        :param id: Backup target with the backup data
        :type  id: Instance of :class:`BackupTarget`

        :rtype: :class:`BackupTargetJob`
        """
        jobs = self.list_target_jobs(target)
        return list(filter(lambda x: x.id == id, jobs))[0]

    def list_target_jobs(self, target):
        """
        List the backup jobs on a target

        :param target: Backup target with the backup data
        :type  target: Instance of :class:`BackupTarget`

        :rtype: ``list`` of :class:`BackupTargetJob`
        """
        params = {
            "Action": "DescribeSnapshots",
            "Filter.1.Name": "volume-id",
            "Filter.1.Value": target.extra["volume-id"],
            "Filter.2.Name": "status",
            "Filter.2.Value": "pending",
        }
        data = self.connection.request(ROOT, params=params).object
        return self._to_jobs(data)

    def create_target_job(self, target, extra=None):
        """
        Create a new backup job on a target

        :param target: Backup target with the backup data
        :type  target: Instance of :class:`BackupTarget`

        :param extra: (optional) Extra attributes (driver specific).
        :type extra: ``dict``

        :rtype: Instance of :class:`BackupTargetJob`
        """
        params = {"Action": "CreateSnapshot", "VolumeId": target.extra["volume-id"]}
        data = self.connection.request(ROOT, params=params).object
        xpath = "CreateSnapshotResponse"
        return self._to_job(findall(element=data, xpath=xpath, namespace=NS)[0])

    def resume_target_job(self, job):
        """
        Resume a suspended backup job on a target

        :param job: Backup target job to resume
        :type  job: Instance of :class:`BackupTargetJob`

        :rtype: ``bool``
        """
        raise NotImplementedError("resume_target_job not supported for this driver")

    def suspend_target_job(self, job):
        """
        Suspend a running backup job on a target

        :param job: Backup target job to suspend
        :type  job: Instance of :class:`BackupTargetJob`

        :rtype: ``bool``
        """
        raise NotImplementedError("suspend_target_job not supported for this driver")

    def cancel_target_job(self, job):
        """
        Cancel a backup job on a target

        :param job: Backup target job to cancel
        :type  job: Instance of :class:`BackupTargetJob`

        :rtype: ``bool``
        """
        raise NotImplementedError("cancel_target_job not supported for this driver")

    def _to_recovery_points(self, data, target):
        xpath = "DescribeSnapshotsResponse/snapshotSet/item"
        return [
            self._to_recovery_point(el, target)
            for el in findall(element=data, xpath=xpath, namespace=NS)
        ]

    def _to_recovery_point(self, el, target):
        id = findtext(element=el, xpath="snapshotId", namespace=NS)
        date = parse_date(findtext(element=el, xpath="startTime", namespace=NS))
        tags = self._get_resource_tags(el)
        point = BackupTargetRecoveryPoint(
            id=id,
            date=date,
            target=target,
            driver=self.connection.driver,
            extra={"snapshot-id": id, "tags": tags},
        )
        return point

    def _to_jobs(self, data):
        xpath = "DescribeSnapshotsResponse/snapshotSet/item"
        return [self._to_job(el) for el in findall(element=data, xpath=xpath, namespace=NS)]

    def _to_job(self, el):
        id = findtext(element=el, xpath="snapshotId", namespace=NS)
        progress = findtext(element=el, xpath="progress", namespace=NS).replace("%", "")
        volume_id = findtext(element=el, xpath="volumeId", namespace=NS)
        target = self.ex_get_target_by_volume_id(volume_id)
        job = BackupTargetJob(
            id=id,
            status=BackupTargetJobStatusType.PENDING,
            progress=int(progress),
            target=target,
            driver=self.connection.driver,
            extra={},
        )
        return job

    def ex_get_target_by_volume_id(self, volume_id):
        return BackupTarget(
            id=volume_id,
            name=volume_id,
            address=volume_id,
            type=BackupTargetType.VOLUME,
            driver=self.connection.driver,
            extra={"volume-id": volume_id},
        )

    def _get_resource_tags(self, element):
        """
        Parse tags from the provided element and return a dictionary with
        key/value pairs.

        :rtype: ``dict``
        """
        tags = {}

        # Get our tag set by parsing the element
        tag_set = findall(element=element, xpath="tagSet/item", namespace=NS)

        for tag in tag_set:
            key = findtext(element=tag, xpath="key", namespace=NS)

            value = findtext(element=tag, xpath="value", namespace=NS)

            tags[key] = value

        return tags
