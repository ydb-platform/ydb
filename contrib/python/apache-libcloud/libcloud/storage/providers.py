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

from types import ModuleType
from typing import TYPE_CHECKING, Type, Union

from libcloud.storage.types import OLD_CONSTANT_TO_NEW_MAPPING, Provider
from libcloud.common.providers import get_driver as _get_provider_driver
from libcloud.common.providers import set_driver as _set_provider_driver

if TYPE_CHECKING:
    from libcloud.storage.base import StorageDriver

DRIVERS = {
    Provider.DUMMY: ("libcloud.storage.drivers.dummy", "DummyStorageDriver"),
    Provider.CLOUDFILES: (
        "libcloud.storage.drivers.cloudfiles",
        "CloudFilesStorageDriver",
    ),
    Provider.OPENSTACK_SWIFT: (
        "libcloud.storage.drivers.cloudfiles",
        "OpenStackSwiftStorageDriver",
    ),
    Provider.S3: ("libcloud.storage.drivers.s3", "S3StorageDriver"),
    Provider.S3_US_EAST2: ("libcloud.storage.drivers.s3", "S3USEast2StorageDriver"),
    Provider.S3_US_WEST: ("libcloud.storage.drivers.s3", "S3USWestStorageDriver"),
    Provider.S3_US_WEST_OREGON: (
        "libcloud.storage.drivers.s3",
        "S3USWestOregonStorageDriver",
    ),
    Provider.S3_US_GOV_WEST: (
        "libcloud.storage.drivers.s3",
        "S3USGovWestStorageDriver",
    ),
    Provider.S3_CN_NORTH: ("libcloud.storage.drivers.s3", "S3CNNorthStorageDriver"),
    Provider.S3_CN_NORTHWEST: (
        "libcloud.storage.drivers.s3",
        "S3CNNorthWestStorageDriver",
    ),
    Provider.S3_EU_WEST: ("libcloud.storage.drivers.s3", "S3EUWestStorageDriver"),
    Provider.S3_EU_WEST2: ("libcloud.storage.drivers.s3", "S3EUWest2StorageDriver"),
    Provider.S3_EU_CENTRAL: ("libcloud.storage.drivers.s3", "S3EUCentralStorageDriver"),
    Provider.S3_EU_NORTH1: ("libcloud.storage.drivers.s3", "S3EUNorth1StorageDriver"),
    Provider.S3_AP_SOUTH: ("libcloud.storage.drivers.s3", "S3APSouthStorageDriver"),
    Provider.S3_AP_SOUTHEAST: ("libcloud.storage.drivers.s3", "S3APSEStorageDriver"),
    Provider.S3_AP_SOUTHEAST2: ("libcloud.storage.drivers.s3", "S3APSE2StorageDriver"),
    Provider.S3_AP_NORTHEAST: ("libcloud.storage.drivers.s3", "S3APNE1StorageDriver"),
    Provider.S3_AP_NORTHEAST1: ("libcloud.storage.drivers.s3", "S3APNE1StorageDriver"),
    Provider.S3_AP_NORTHEAST2: ("libcloud.storage.drivers.s3", "S3APNE2StorageDriver"),
    Provider.S3_SA_EAST: ("libcloud.storage.drivers.s3", "S3SAEastStorageDriver"),
    Provider.S3_CA_CENTRAL: ("libcloud.storage.drivers.s3", "S3CACentralStorageDriver"),
    Provider.S3_RGW: ("libcloud.storage.drivers.rgw", "S3RGWStorageDriver"),
    Provider.S3_RGW_OUTSCALE: (
        "libcloud.storage.drivers.rgw",
        "S3RGWOutscaleStorageDriver",
    ),
    Provider.NINEFOLD: ("libcloud.storage.drivers.ninefold", "NinefoldStorageDriver"),
    Provider.GOOGLE_STORAGE: (
        "libcloud.storage.drivers.google_storage",
        "GoogleStorageDriver",
    ),
    Provider.NIMBUS: ("libcloud.storage.drivers.nimbus", "NimbusStorageDriver"),
    Provider.LOCAL: ("libcloud.storage.drivers.local", "LocalStorageDriver"),
    Provider.AZURE_BLOBS: (
        "libcloud.storage.drivers.azure_blobs",
        "AzureBlobsStorageDriver",
    ),
    Provider.KTUCLOUD: ("libcloud.storage.drivers.ktucloud", "KTUCloudStorageDriver"),
    Provider.AURORAOBJECTS: (
        "libcloud.storage.drivers.auroraobjects",
        "AuroraObjectsStorageDriver",
    ),
    Provider.BACKBLAZE_B2: (
        "libcloud.storage.drivers.backblaze_b2",
        "BackblazeB2StorageDriver",
    ),
    Provider.ALIYUN_OSS: ("libcloud.storage.drivers.oss", "OSSStorageDriver"),
    Provider.DIGITALOCEAN_SPACES: (
        "libcloud.storage.drivers.digitalocean_spaces",
        "DigitalOceanSpacesStorageDriver",
    ),
    Provider.MINIO: ("libcloud.storage.drivers.minio", "MinIOStorageDriver"),
    Provider.SCALEWAY: ("libcloud.storage.drivers.scaleway", "ScalewayStorageDriver"),
    Provider.OVH: ("libcloud.storage.drivers.ovh", "OvhStorageDriver"),
}


def get_driver(provider):
    # type: (Union[Provider, str]) -> Type[StorageDriver]
    deprecated_constants = OLD_CONSTANT_TO_NEW_MAPPING
    return _get_provider_driver(
        drivers=DRIVERS, provider=provider, deprecated_constants=deprecated_constants
    )


def set_driver(provider, module, klass):
    # type: (Union[Provider, str], ModuleType, type) -> Type[StorageDriver]
    return _set_provider_driver(drivers=DRIVERS, provider=provider, module=module, klass=klass)
