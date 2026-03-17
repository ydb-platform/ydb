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

from libcloud.common.types import LibcloudError

__all__ = [
    "Provider",
    "ContainerError",
    "ObjectError",
    "ContainerAlreadyExistsError",
    "ContainerDoesNotExistError",
    "ContainerIsNotEmptyError",
    "ObjectDoesNotExistError",
    "ObjectHashMismatchError",
    "InvalidContainerNameError",
    "OLD_CONSTANT_TO_NEW_MAPPING",
]


class Provider:
    """
    Defines for each of the supported providers

    Non-Dummy drivers are sorted in alphabetical order. Please preserve this
    ordering when adding new drivers.

    :cvar DUMMY: Example provider
    :cvar ALIYUN_OSS: Aliyun OSS storage driver
    :cvar AURORAOBJECTS: AuroraObjects storage driver
    :cvar AZURE_BLOBS: Azure Blob Storage driver
    :cvar BACKBLAZE_B2: Backblaze B2 Cloud Storage driver
    :cvar CLOUDFILES: CloudFiles
    :cvar DIGITALOCEAN_SPACES: Digital Ocean Spaces driver
    :cvar GOOGLE_STORAGE Google Storage
    :cvar KTUCLOUD: KT UCloud Storage driver
    :cvar LOCAL: Local storage driver
    :cvar NIMBUS: Nimbus.io driver
    :cvar NINEFOLD: Ninefold
    :cvar OPENSTACK_SWIFT: OpenStack Swift driver
    :cvar S3: Amazon S3 US
    :cvar S3_AP_NORTHEAST: Amazon S3 Asia North East (Tokyo)
    :cvar S3_AP_NORTHEAST1: Amazon S3 Asia North East (Tokyo)
    :cvar S3_AP_NORTHEAST2: Amazon S3 Asia North East (Seoul)
    :cvar S3_AP_SOUTH: Amazon S3 Asia South (Mumbai)
    :cvar S3_AP_SOUTHEAST: Amazon S3 Asia South East (Singapore)
    :cvar S3_AP_SOUTHEAST2: Amazon S3 Asia South East 2 (Sydney)
    :cvar S3_CA_CENTRAL: Amazon S3 Canada (Central)
    :cvar S3_CN_NORTH: Amazon S3 CN North (Beijing)
    :cvar S3_EU_WEST: Amazon S3 EU West (Ireland)
    :cvar S3_EU_WEST2: Amazon S3 EU West 2 (London)
    :cvar S3_EU_WEST3: Amazon S3 EU West 3 (Paris)
    :cvar S3_EU_CENTRAL: Amazon S3 EU Central (Frankfurt)
    :cvar S3_EU_NORTH1: Amazon S3 EU North 1 (Stockholm)
    :cvar S3_SA_EAST: Amazon S3 South America East (Sao Paulo)
    :cvar S3_US_EAST2: Amazon S3 US East 2 (Ohio)
    :cvar S3_US_WEST: Amazon S3 US West (Northern California)
    :cvar S3_US_WEST_OREGON: Amazon S3 US West 2 (Oregon)
    :cvar S3_US_GOV_WEST: Amazon S3 GovCloud (US)
    :cvar S3_RGW: S3 RGW
    :cvar S3_RGW_OUTSCALE: OUTSCALE S3 RGW
    """

    DUMMY = "dummy"
    ALIYUN_OSS = "aliyun_oss"
    AURORAOBJECTS = "auroraobjects"
    AZURE_BLOBS = "azure_blobs"
    BACKBLAZE_B2 = "backblaze_b2"
    CLOUDFILES = "cloudfiles"
    DIGITALOCEAN_SPACES = "digitalocean_spaces"
    GOOGLE_STORAGE = "google_storage"
    KTUCLOUD = "ktucloud"
    LOCAL = "local"
    NIMBUS = "nimbus"
    NINEFOLD = "ninefold"
    OPENSTACK_SWIFT = "openstack_swift"
    S3 = "s3"
    S3_AP_NORTHEAST = "s3_ap_northeast"
    S3_AP_NORTHEAST1 = "s3_ap_northeast_1"
    S3_AP_NORTHEAST2 = "s3_ap_northeast_2"
    S3_AP_SOUTH = "s3_ap_south"
    S3_AP_SOUTHEAST = "s3_ap_southeast"
    S3_AP_SOUTHEAST2 = "s3_ap_southeast2"
    S3_CA_CENTRAL = "s3_ca_central"
    S3_CN_NORTH = "s3_cn_north"
    S3_CN_NORTHWEST = "s3_cn_northwest"
    S3_EU_WEST = "s3_eu_west"
    S3_EU_WEST2 = "s3_eu_west_2"
    S3_EU_WEST3 = "s3_eu_west_3"
    S3_EU_CENTRAL = "s3_eu_central"
    S3_EU_NORTH1 = "s3_eu_north_1"
    S3_SA_EAST = "s3_sa_east"
    S3_US_EAST2 = "s3_us_east_2"
    S3_US_WEST = "s3_us_west"
    S3_US_WEST_OREGON = "s3_us_west_oregon"
    S3_US_GOV_WEST = "s3_us_gov_west"
    S3_RGW = "s3_rgw"
    S3_RGW_OUTSCALE = "s3_rgw_outscale"
    MINIO = "minio"
    SCALEWAY = "scaleway"
    OVH = "ovh"

    # Deprecated
    CLOUDFILES_US = "cloudfiles_us"
    CLOUDFILES_UK = "cloudfiles_uk"
    CLOUDFILES_SWIFT = "cloudfiles_swift"


OLD_CONSTANT_TO_NEW_MAPPING = {
    # CloudFiles
    Provider.CLOUDFILES_US: Provider.CLOUDFILES,
    Provider.CLOUDFILES_UK: Provider.CLOUDFILES_UK,
    Provider.CLOUDFILES_SWIFT: Provider.OPENSTACK_SWIFT,
}


class ContainerError(LibcloudError):
    error_type = "ContainerError"

    def __init__(self, value, driver, container_name):
        self.container_name = container_name
        super().__init__(value=value, driver=driver)

    def __str__(self):
        return "<{} in {}, container={}, value={}>".format(
            self.error_type,
            repr(self.driver),
            self.container_name,
            self.value,
        )


class ObjectError(LibcloudError):
    error_type = "ContainerError"

    def __init__(self, value, driver, object_name):
        self.object_name = object_name
        super().__init__(value=value, driver=driver)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<{} in {}, value={}, object = {}>".format(
            self.error_type,
            repr(self.driver),
            self.value,
            self.object_name,
        )


class ContainerAlreadyExistsError(ContainerError):
    error_type = "ContainerAlreadyExistsError"


class ContainerDoesNotExistError(ContainerError):
    error_type = "ContainerDoesNotExistError"


class ContainerIsNotEmptyError(ContainerError):
    error_type = "ContainerIsNotEmptyError"


class ObjectDoesNotExistError(ObjectError):
    error_type = "ObjectDoesNotExistError"


class ObjectHashMismatchError(ObjectError):
    error_type = "ObjectHashMismatchError"


class InvalidContainerNameError(ContainerError):
    error_type = "InvalidContainerNameError"
