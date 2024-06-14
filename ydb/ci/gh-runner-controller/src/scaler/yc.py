import logging
import requests
import yandexcloud

# noinspection PyUnresolvedReferences
from yandex.cloud.compute.v1.image_service_pb2 import GetImageLatestByFamilyRequest
from yandex.cloud.compute.v1.image_service_pb2_grpc import ImageServiceStub

# noinspection PyUnresolvedReferences
from yandex.cloud.compute.v1.instance_pb2 import Instance, SchedulingPolicy

# noinspection PyUnresolvedReferences
from yandex.cloud.compute.v1.instance_service_pb2 import (
    AttachedDiskSpec,
    CreateInstanceMetadata,
    CreateInstanceRequest,
    DeleteInstanceRequest,
    GetInstanceRequest,
    ListInstancesRequest,
    NetworkInterfaceSpec,
    PrimaryAddressSpec,
    ResourcesSpec,
)
from yandex.cloud.compute.v1.instance_service_pb2_grpc import InstanceServiceStub

# noinspection PyUnresolvedReferences
from yandex.cloud.lockbox.v1.payload_service_pb2 import GetPayloadRequest
from yandex.cloud.lockbox.v1.payload_service_pb2_grpc import PayloadServiceStub


def metadata_query(path):
    headers = {"Metadata-Flavor": "Google"}
    # noinspection HttpUrlsUsage
    return requests.get(f"http://169.254.169.254{path}", headers=headers)


def discover_folder_id():
    return metadata_query("/computeMetadata/v1/instance/vendor/folder-id").text


class YandexCloudProvider:
    def __init__(self, use_metadata, sa_key):
        self.logger = logging.getLogger(__name__)
        if not use_metadata and not sa_key:
            raise ValueError("Service account key or metadata discovery is required")

        if use_metadata:
            self.sdk = yandexcloud.SDK()
        else:
            self.sdk = yandexcloud.SDK(service_account_key=sa_key)
        self.cfg = None

    def set_config(self, cfg):
        # FIXME: strange decision
        self.cfg = cfg

    def get_latest_image_id(self):
        image_service = self.sdk.client(ImageServiceStub)
        image = image_service.GetLatestByFamily(
            GetImageLatestByFamilyRequest(folder_id=self.cfg.yc_folder_id, family=self.cfg.image_family)
        )
        return image.id

    def get_vm(self, instance_id):
        return self.sdk.client(InstanceServiceStub).Get(GetInstanceRequest(instance_id=instance_id))

    def get_vm_list(self, prefix):
        request = ListInstancesRequest(folder_id=self.cfg.yc_folder_id, page_size=1000)
        cnt = cnt_provisioning = 0
        names = dict()

        while 1:
            response = self.sdk.client(InstanceServiceStub).List(request)
            for instance in response.instances:
                if prefix in instance.labels:
                    cnt += 1

                    if instance.status == Instance.Status.PROVISIONING:
                        cnt_provisioning += 1
                    elif instance.status != Instance.Status.DELETING:
                        names[instance.labels[prefix]] = (instance.id, instance.created_at.ToDatetime())

            request.page_token = response.next_page_token
            if not request.page_token:
                break

        return names, cnt, cnt_provisioning

    def start_vm(self, zone_id: str, subnet_id: str, instance_name: str, preset_name: str, user_data, vm_labels):
        metadata = {
            "serial-port-enable": "1",
            "user-data": user_data,
        }

        preset = self.cfg.vm_presets["default"].copy()

        if preset_name in self.cfg.vm_presets:
            preset.update(self.cfg.vm_presets[preset_name])

        self.logger.info("create vm with preset: %r, labels %r", preset, vm_labels)

        #
        # class A:
        #     id = "fake-id"
        #
        # return A()

        request = CreateInstanceRequest(
            name=instance_name,
            folder_id=self.cfg.yc_folder_id,
            zone_id=zone_id,
            platform_id=preset['platform_id'],
            resources_spec=ResourcesSpec(
                cores=int(preset["cpu_cores"]),
                memory=int(preset["memory_gb"] * 1024 * 1024 * 1024),
                core_fraction=100,
            ),
            scheduling_policy=SchedulingPolicy(preemptible=preset['preemptible']),
            boot_disk_spec=AttachedDiskSpec(
                auto_delete=True,
                disk_spec=AttachedDiskSpec.DiskSpec(
                    type_id=preset["disk_type"],
                    size=int(preset["disk_size_gb"] * 1024 * 1024 * 1024),
                    image_id=self.get_latest_image_id(),
                ),
            ),
            network_interface_specs=[
                NetworkInterfaceSpec(
                    subnet_id=subnet_id,
                    primary_v4_address_spec=PrimaryAddressSpec(),
                ),
            ],
            labels=vm_labels,
            metadata=metadata,
        )

        op = self.sdk.client(InstanceServiceStub).Create(request)
        return op
        # result = self.sdk.wait_operation_and_get_result(op, response_type=Instance, meta_type=CreateInstanceMetadata)
        # return result.response

    def delete_vm(self, instance_id, our_label):
        instance = self.get_vm(instance_id)
        if our_label not in instance.labels:
            raise Exception("Unable to delete non runner instance")

        op = self.sdk.client(InstanceServiceStub).Delete(DeleteInstanceRequest(instance_id=instance_id))

        return op

    def get_lockbox_secret_entries(self, secret_id):
        secret = self.sdk.client(PayloadServiceStub).Get(GetPayloadRequest(secret_id=secret_id))
        return {e.key: e.text_value for e in secret.entries}
