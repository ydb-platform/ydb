import logging
import os
import re
from typing import List, Optional

from ydb.tools.mnc.agent import config
from ydb.tools.mnc.agent.schemas.disk import (
    DiskCheckItemSchema,
    DiskCheckRequest,
    DiskCheckResponse,
    DiskDeviceSchema,
    DiskInfoRequest,
    DiskInfoResponse,
    DiskInfoSchema,
    DiskOperationItemSchema,
    DiskOperationRequest,
    DiskOperationResponse,
    DiskPartSchema,
)
from ydb.tools.mnc.lib import common, device as device_lib
from ydb.tools.mnc.lib import term


logger = logging.getLogger(__name__)

PARTLABEL_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
DEVICE_RE = re.compile(r"^/dev/(sd[a-z]+|nvme\d+n\d+)$")


class DiskService:
    def _validate_partlabel(self, partlabel: str):
        if not PARTLABEL_RE.fullmatch(partlabel):
            raise ValueError(f"invalid partlabel: {partlabel}")
        if not config.is_managed_partlabel(partlabel):
            raise ValueError(f"partlabel is not managed by this agent: {partlabel}")

    def _validate_device_path(self, device: Optional[str]):
        if device is not None and not DEVICE_RE.fullmatch(device):
            raise ValueError(f"invalid device path: {device}")

    def _make_device(self, device: DiskDeviceSchema) -> common.Device:
        self._validate_partlabel(device.partlabel)
        self._validate_device_path(device.device)
        return common.Device(device.partlabel, device.device)

    async def _run(self, args: List[str]):
        return await term.run(args)

    async def get_partlabels(self, pattern: Optional[str] = None) -> Optional[List[str]]:
        labels_path = "/dev/disk/by-partlabel"
        try:
            labels = os.listdir(labels_path)
        except FileNotFoundError:
            return []
        except Exception as exc:
            logger.error("failed to list %s: %s", labels_path, exc)
            return None
        if pattern:
            self._validate_partlabel(pattern)
            labels = [label for label in labels if pattern in label]
        labels = [label for label in labels if config.is_managed_partlabel(label)]
        return sorted(labels)

    async def get_link(self, partlabel: str) -> Optional[str]:
        self._validate_partlabel(partlabel)
        result = await self._run(["sudo", "readlink", "-e", f"/dev/disk/by-partlabel/{partlabel}"])
        if result.returncode:
            return None
        return result.stdout.strip()

    async def parted_info(self, device: common.Device) -> device_lib.DeviceInfo:
        path = device.path if device.path else await self.get_link(device.partlabel)
        good_path = device_lib.from_part_path_to_device_path(path)
        if not good_path:
            return device_lib.DeviceInfo.make_error(device.path, f"not correct path: '{path}'")
        result = await self._run(["sudo", "parted", good_path, "-m", "-s", "print"])
        if result.returncode:
            return device_lib.DeviceInfo.make_error(good_path, result.stderr)
        info = device_lib.make_device_info(result.stdout)
        if not info:
            return device_lib.DeviceInfo.make_error(good_path, f"can't parse parted output: {result.stdout}")
        return info

    async def remove_part(self, path: str, part_id: str) -> bool:
        if str(part_id) == "1":
            logger.error("rm was forbidden, can't remove first part")
            return False
        result = await self._run(["sudo", "parted", path, "-m", "-s", "rm", str(part_id)])
        return result.returncode == 0

    async def make_part(self, path: str, label: str, last_end, part_end) -> bool:
        self._validate_partlabel(label)
        result = await self._run(["sudo", "parted", path, "-s", "mkpart", label, str(last_end), str(part_end)])
        return result.returncode == 0

    async def parted_disk(self, device: common.Device, part_size: common.Memory = None, part_count: int = None) -> bool:
        info = await self.parted_info(device)
        if not info or info.error:
            logger.error("failed to receive parted info for %s: %s", device.partlabel, getattr(info, "error", None))
            return False

        full = True
        if not part_count and part_size:
            part_count = int(info.size / part_size)
        elif not part_size and part_count:
            part_size = info.size / part_count
        elif not part_count and not part_size:
            raise ValueError("part_count or part_size is required")
        else:
            full = False

        with_error = False
        for part in info.parts:
            if part.id == "1":
                continue
            if not await self.remove_part(info.path, part.id):
                logger.error("failed to remove part %s on %s", part.id, info.path)
                with_error = True
        if with_error:
            return False

        info = await self.parted_info(device)
        if not info or info.error or len(info.parts) != 1:
            logger.error("error during removing old parts on %s", device.partlabel)
            return False

        last_end = info.parts[0].to_mem
        for idx in range(2, part_count + 2):
            part_end = last_end + part_size
            if full and idx == part_count + 1:
                part_end = "100%"
            label = device.partlabel if part_count == 1 else f"{device.partlabel}_{idx - 2}"
            if not await self.make_part(info.path, label, last_end, part_end):
                logger.error("failed to make part %s on %s with label %s", idx, info.path, label)
                return False
            last_end = part_end
        return True

    def _info_to_schema(self, source: DiskDeviceSchema, info: device_lib.DeviceInfo) -> DiskInfoSchema:
        return DiskInfoSchema(
            partlabel=source.partlabel,
            device=source.device,
            path=info.path,
            size=str(info.size) if info.size is not None else None,
            parts=[
                DiskPartSchema(
                    id=part.id,
                    from_mem=str(part.from_mem),
                    to_mem=str(part.to_mem),
                    size=str(part.size),
                    label=part.label,
                )
                for part in (info.parts or [])
            ],
            error=info.error,
        )

    async def info(self, request: DiskInfoRequest) -> DiskInfoResponse:
        if not request.devices:
            labels = await self.get_partlabels()
            if labels is None:
                return DiskInfoResponse(disks=[DiskInfoSchema(partlabel="", error="failed to list partlabels")])
            request.devices = [DiskDeviceSchema(partlabel=label) for label in labels]

        disks = []
        for source in request.devices:
            try:
                device = self._make_device(source)
                disks.append(self._info_to_schema(source, await self.parted_info(device)))
            except Exception as exc:
                disks.append(DiskInfoSchema(partlabel=source.partlabel, device=source.device, error=str(exc)))
        return DiskInfoResponse(disks=disks)

    async def _check_disk_for_use(self, disk: DiskDeviceSchema) -> DiskCheckItemSchema:
        labels = await self.get_partlabels(disk.partlabel)
        success = labels is not None and disk.partlabel in labels
        return DiskCheckItemSchema(
            partlabel=disk.partlabel,
            device=disk.device,
            success=success,
            labels=labels or [],
            message=None if success else f"partlabel {disk.partlabel} not found",
        )

    async def _check_disk_for_split(self, disk: DiskDeviceSchema) -> DiskCheckItemSchema:
        if disk.device is None:
            return DiskCheckItemSchema(partlabel=disk.partlabel, device=disk.device, success=False, message="device is required")
        labels = await self.get_partlabels(disk.partlabel)
        if labels is None:
            return DiskCheckItemSchema(partlabel=disk.partlabel, device=disk.device, success=False, message="failed to list partlabels")

        matched_labels = [label for label in labels if label.startswith(disk.partlabel)]
        if not matched_labels:
            return DiskCheckItemSchema(
                partlabel=disk.partlabel,
                device=disk.device,
                success=False,
                labels=[],
                message=f"partlabel prefix {disk.partlabel} not found",
            )
        for label in matched_labels:
            remote_path = await self.get_link(label)
            if remote_path is None or not remote_path.startswith(disk.device):
                return DiskCheckItemSchema(
                    partlabel=disk.partlabel,
                    device=disk.device,
                    success=False,
                    labels=matched_labels,
                    message=f"partlabel {label} points to {remote_path}, expected prefix {disk.device}",
                )

        return DiskCheckItemSchema(partlabel=disk.partlabel, device=disk.device, success=True, labels=matched_labels)

    async def check(self, request: DiskCheckRequest) -> DiskCheckResponse:
        checks = []
        for disk in request.disks_for_split:
            try:
                self._make_device(disk)
                checks.append(await self._check_disk_for_split(disk))
            except Exception as exc:
                checks.append(DiskCheckItemSchema(partlabel=disk.partlabel, device=disk.device, success=False, message=str(exc)))
        for disk in request.disks_for_use:
            try:
                self._make_device(disk)
                checks.append(await self._check_disk_for_use(disk))
            except Exception as exc:
                checks.append(DiskCheckItemSchema(partlabel=disk.partlabel, device=disk.device, success=False, message=str(exc)))
        return DiskCheckResponse(success=all(check.success for check in checks), checks=checks)

    async def split(self, request: DiskOperationRequest) -> DiskOperationResponse:
        return await self._partition(request, operation="split")

    async def unite(self, request: DiskOperationRequest) -> DiskOperationResponse:
        request.part_count = 1
        request.part_size = None
        return await self._partition(request, operation="unite")

    async def _partition(self, request: DiskOperationRequest, operation: str) -> DiskOperationResponse:
        part_size = common.Memory(request.part_size) if request.part_size else None
        operations = []
        for source in request.devices:
            try:
                device = self._make_device(source)
                ok = await self.parted_disk(device, part_size=part_size, part_count=request.part_count)
                operations.append(
                    DiskOperationItemSchema(
                        partlabel=source.partlabel,
                        device=source.device,
                        success=ok,
                        message=f"{operation} completed" if ok else f"{operation} failed",
                    )
                )
            except Exception as exc:
                operations.append(DiskOperationItemSchema(partlabel=source.partlabel, device=source.device, success=False, message=str(exc)))
        return DiskOperationResponse(success=all(operation.success for operation in operations), operations=operations)

    async def obliterate(self, request: DiskOperationRequest) -> DiskOperationResponse:
        operations = []
        for source in request.devices:
            try:
                device = self._make_device(source)
                info = await self.parted_info(device)
                if info.error:
                    operations.append(
                        DiskOperationItemSchema(
                            partlabel=source.partlabel,
                            device=source.device,
                            success=False,
                            message=info.error,
                        )
                    )
                    continue

                ok = True
                messages = []
                for part in info.parts:
                    if part.id == "1" or not part.label:
                        continue
                    self._validate_partlabel(part.label)
                    result = await self._run(
                        [
                            "sudo",
                            f"{config.mnc_home}/ydb/bin/ydb",
                            "admin",
                            "blobstorage",
                            "disk",
                            "obliterate",
                            f"/dev/disk/by-partlabel/{part.label}",
                        ]
                    )
                    if result.returncode:
                        ok = False
                        messages.append(f"{part.label}: {result.stderr.strip()}")
                operations.append(
                    DiskOperationItemSchema(
                        partlabel=source.partlabel,
                        device=source.device,
                        success=ok,
                        message="obliterate completed" if ok else "; ".join(messages),
                    )
                )
            except Exception as exc:
                operations.append(DiskOperationItemSchema(partlabel=source.partlabel, device=source.device, success=False, message=str(exc)))
        return DiskOperationResponse(success=all(operation.success for operation in operations), operations=operations)


disk_service = DiskService()
