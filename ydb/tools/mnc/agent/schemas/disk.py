from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class DiskDeviceSchema:
    partlabel: str
    device: Optional[str] = None


@dataclass
class DiskPartSchema:
    id: str
    from_mem: str
    to_mem: str
    size: str
    label: str


@dataclass
class DiskInfoSchema:
    partlabel: str
    device: Optional[str] = None
    path: Optional[str] = None
    size: Optional[str] = None
    parts: List[DiskPartSchema] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class DiskCheckItemSchema:
    partlabel: str
    device: Optional[str] = None
    success: bool = False
    message: Optional[str] = None
    labels: List[str] = field(default_factory=list)


@dataclass
class DiskCheckRequest:
    disks_for_split: List[DiskDeviceSchema] = field(default_factory=list)
    disks_for_use: List[DiskDeviceSchema] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            disks_for_split=[DiskDeviceSchema(**item) for item in data.get("disks_for_split", []) or []],
            disks_for_use=[DiskDeviceSchema(**item) for item in data.get("disks_for_use", []) or []],
        )


@dataclass
class DiskCheckResponse:
    success: bool
    checks: List[DiskCheckItemSchema] = field(default_factory=list)


@dataclass
class DiskInfoRequest:
    devices: List[DiskDeviceSchema] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(devices=[DiskDeviceSchema(**item) for item in data.get("devices", []) or []])


@dataclass
class DiskInfoResponse:
    disks: List[DiskInfoSchema] = field(default_factory=list)


@dataclass
class DiskOperationRequest:
    devices: List[DiskDeviceSchema] = field(default_factory=list)
    part_count: Optional[int] = None
    part_size: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            devices=[DiskDeviceSchema(**item) for item in data.get("devices", []) or []],
            part_count=data.get("part_count"),
            part_size=data.get("part_size"),
        )


@dataclass
class DiskOperationItemSchema:
    partlabel: str
    device: Optional[str] = None
    success: bool = False
    message: Optional[str] = None


@dataclass
class DiskOperationResponse:
    success: bool
    operations: List[DiskOperationItemSchema] = field(default_factory=list)
