from typing import List, Optional
from pydantic import BaseModel, Field


class DiskDeviceSchema(BaseModel):
    partlabel: str = Field(..., description="Expected partition label")
    device: Optional[str] = Field(None, description="Expected block device path")


class DiskPartSchema(BaseModel):
    id: str
    from_mem: str
    to_mem: str
    size: str
    label: str


class DiskInfoSchema(BaseModel):
    partlabel: str
    device: Optional[str] = None
    path: Optional[str] = None
    size: Optional[str] = None
    parts: List[DiskPartSchema] = Field(default_factory=list)
    error: Optional[str] = None


class DiskCheckItemSchema(BaseModel):
    partlabel: str
    device: Optional[str] = None
    success: bool
    message: Optional[str] = None
    labels: List[str] = Field(default_factory=list)


class DiskCheckRequest(BaseModel):
    disks_for_split: List[DiskDeviceSchema] = Field(default_factory=list)
    disks_for_use: List[DiskDeviceSchema] = Field(default_factory=list)


class DiskCheckResponse(BaseModel):
    success: bool
    checks: List[DiskCheckItemSchema] = Field(default_factory=list)


class DiskInfoRequest(BaseModel):
    devices: List[DiskDeviceSchema] = Field(default_factory=list)


class DiskInfoResponse(BaseModel):
    disks: List[DiskInfoSchema] = Field(default_factory=list)


class DiskOperationRequest(BaseModel):
    devices: List[DiskDeviceSchema] = Field(default_factory=list)
    part_count: Optional[int] = None
    part_size: Optional[str] = None


class DiskOperationItemSchema(BaseModel):
    partlabel: str
    device: Optional[str] = None
    success: bool
    message: Optional[str] = None


class DiskOperationResponse(BaseModel):
    success: bool
    operations: List[DiskOperationItemSchema] = Field(default_factory=list)
