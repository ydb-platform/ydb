#include "defs.h"

#include "drivedata_serializer.h"
#include <ydb/library/pdisk_io/drivedata.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {
namespace NPDisk {

void DriveDataToDriveData(const NKikimrBlobStorage::TDriveData& p, TDriveData& out_data) {
    out_data.Path = p.GetPath();
    out_data.SerialNumber = p.GetSerialNumber();
    out_data.FirmwareRevision = p.GetFirmwareRevision();
    out_data.ModelNumber = p.GetModelNumber();
    out_data.DeviceType = PDiskTypeToPDiskType(p.GetDeviceType());
    out_data.Size = (p.HasSize() ? p.GetSize() : 0);
    out_data.IsMock = (p.HasIsMock() ? p.GetIsMock() : false);
}

void DriveDataToDriveData(const TDriveData& data, NKikimrBlobStorage::TDriveData *out_p) {
    out_p->SetPath(data.Path);
    out_p->SetSerialNumber(data.SerialNumber);
    out_p->SetFirmwareRevision(data.FirmwareRevision);
    out_p->SetModelNumber(data.ModelNumber);
    out_p->SetDeviceType(PDiskTypeToPDiskType(data.DeviceType));
    out_p->SetSize(data.Size);
    out_p->SetIsMock(data.IsMock);
}

} // NPDisk
} // NKikimr
