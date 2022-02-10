#include "defs.h"

#include "blobstorage_pdisk_drivedata.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {
namespace NPDisk {

TDriveData::TDriveData(const NKikimrBlobStorage::TDriveData& p)
    : Path(p.GetPath())
    , SerialNumber(p.GetSerialNumber())
    , FirmwareRevision(p.GetFirmwareRevision())
    , ModelNumber(p.GetModelNumber())
    , DeviceType(PDiskTypeToPDiskType(p.GetDeviceType()))
    , Size(p.HasSize() ? p.GetSize() : 0)
    , IsMock(p.HasIsMock() ? p.GetIsMock() : false)
{}

TString TDriveData::ToString(bool isMultiline) const {
    const char *x = isMultiline ? "\n" : "";
    TStringStream str;
    str << "{Path# " << Path.Quote() << x
        << " IsWriteCacheValid# " << IsWriteCacheValid << x
        << " IsWriteCacheEnabled# " << IsWriteCacheEnabled << x
        << " ModelNumber# " << ModelNumber.Quote() << x
        << " SerialNumber# " << SerialNumber.Quote() << x
        << " FirmwareRevision# " << FirmwareRevision.Quote() << x
        << " DeviceType# " << TPDiskCategory::DeviceTypeStr(DeviceType, true) << x
        << "}" << x;
    return str.Str();
}

void TDriveData::ToProto(NKikimrBlobStorage::TDriveData *p) const {
    p->SetPath(Path);
    p->SetSerialNumber(SerialNumber);
    p->SetFirmwareRevision(FirmwareRevision);
    p->SetModelNumber(ModelNumber);
    p->SetDeviceType(PDiskTypeToPDiskType(DeviceType));
    p->SetSize(Size);
    p->SetIsMock(IsMock);
}

bool operator==(const NPDisk::TDriveData& lhs, const NPDisk::TDriveData& rhs) {
    NKikimrBlobStorage::TDriveData l_p;
    lhs.ToProto(&l_p);
    TString l_s;
    Y_PROTOBUF_SUPPRESS_NODISCARD l_p.SerializeToString(&l_s);
    NKikimrBlobStorage::TDriveData r_p;
    rhs.ToProto(&r_p);
    TString r_s;
    Y_PROTOBUF_SUPPRESS_NODISCARD r_p.SerializeToString(&r_s);
    return l_s == r_s;
}

} // NPDisk
} // NKikimr
