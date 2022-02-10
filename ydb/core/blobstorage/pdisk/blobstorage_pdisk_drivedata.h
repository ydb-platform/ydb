#pragma once 
 
#include <ydb/core/base/blobstorage_pdisk_category.h>
#include <util/generic/string.h>

namespace NKikimrBlobStorage {
class TDriveData;
}

namespace NKikimr { 
namespace NPDisk { 
 
struct TDriveData { 
    TString Path;
    bool IsWriteCacheValid = false;
    bool IsWriteCacheEnabled = false;
    TString SerialNumber; 
    TString FirmwareRevision; 
    TString ModelNumber; 
    TPDiskCategory::EDeviceType DeviceType = TPDiskCategory::DEVICE_TYPE_UNKNOWN;
    ui64 Size = 0;
    bool IsMock = false;
 
    TDriveData() = default;

    TDriveData(const NKikimrBlobStorage::TDriveData& p);
 
    TString ToString(bool isMultiline) const;

    void ToProto(NKikimrBlobStorage::TDriveData *p) const;
}; 
 
bool operator==(const NPDisk::TDriveData& lhs, const NPDisk::TDriveData& rhs);

} // NPDisk 
} // NKikimr 
