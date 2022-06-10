#pragma once

namespace NKikimrBlobStorage {
class TDriveData;
}

namespace NKikimr {
namespace NPDisk {

struct TDriveData;

void DriveDataToDriveData(const TDriveData& data, NKikimrBlobStorage::TDriveData *out_p);
void DriveDataToDriveData(const NKikimrBlobStorage::TDriveData& p, TDriveData& out_data);

} // NPDisk
} // NKikimr
