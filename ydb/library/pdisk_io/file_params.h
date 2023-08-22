#pragma once

#include "drivedata.h"

#include <util/folder/path.h>
#include <optional>

namespace NKikimr {

void DetectFileParameters(TString path, ui64 &outDiskSizeBytes, bool &outIsBlockDevice);

std::optional<NPDisk::TDriveData> FindDeviceBySerialNumber(const TString& serial, bool partlabelOnly);

TVector<NPDisk::TDriveData> ListDevicesWithPartlabel(TStringStream& details);

TVector<NPDisk::TDriveData> ListAllDevices();

} // NKikimr
