#include "file_params.h"

#include <windows.h>

namespace NKikimr {

TString GetLastErrorStr() {
    TStringStream errStr;
    DWORD errorId = GetLastError();
    LPSTR messageBuffer = nullptr;
    FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
            NULL, errorId, 0, (LPSTR)&messageBuffer, 0, NULL);
    errStr << messageBuffer;
    LocalFree(messageBuffer);
    return errStr.Str();
}

void DetectFileParameters(TString path, ui64 &outDiskSizeBytes, bool &outIsBlockDevice) {
    HANDLE hFile = CreateFile(path.c_str(), GENERIC_READ, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    //int file = open(path.c_str(), O_RDWR);
    if (hFile == INVALID_HANDLE_VALUE) {
        TStringStream errStr;
        errStr << "Can't open file, path# \"" << path << "\": errorStr# " << GetLastErrorStr();
        ythrow yexception() << errStr.Str();
    } else {
        LARGE_INTEGER lFileSize;
        if (GetFileSizeEx(hFile, &lFileSize) != 0) {
            outDiskSizeBytes = (ui64)lFileSize.QuadPart;
        } else {
            TStringStream errStr;
            errStr << "Can't get file size, path# \"" << path << "\": errorStr# " << GetLastErrorStr();
            ythrow yexception() << errStr.Str();
        }
    }
}

std::optional<NPDisk::TDriveData> FindDeviceBySerialNumber(const TString& /*serial*/, bool /*partlabelOnly*/) {
    return {};
}

TVector<NPDisk::TDriveData> ListDevicesWithPartlabel(TStringStream& /*details*/) {
    return {};
}

}
