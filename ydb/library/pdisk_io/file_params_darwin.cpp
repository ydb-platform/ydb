#include "file_params.h"

#include <fcntl.h>
#include <sys/disk.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace NKikimr {

void DetectFileParameters(TString path, ui64 &outDiskSizeBytes, bool &outIsBlockDevice) {
    int file = open(path.c_str(), O_RDWR);
    if (file < 0) {
        TStringStream errStr;
        errStr << "Can't open file \"" << path << "\": ";
        if (errno == EACCES) {
            errStr << "you have no rights";
        } else if (errno == ENOENT) {
            errStr << "no such file";
        } else {
            errStr << "unknown reason, errno# " << errno << ", strerror(errno)# " << strerror(errno);
        }
        ythrow yexception() << errStr.Str();
    } else {
        struct stat stats;
        if (fstat(file, &stats) == 0) {
            if (S_ISREG(stats.st_mode)) {
                outIsBlockDevice = false;
                outDiskSizeBytes = stats.st_size;
            } else if (S_ISBLK(stats.st_mode)) {
                outIsBlockDevice = true;
                ui64 sectorCount = 0;
                if (ioctl(file, DKIOCGETBLOCKCOUNT, &sectorCount) < 0) {
                    ythrow yexception() << "Can't get device size, errno# " << errno << ", strerror(errno)# "
                        << strerror(errno) << Endl;
                }
                ui32 sectorSize = 0;
                if (ioctl(file, DKIOCGETBLOCKSIZE, &sectorSize) < 0) {
                    ythrow yexception() << "Can't get device size, errno# " << errno << ", strerror(errno)# "
                        << strerror(errno) << Endl;
                }
                outDiskSizeBytes = sectorCount * sectorSize;
            } else {
                ythrow yexception() << "Unknown file type - neither file nor block device" << Endl;
            }
        } else {
            ythrow yexception() << "Can't get info about file/device, errno# " << errno << ", strerror(errno)# "
                << strerror(errno) << Endl;
        }
        close(file);
    }
}

std::optional<NPDisk::TDriveData> FindDeviceBySerialNumber(const TString& /*serial*/, bool /*partlabelOnly*/) {
    return {};
}

TVector<NPDisk::TDriveData> ListDevicesWithPartlabel(TStringStream& /*details*/) {
    return {};
}

}
