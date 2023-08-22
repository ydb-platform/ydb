#include "file_params.h"

#include <ydb/library/pdisk_io/drivedata.h>
#include <ydb/library/pdisk_io/spdk_state.h>
#include <ydb/library/pdisk_io/aio.h>
#include <ydb/library/pdisk_io/wcache.h>

#include <regex>

#ifdef RWF_APPEND
static constexpr ui64 RWFAppendCheck = (ui64)RWF_APPEND;
#define NEED_CHECK
#undef RWF_APPEND
#endif

#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef NEED_CHECK
static_assert(RWFAppendCheck == (ui64)RWF_APPEND);
#endif

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
                if (ioctl(file, BLKGETSIZE64, &outDiskSizeBytes) < 0) {
                    ythrow yexception() << "Can't get device size, errno# " << errno << ", strerror(errno)# "
                        << strerror(errno) << Endl;
                }
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

static TVector<NPDisk::TDriveData> FilterOnlyUniqueSerial(TVector<NPDisk::TDriveData> devices) {
    TVector<NPDisk::TDriveData> result;
    std::sort(devices.begin(), devices.end(),
        [] (const NPDisk::TDriveData& lhs, const NPDisk::TDriveData& rhs) {
            return lhs.SerialNumber < rhs.SerialNumber;
        }
    );

    for (size_t i = 0; i < devices.size(); ) {
        bool duplicate = false;
        while (i + 1 < devices.size() && devices[i].SerialNumber == devices[i + 1].SerialNumber) {
            ++i;
            duplicate = true;
        }
        if (!duplicate) {
            result.push_back(devices[i]);
        }
        ++i;
    }
    return result;
}

static TVector<NPDisk::TDriveData> ListDevices(const char *folder, const TString& serial, std::regex device_regex, TStringStream& details) {
    TFsPath path(folder);
    TVector<TFsPath> children;
    try {
        path.List(children);
    } catch (std::exception&) {
        return {};
    }
    TVector<NPDisk::TDriveData> devicesFound;
    for (const auto& child : children) {
        if (std::regex_match(child.GetName().c_str(), device_regex)) {
            std::optional<NPDisk::TDriveData> data = NPDisk::GetDriveData(child.GetPath(), &details);
            if (data && (!serial || data->SerialNumber == serial)) {
                devicesFound.push_back(*data);
            }
            details << "#";
        }
    }

    return FilterOnlyUniqueSerial(devicesFound);
}

static TVector<NPDisk::TDriveData> ListDevices(const char *folder, const TString& serial, std::regex device_regex) {
    TStringStream details;
    return ListDevices(folder, serial, device_regex, details);
}

static std::optional<NPDisk::TDriveData> FindDeviceBySerialNumber(const char *folder, const TString& serial,
        std::regex device_regex) {
    TVector<NPDisk::TDriveData> devicesFound = ListDevices(folder, serial, device_regex);

    // There must be only one device with the serial
    // If the folder is /dev, then device exptected not to have GPT partitions
    // If the folder is /dev/disk/by-partlabel, then there only one symlink to partition to be found
    if (devicesFound.size() == 1) {
        return {devicesFound.front()};
    } else {
        return {};
    }
}

static const std::regex kikimrDevice{".*(kikimr|KIKIMR).*"};

TVector<NPDisk::TDriveData> ListAllDevices() {
    return ListDevices("/dev", {}, std::regex(".*"));
}

TVector<NPDisk::TDriveData> ListDevicesWithPartlabel(TStringStream& details) {
    return ListDevices("/dev/disk/by-partlabel", "", kikimrDevice, details);
}

std::optional<NPDisk::TDriveData> FindDeviceBySerialNumber(const TString& serial, bool partlabelOnly) {
    std::optional<NPDisk::TDriveData> data;

    if (data = FindDeviceBySerialNumber("/dev/disk/by-partlabel", serial, kikimrDevice)) {
        return data;
    } else if (partlabelOnly) {
        return {};
    } else if (data = FindDeviceBySerialNumber("/dev", serial, std::regex("sd\\w\\d*"))) {
        return data;
    } else {
        return {};
    }
}

}
