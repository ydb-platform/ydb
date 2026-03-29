#pragma once

namespace NKikimr::NDDisk {

struct TDDiskConfig {
    bool UseSQPoll = false;
    bool UseIOPoll = false;
    bool ForcePDiskFallback = false;
};

} // NKikimr::NDDisk
