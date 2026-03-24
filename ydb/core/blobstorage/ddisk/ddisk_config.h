#pragma once

namespace NKikimr::NDDisk {

struct TDDiskConfig {
    bool UseSQPoll = false;
    bool UseIOPoll = false;
};

} // NKikimr::NDDisk
