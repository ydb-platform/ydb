#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKvVolumeStress {

struct TOptions {
    TString Endpoint = "grpc://localhost:2135";
    TString Database;
    ui32 Duration = 120;
    ui32 InFlight = 1;
    ui32 ActionPoolSize = 1;
    ui32 GrpcCqThreads = 0;
    TString Version = "v1";
    TString ConfigPath;
    TString ConfigName;
    bool AllowErrors = false;
    bool Verbose = false;
    bool NoTui = false;
};

struct TKeyInfo {
    ui32 PartitionId = 0;
    ui32 KeySize = 0;
};

} // namespace NKvVolumeStress
