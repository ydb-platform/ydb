#pragma once

#include <util/system/types.h>

namespace NKikimr {
namespace NFake {

    struct TConf {
        ui64 Shared = 32 * (ui64(1) << 20); // Shared cache limit, bytes
        ui64 ScanQueue = 256 * 1024;        // Scan queue flight bytes
        ui64 AsyncQueue = 256 * 1024;       // Async queue flight bytes
    };

}
}
