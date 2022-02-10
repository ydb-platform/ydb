#pragma once

namespace NKikiSched {
    class ILogger;
}

namespace NKikimr {
namespace NTable {

namespace NTest {

    enum class ESponge {
        None    = 0,
        Murmur  = 1,
        City    = 2,
        Fnv     = 3,
        Xor     = 4,
    };
}

namespace NPerf {
    using ILogger = NKikiSched::ILogger;
}

}
}
