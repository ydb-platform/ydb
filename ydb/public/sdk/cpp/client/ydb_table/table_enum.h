#pragma once

#include <limits>

namespace NYdb {
namespace NTable {

//! Column family compression codec
enum class EColumnFamilyCompression {
    None,
    LZ4,
};

//! State of build index operation
enum class EBuildIndexState {
    Unspecified = 0,
    Preparing = 1,
    TransferData = 2,
    Applying = 3,
    Done = 4,
    Cancellation = 5,
    Cancelled = 6,
};

enum class EIndexType {
    GlobalSync,
    GlobalAsync,

    Unknown = std::numeric_limits<int>::max()
};

enum class EChangefeedMode {
    KeysOnly,
    Updates,
    NewImage,
    OldImage,
    NewAndOldImages,

    Unknown = std::numeric_limits<int>::max()
};

enum class EChangefeedFormat {
    Json,

    Unknown = std::numeric_limits<int>::max()
};

} // namespace NTable
} // namespace NYdb
