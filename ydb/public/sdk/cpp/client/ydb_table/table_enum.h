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
    KeysOnly /* "KEYS_ONLY" */,
    Updates /* "UPDATES" */,
    NewImage /* "NEW_IMAGE" */,
    OldImage /* "OLD_IMAGE" */,
    NewAndOldImages /* "NEW_AND_OLD_IMAGES" */,

    Unknown = std::numeric_limits<int>::max()
};

enum class EChangefeedFormat {
    Json /* "JSON" */,
    DocumentTableJson /* "DOCUMENT_TABLE_JSON" */,

    Unknown = std::numeric_limits<int>::max()
};

enum class EChangefeedState {
    Enabled,
    Disabled,
    InitialScan,

    Unknown = std::numeric_limits<int>::max()
};

} // namespace NTable
} // namespace NYdb
