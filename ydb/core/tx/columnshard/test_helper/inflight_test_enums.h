#pragma once

namespace NKikimr::NColumnShard {

enum class EImportInflightTestCase {
    Read,
    Write,
    SaveProgress,
};

enum class EExportInflightTestCase {
    Read,
    Write,
    SaveCursor,
    CancelledRead,
};

}   // namespace NKikimr::NColumnShard
