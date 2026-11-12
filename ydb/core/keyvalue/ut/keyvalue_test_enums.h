#pragma once

namespace NKikimr {

enum class EOverwriteReader {
    Read,
    Range,
    NewRead,
    NewRange,
    Patch,
};

} // namespace NKikimr::NKeyValueTests
