#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <optional>

namespace NKikimr::NBackup {

struct TTableChecksum {
    static constexpr ui32 Version = 1;

    struct TError {
        TString Code;
        TString Message;

        bool operator==(const TError&) const = default;
    };

    ui64 Count = 0;
    TString Hash; // Uppercase hexadecimal representation
    TString Algorithm;
    std::optional<TError> Error = std::nullopt;

    bool operator==(const TTableChecksum&) const = default;

    TString ToString() const;
};

} // namespace NKikimr::NBackup
