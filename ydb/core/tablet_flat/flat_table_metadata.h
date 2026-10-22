#pragma once

#include "defs.h"

#include <ydb/core/base/row_version.h>

#include <optional>
#include <variant>
#include <vector>

namespace NKikimr {
namespace NTable {

    struct TTableEraseBoundary {
        TEpoch Epoch;

        friend bool operator==(const TTableEraseBoundary& a, const TTableEraseBoundary& b) {
            return a.Epoch == b.Epoch;
        }
    };

    using TTableMetadataEffect = std::variant<TTableEraseBoundary>;

    struct TVersionedTableMetadata {
        TRowVersion Version;
        std::vector<TTableMetadataEffect> Effects;
    };

    inline std::optional<TRowVersion> SourceHiddenSince(
            TEpoch epoch,
            const std::optional<TRowVersion>& stamp,
            const TVector<TVersionedTableMetadata>& metadata)
    {
        // Metadata is ordered by version, so the first matching erase is earliest.
        for (const auto& meta : metadata) {
            for (const auto& effect : meta.Effects) {
                if (const auto* erase = std::get_if<TTableEraseBoundary>(&effect); erase && epoch < erase->Epoch) {
                    return stamp ? Min(*stamp, meta.Version) : meta.Version;
                }
            }
        }
        return stamp;
    }

    inline bool IsHiddenAt(
            TEpoch epoch,
            const std::optional<TRowVersion>& stamp,
            TRowVersion snapshot,
            const TVector<TVersionedTableMetadata>& metadata)
    {
        const auto hidden = SourceHiddenSince(epoch, stamp, metadata);
        return hidden && *hidden <= snapshot;
    }

}
}
