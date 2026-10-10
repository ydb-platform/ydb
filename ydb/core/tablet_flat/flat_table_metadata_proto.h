#pragma once

#include "flat_table_metadata.h"

#include <ydb/core/tablet_flat/flat_executor.pb.h>

namespace NKikimr {
namespace NTable {

    inline void MetadataToProto(
            ui32 table,
            const TVector<TVersionedTableMetadata>& metadata,
            NKikimrExecutorFlat::TVersionedTableMetadataState& proto)
    {
        proto.SetTable(table);
        for (const auto& meta : metadata) {
            auto* version = proto.AddVersions();
            meta.Version.ToProto(version->MutableVersion());
            for (const auto& effect : meta.Effects) {
                auto* out = version->AddEffects();
                if (const auto* erase = std::get_if<TTableEraseBoundary>(&effect)) {
                    out->MutableEraseAll()->SetEpoch(erase->Epoch.ToProto());
                }
            }
        }
    }

    inline TVector<TVersionedTableMetadata> MetadataFromProto(
            const NKikimrExecutorFlat::TVersionedTableMetadataState& proto)
    {
        TVector<TVersionedTableMetadata> metadata;
        metadata.reserve(proto.VersionsSize());
        for (const auto& version : proto.GetVersions()) {
            TVersionedTableMetadata meta;
            meta.Version = TRowVersion::FromProto(version.GetVersion());
            meta.Effects.reserve(version.EffectsSize());
            for (const auto& effect : version.GetEffects()) {
                if (effect.HasEraseAll()) {
                    meta.Effects.emplace_back(TTableEraseBoundary{
                        TEpoch(effect.GetEraseAll().GetEpoch()) });
                }
            }
            if (!meta.Effects.empty()) {
                metadata.push_back(std::move(meta));
            }
        }
        return metadata;
    }

}
}
