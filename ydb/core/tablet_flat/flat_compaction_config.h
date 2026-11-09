#pragma once

#include "flat_executor_misc.h"
#include "flat_row_scheme.h"

#include <util/generic/hash_set.h>
#include <util/generic/ymath.h>

namespace NKikimr::NTabletFlatExecutor {

    struct TCompactionOutputSettings {
        bool WriteBTreeIndexV1 = true;
        bool WriteBTreeIndexV2 = false;
        bool WriteFlatIndex = true;
        ui32 MaxBlobSize = 8 * 1024 * 1024;
        NUtil::TChannelsShares ChannelsShares;
    };

    // Prepares possible destinations; choosing an output for a key belongs to the router.
    template<class TGroupForChannel>
    void PrepareCompactionOutputs(TCompactCfg& conf, const NTable::TScheme::TTableInfo& table,
            const NTable::TRowScheme& scheme, const NTable::TSubset& subset,
            const TCompactionOutputSettings& settings, TGroupForChannel groupForChannel) {
        using NTable::TCompactionParams;
        using NTable::NPage::ECache;

        Y_ENSURE(conf.Outputs.empty(), "Compaction outputs are already prepared");
        Y_ENSURE(!scheme.Families.empty(), "Compaction requires column groups");
        const auto& params = *conf.Params;
        if (params.Context) {
            Y_ENSURE(!conf.IsFulltextCompact,
                "Prepared compaction context is not supported for fulltext");
            params.Context->ValidateSubset(subset);
            Y_ENSURE(!params.Context->RequiresTierAware()
                    || params.Mode == TCompactionParams::EMode::TierAware,
                "Closed compaction inputs span multiple tiers");
            Y_ENSURE(!params.UnderlayMask,
                "Prepared compaction context supplies independent underlay cursors");
            Y_ENSURE(!params.Context->Epochs || conf.Epoch == params.Context->Epochs->Max,
                "Compaction writer epoch differs from its prepared interval");
        }
        TVector<ui32> tiers;
        switch (params.Mode) {
            case TCompactionParams::EMode::Regular:
                if (params.Context) {
                    for (ui32 tier : params.Context->InputTiers) {
                        Y_ENSURE(tier == params.TierId,
                            "Regular compaction must stay within its input tier");
                    }
                }
                tiers.push_back(params.TierId);
                break;
            case TCompactionParams::EMode::TierAware: {
                Y_ENSURE(!conf.IsFulltextCompact, "Tier-aware fulltext compaction is not supported");
                Y_ENSURE(!params.UnderlayMask && !params.SplitKeys,
                    "Tier-aware outputs require independent key-space and split-key cursors");
                THashSet<ui32> seen;
                auto addTier = [&](ui32 tierId) {
                    if (seen.insert(tierId).second) {
                        tiers.push_back(tierId);
                    }
                };
                // FinalParts and keys matching no rule belong to the default tier.
                addTier(0);
                addTier(params.TierId);
                if (params.Context) {
                    for (ui32 tier : params.Context->InputTiers) {
                        addTier(tier);
                    }
                }
                for (const auto& generation : table.CompactionPolicy->Generations) {
                    addTier(generation.TierId.value_or(0));
                }
                break;
            }
            default:
                Y_ENSURE(false, "Unknown compaction mode");
        }

        static const NTable::TScheme::TFamily defaultFamily;
        const auto& leader = table.Families.ValueRef(0, defaultFamily);
        const auto* leaderRoom = table.Rooms.FindPtr(leader.Room);
        Y_ENSURE(leaderRoom, "Cannot find room " << leader.Room << " in table " << table.Id);
        const auto& policy = *table.CompactionPolicy;
        const bool writeBTree = settings.WriteBTreeIndexV1 || settings.WriteBTreeIndexV2;
        const ECache cache = params.KeepInCache ? ECache::Once : ECache::None;

        for (ui32 tierId : tiers) {
            const auto* tier = tierId ? table.Families.FindPtr(tierId) : nullptr;
            Y_ENSURE(!tierId || tier, "Cannot find tier family " << tierId << " in table " << table.Id);
            auto& output = conf.Outputs.emplace_back();
            output.OutputId = ui32(conf.Outputs.size() - 1);
            output.TierId = tierId;
            output.Layout.Final = params.IsFinal;
            if (params.Context) {
                auto garbage = NTable::MakeCompactionGarbageConf(
                    scheme.Keys, params.Context->Remaining,
                    policy.KeepEraseMarkers || (params.Tiering && params.Tiering->HasUnknownRemaining));
                output.Layout.Final = garbage.Final;
                output.OwnedUnderlayMask = std::move(garbage.UnderlayMask);
            }
            output.Layout.WriteBTreeIndexV1 = settings.WriteBTreeIndexV1;
            output.Layout.WriteBTreeIndexV2 = settings.WriteBTreeIndexV2;
            output.Layout.WriteFlatIndex = !settings.WriteBTreeIndexV2 && settings.WriteFlatIndex;
            output.Layout.MaxRows = subset.MaxRows();
            output.Layout.MinRowVersion = subset.MinRowVersion();
            if (conf.RemovedRowVersions) {
                output.Layout.MinRowVersion = conf.RemovedRowVersions.AdjustDown(output.Layout.MinRowVersion);
            }
            for (const auto& prefix : table.ByKeyFilterPrefixes) {
                output.Layout.ByKeyFilterPrefixes.push_back({prefix.PrefixLength, prefix.FalsePositiveProbability});
            }
            output.Layout.UnderlayMask = params.Context
                ? output.OwnedUnderlayMask.Get() : params.UnderlayMask.Get();
            output.Layout.SplitKeys = params.SplitKeys.Get();
            output.Layout.Groups.resize(scheme.Families.size());
            output.Writer.Groups.resize(scheme.Families.size());
            output.Writer.StickyFlatIndex = !writeBTree;
            output.Writer.WriteBTreeIndexV1 = settings.WriteBTreeIndexV1;
            output.Writer.WriteBTreeIndexV2 = settings.WriteBTreeIndexV2;
            output.Writer.ChannelsShares = settings.ChannelsShares;
            if (tier) {
                output.Layout.Layout.KeyIndexInDescriptor = tier->KeyIndexInDescriptor;
                output.Layout.Layout.GroupIndexesInDescriptor = tier->GroupIndexesInDescriptor;
            }

            auto addChannel = [&](ui8 channel) {
                output.Writer.Slots.emplace_back(channel, groupForChannel(channel));
            };
            ui8 descriptorChannel = leaderRoom->Main;
            for (size_t group = 0; group < scheme.Families.size(); ++group) {
                // Preserve the missing-family fallback of ordinary column-group compaction (KIKIMR-17222).
                const auto& family = tier ? *tier : table.Families.ValueRef(scheme.Families[group], defaultFamily);
                const auto* room = table.Rooms.FindPtr(family.Room);
                Y_ENSURE(room, "Cannot find room " << family.Room << " in table " << table.Id);
                auto& pageGroup = output.Layout.Groups[group];
                pageGroup.Codec = family.Codec;
                pageGroup.PageSize = policy.MinDataPageSize;
                pageGroup.BTreeIndexNodeTargetSize = policy.MinBTreeIndexNodeSize;
                pageGroup.BTreeIndexNodeKeysMin = policy.MinBTreeIndexNodeKeys;
                auto& writerGroup = output.Writer.Groups[group];
                writerGroup.Channel = room->Main;
                writerGroup.Cache = Max(family.Cache, cache);
                writerGroup.CacheMode = family.CacheMode;
                writerGroup.MaxBlobSize = settings.MaxBlobSize;
                addChannel(room->Main);
                if (group == 0) {
                    if (!tier) {
                        descriptorChannel = room->Main;
                    }
                    output.Layout.SmallEdge = family.Small;
                    output.Layout.LargeEdge = family.Large;
                    // A TAC must rewrite external references into the destination tier.
                    output.MaterializeExtEdge = params.Mode == TCompactionParams::EMode::TierAware
                        ? Max<ui32>() : family.Large;
                    output.Writer.BlobsChannels = room->Blobs;
                    output.Writer.OuterChannel = room->Outer;
                    for (ui8 channel : room->Blobs) {
                        addChannel(channel);
                    }
                    addChannel(room->Outer);
                }
            }

            // Transaction status stays on the extra channel for every output.
            addChannel(output.Writer.ExtraChannel);
            if (NWriter::ApplyDescriptorChannel(output.Layout, output.Writer, descriptorChannel)) {
                addChannel(descriptorChannel);
            }
            Y_ENSURE(output.Layout.Layout.SeparateDescriptorCollection
                    || (output.Layout.Layout.KeyIndexInDescriptor && output.Layout.Layout.GroupIndexesInDescriptor),
                "Compaction output index placement requires a separate descriptor collection");
        }
    }

}
