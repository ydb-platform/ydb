#pragma once

#include "flat_executor_misc.h"
#include "flat_part_writer.h"
#include "flat_writer_bundle.h"

#include <util/generic/hash_set.h>

namespace NKikimr::NTabletFlatExecutor {

    class ICompactionRouter {
    public:
        virtual ~ICompactionRouter() = default;

        // Called once per key. Returns an index into TCompactCfg::Outputs.
        // The key cells are only valid for the duration of this call.
        virtual ui32 RouteKey(TArrayRef<const TCell> key) = 0;
    };

    class TDefaultCompactionRouter final : public ICompactionRouter {
    public:
        ui32 RouteKey(TArrayRef<const TCell>) override {
            return 0;
        }
    };

    // All outputs share cookie allocators, but retain independent writers and
    // bundles. BlobStorage scheduling and atomic publication belong to the caller.
    class TCompactionOutputs {
    public:
        struct TResult : NWriter::TBundle::TResult {
            ui32 OutputId;
        };

        TCompactionOutputs(const TLogoBlobID& mask, TCompactCfg& conf,
                THolder<ICompactionRouter> router)
            : Conf(conf)
            , Banks(mask, CollectSlots(conf))
            , Router(std::move(router))
        {
            Y_ENSURE(Router, "Compaction requires a router");
            Y_ENSURE(!Conf.IsFulltextCompact || Conf.Outputs.size() == 1,
                "Multiple outputs with fulltext compaction are not supported");

            THashSet<ui32> ids;
            THashSet<NTable::NPage::ISplitKeys*> splitKeys;
            for (const auto& output : Conf.Outputs) {
                Y_ENSURE(ids.insert(output.OutputId).second, "Duplicate compaction output id");
                Y_ENSURE(!output.Layout.SplitKeys || splitKeys.insert(output.Layout.SplitKeys).second,
                    "Compaction outputs must not share a stateful split-key cursor");
                Y_ENSURE(output.Writer.Groups.size() == output.Layout.Groups.size()
                        + output.Layout.Layout.SeparateDescriptorCollection,
                    "Compaction output page collection groups do not match its layout");
                // Validate that every channel used by this output is present in the shared banks.
                for (const auto& group : output.Writer.Groups) {
                    (void)Banks.Data.GroupBy(group.Channel);
                }
                (void)Banks.Data.GroupBy(output.Writer.OuterChannel);
                (void)Banks.Data.GroupBy(output.Writer.ExtraChannel);
                Y_ENSURE(!output.Writer.BlobsChannels.empty(), "Compaction output has no blob channels");
                for (auto channel : output.Writer.BlobsChannels) {
                    (void)Banks.Data.GroupBy(channel);
                }
                States.emplace_back().Bundle = MakeHolder<NWriter::TBundle>(&Banks, output.Writer);
            }
        }

        void Start(const NTable::TRowScheme& scheme) {
            Y_ENSURE(!Started, "Compaction outputs started twice");
            auto partScheme = MakeIntrusive<NTable::TPartScheme>(scheme.Cols);
            const auto tags = scheme.Tags();
            for (size_t i = 0; i < States.size(); ++i) {
                Y_ENSURE(Conf.Outputs[i].Layout.Groups.size() == partScheme->Groups.size(),
                    "Compaction output column groups do not match the row scheme");
                States[i].Writer = new NTable::TPartWriter(partScheme, tags,
                    *States[i].Bundle, Conf.Outputs[i].Layout, Conf.Epoch);
            }
            Started = true;
        }

        // Borrowed writers remain owned by this object for its lifetime.
        NTable::TPartWriter* GetWriter(ui32 index) const {
            Y_ENSURE(Started && !Finished, "Compaction writer is not active");
            return States.at(index).Writer.Get();
        }

        void BeginKey(TArrayRef<const TCell> key) {
            Y_ENSURE(Started && !Finished && !KeyActive, "Unexpected compaction key start");
            CurrentOutput = Router->RouteKey(key);
            Y_ENSURE(CurrentOutput < States.size(), "Compaction router returned an invalid output index");
            auto* writer = GetWriter(CurrentOutput);
            writer->BeginKey(key);
            KeyActive = true;
        }

        void AddKeyLock(NTable::ELockMode mode, ui64 txId) {
            CurrentWriter().AddKeyLock(mode, txId);
        }

        void AddKeyDelta(const NTable::TRowState& row, ui64 txId) {
            CurrentWriter().AddKeyDelta(row, txId);
        }

        void AddKeyVersion(const NTable::TRowState& row, TRowVersion version) {
            CurrentWriter().AddKeyVersion(row, version);
        }

        ui32 EndKey() {
            auto written = CurrentWriter().EndKey();
            KeyActive = false;
            return written;
        }

        TVector<TResult> Finish(NTable::TWriteStats& total) {
            Y_ENSURE(Started && !Finished && !KeyActive, "Unexpected compaction outputs finish");
            TVector<TResult> results;
            for (size_t i = 0; i < States.size(); ++i) {
                const auto stats = States[i].Writer->Finish();
                total.Rows += stats.Rows;
                total.Bytes += stats.Bytes;
                total.Coded += stats.Coded;
                total.Drops += stats.Drops;
                total.Parts += stats.Parts;
                total.HiddenRows += stats.HiddenRows;
                total.HiddenDrops += stats.HiddenDrops;

                auto parts = States[i].Bundle->Results();
                Y_ENSURE(stats.Parts == parts.size());
                for (auto& part : parts) {
                    results.push_back({std::move(part), Conf.Outputs[i].OutputId});
                }
            }
            Finished = true;
            return results;
        }

        TVector<NPageCollection::TGlob> GetBlobsToSave() {
            TVector<NPageCollection::TGlob> blobs;
            for (auto& state : States) {
                for (auto& blob : state.Bundle->GetBlobsToSave()) {
                    blobs.push_back(std::move(blob));
                }
            }
            return blobs;
        }

        NPageCollection::TLargeGlobId WriteExtra(TArrayRef<const char> data) {
            // Transaction status belongs to the compaction, even when output 0 is empty.
            return States.front().Bundle->WriteExtra(data);
        }

        void UpdateChannelsShares(ui32 channel, float share) {
            for (auto& output : Conf.Outputs) {
                output.Writer.ChannelsShares.Update(channel, share);
            }
        }

        ui32 GetScanLargeEdge() const {
            // TODO: https://github.com/ydb-platform/ydb/issues/57850
            // Select the output before materialization. Until then read enough
            // for every output and keep materializing all outer values. Preserve
            // comparison against physical blob sizes, including their wrappers.
            ui32 edge = 0;
            for (const auto& output : Conf.Outputs) {
                edge = Max(edge, output.MaterializeExtEdge);
            }
            return edge;
        }

    private:
        NTable::TPartWriter& CurrentWriter() const {
            Y_ENSURE(KeyActive, "Compaction key is not active");
            return *States[CurrentOutput].Writer;
        }

        static TVector<NWriter::TConf::TSlot> CollectSlots(const TCompactCfg& conf) {
            Y_ENSURE(!conf.Outputs.empty(), "Compaction requires at least one output");
            TVector<NWriter::TConf::TSlot> slots;
            for (const auto& output : conf.Outputs) {
                slots.insert(slots.end(), output.Writer.Slots.begin(), output.Writer.Slots.end());
            }
            // TBanks deduplicates channels and rejects conflicting group bindings.
            return slots;
        }

        struct TState {
            THolder<NWriter::TBundle> Bundle;
            TIntrusivePtr<NTable::TPartWriter> Writer;
        };

        TCompactCfg& Conf;
        NWriter::TBanks Banks;
        TVector<TState> States;
        THolder<ICompactionRouter> Router;
        ui32 CurrentOutput = 0;
        bool Started = false;
        bool Finished = false;
        bool KeyActive = false;
    };

}
