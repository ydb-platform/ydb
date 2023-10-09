#pragma once

#include "flat_boot_blobs.h"
#include "flat_store_hotdog.h"
#include "flat_store_solid.h"

#include <ydb/core/tablet_flat/flat_executor.pb.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    struct TSwitch {

        struct TBundle {
            TVector<NPageCollection::TLargeGlobId> LargeGlobIds;
            TString Legacy;
            TString Opaque;
            TVector<TString> Deltas;
            NTable::TEpoch Epoch = NTable::TEpoch::Max();
            bool Load = true;

            template<class TLookup>
            void AddGroups(const TLookup &lookup)
            {
                /* replaced by TLargeGlobId, required for (Evolution < 2) */

                for (auto &largeGlobId : LargeGlobIds) {
                    if (largeGlobId.Group == NPageCollection::TLargeGlobId::InvalidGroup) {
                        largeGlobId.Group = lookup(largeGlobId.Lead);
                    }
                }
            }
        };

        struct TChange {
            TLogoBlobID Label;
            TString Legacy;
            TString Opaque;
        };

        struct TDelta {
            TLogoBlobID Label;
            TString Delta;
        };

        struct TMove {
            TLogoBlobID Label;
            NTable::TEpoch RebasedEpoch = NTable::TEpoch::Max();
            ui32 SourceTable = Max<ui32>();
        };

        struct TCompactionChanges {
            NKikimrSchemeOp::ECompactionStrategy Strategy = NKikimrSchemeOp::CompactionStrategyUnset;
            THashMap<ui64, TString> KeyValues;
        };

        struct TRemovedRowVersions {
            TRowVersion Lower;
            TRowVersion Upper;
        };

        struct TTxStatus {
            NPageCollection::TLargeGlobId DataId;
            NTable::TEpoch Epoch = NTable::TEpoch::Max();
            bool Load = true;
        };

        TSwitch() { }
        TSwitch(NPageCollection::TLargeGlobId largeGlobId) : LargeGlobId(largeGlobId) { }

        bool Loaded() const noexcept
        {
            return Table != Max<ui32>();
        }

        void Init(const NKikimrExecutorFlat::TTablePartSwitch &proto)
        {
            InitTable(proto.GetTableId());

            if (proto.HasIntroducedParts()) {
                Init(proto.GetIntroducedParts());
            }

            Changes.reserve(proto.ChangedBundlesSize());
            for (const auto &one : proto.GetChangedBundles()) {
                AddChange(one);
            }

            Deltas.reserve(proto.BundleDeltasSize());
            for (const auto &one : proto.GetBundleDeltas()) {
                AddDelta(one);
            }

            Moves.reserve(proto.BundleMovesSize());
            for (const auto &one : proto.GetBundleMoves()) {
                AddMove(one);
            }

            Leaving.reserve(proto.LeavingBundlesSize());
            for (const auto &one : proto.GetLeavingBundles()) {
                Leaving.push_back(LogoBlobIDFromLogoBlobID(one));
            }

            if (proto.HasCompactionChanges()) {
                Init(proto.GetCompactionChanges());
            }

            if (proto.HasRowVersionChanges()) {
                Init(proto.GetRowVersionChanges());
            }

            if (proto.HasIntroducedTxStatus()) {
                Init(proto.GetIntroducedTxStatus());
            }

            LeavingTxStatus.reserve(proto.LeavingTxStatusSize());
            for (const auto &one : proto.GetLeavingTxStatus()) {
                LeavingTxStatus.push_back(LogoBlobIDFromLogoBlobID(one));
            }
        }

        void Init(const NKikimrExecutorFlat::TLogTableSnap &snap)
        {
            InitTable(snap.GetTable());

            if (snap.HasCompactionLevel()) {
                Level = snap.GetCompactionLevel();
            }

            if (snap.DbPartMetaInfoBodySize() > 0) {
                /* Legacy mode, may hold only one page collection per entity */

                AddLegacyLargeGlobId(Bundles.emplace_back(), snap.GetDbPartMetaInfoBody());
            }

            if (snap.BundlesSize() > 0) {
                Bundles.reserve(Bundles.size() + snap.BundlesSize());

                for (auto &bundle: snap.GetBundles()) {
                    auto &one = Bundles.emplace_back();

                    one.LargeGlobIds.reserve(bundle.PageCollectionsSize());

                    for (auto &pageCollection: bundle.GetPageCollections()) {
                        if (pageCollection.HasLargeGlobId()) {
                            auto largeGlobId = TLargeGlobIdProto::Get(pageCollection.GetLargeGlobId());

                            one.LargeGlobIds.emplace_back(largeGlobId);
                        } else {
                            AddLegacyLargeGlobId(one, pageCollection.GetMetaId());
                        }
                    }

                    Y_ABORT_UNLESS(one.LargeGlobIds.size(), "Part bundle has no page collections");

                    if (bundle.HasLegacy())
                        one.Legacy = bundle.GetLegacy();

                    if (bundle.HasOpaque())
                        one.Opaque = bundle.GetOpaque();

                    if (bundle.HasEpoch())
                        one.Epoch = NTable::TEpoch(bundle.GetEpoch());
                }
            }
        }

        void Init(const NKikimrExecutorFlat::TLogTxStatusSnap &snap)
        {
            InitTable(snap.GetTable());

            TxStatus.reserve(TxStatus.size() + snap.TxStatusSize());
            for (const auto &x : snap.GetTxStatus()) {
                auto &one = TxStatus.emplace_back();
                one.DataId = TLargeGlobIdProto::Get(x.GetDataId());
                one.Epoch = NTable::TEpoch(x.GetEpoch());
            }
        }

        void Init(const NKikimrExecutorFlat::TCompactionState &proto)
        {
            InitTable(proto.GetTable());

            CompactionChanges.Strategy = proto.GetStrategy();
            for (const auto& kv : proto.GetKeyValues()) {
                CompactionChanges.KeyValues[kv.GetKey()] = kv.GetValue();
            }
        }

        void Init(const NKikimrExecutorFlat::TRowVersionState &proto)
        {
            InitTable(proto.GetTable());

            RemovedRowVersions.reserve(proto.RemovedRangesSize());
            for (const auto& range : proto.GetRemovedRanges()) {
                RemovedRowVersions.push_back({
                    TRowVersion(range.GetLower().GetStep(), range.GetLower().GetTxId()),
                    TRowVersion(range.GetUpper().GetStep(), range.GetUpper().GetTxId()),
                });
            }
        }

        void AddChange(const NKikimrExecutorFlat::TBundleChange &change)
        {
            auto &c = Changes.emplace_back();

            c.Label = LogoBlobIDFromLogoBlobID(change.GetLabel());

            if (change.HasLegacy())
                c.Legacy = change.GetLegacy();

            if (change.HasOpaque())
                c.Opaque = change.GetOpaque();
        }

        void AddDelta(const NKikimrExecutorFlat::TBundleDelta &delta)
        {
            auto &d = Deltas.emplace_back();

            d.Label = LogoBlobIDFromLogoBlobID(delta.GetLabel());

            if (delta.HasDelta())
                d.Delta = delta.GetDelta();
        }

        void AddMove(const NKikimrExecutorFlat::TBundleMove &proto)
        {
            auto &m = Moves.emplace_back();

            m.Label = LogoBlobIDFromLogoBlobID(proto.GetLabel());

            if (proto.HasRebasedEpoch())
                m.RebasedEpoch = NTable::TEpoch(proto.GetRebasedEpoch());

            if (proto.HasSourceTable())
                m.SourceTable = proto.GetSourceTable();
        }

    private:
        void AddLegacyLargeGlobId(TBundle &bundle, const TLargeGlobIdProto::TRep &rep)
        {
            auto lookup = [](const TLogoBlobID&) {
                return NPageCollection::TLargeGlobId::InvalidGroup; /* Don't know now group id */
            };

            bundle.LargeGlobIds.emplace_back(TLargeGlobIdProto::Get(rep, lookup));
        }

        void InitTable(ui32 table) {
            Y_ABORT_UNLESS(table != Max<ui32>(), "Invalid table id in switch");
            if (Table == Max<ui32>()) {
                Table = table;
            } else {
                Y_ABORT_UNLESS(Table == table,
                    "Inconsistent table id in switch (have %" PRIu32 ", new %" PRIu32 ")",
                    Table, table);
            }
        }

    public:
        const NPageCollection::TLargeGlobId LargeGlobId;  /* Switch blob LargeGlobId */

        ui32 Table = Max<ui32>();
        ui32 Level = 255;

        TVector<TBundle> Bundles;
        TVector<TBundle> MovedBundles;
        TVector<TChange> Changes;
        TVector<TDelta> Deltas;
        TVector<TMove> Moves;
        TVector<TLogoBlobID> Leaving;
        TVector<TTxStatus> TxStatus;
        TVector<TLogoBlobID> LeavingTxStatus;

        TCompactionChanges CompactionChanges;

        TVector<TRemovedRowVersions> RemovedRowVersions;
    };
}
}
}
