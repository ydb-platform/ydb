#pragma once
#include "defs.h"
#include "flat_exec_seat.h"
#include "flat_table_misc.h"
#include "flat_part_store.h"
#include "flat_store_hotdog.h"
#include "flat_store_solid.h"
#include "flat_sausagecache.h"
#include "tablet_flat_executor.h"
#include "flat_executor_snapshot.h"
#include <ydb/core/util/pb.h>
#include <util/generic/hash_set.h>
#include <ydb/core/tablet_flat/flat_executor.pb.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TPageCollectionReadEnv : public NTable::IPages {
        TPageCollectionReadEnv(TPrivatePageCache& cache, TSeat& seat)
            : Cache(cache)
            , Seat(seat)
        { }

        using TInfo = TPrivatePageCache::TInfo;

        struct TStats {
            size_t NewlyPinnedPages = 0;
            ui64 NewlyPinnedBytes = 0;
    
            size_t ToLoadPages = 0;
            ui64 ToLoadBytes = 0;
        };

        const TStats& GetStats() const { return Stats; }

    protected: /* NTable::IPages, page collection backend implementation */
        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) override
        {
            return NTable::MemTableRefLookup(memTable, ref, tag);
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) override
        {
            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            const TSharedData* page = Lookup(ref, partStore->Locate(lob, ref));

            if (!page && ReadMissingReferences) {
                MissingReferencesSize_ += Max<ui64>(1, part->GetPageSize(lob, ref));
            }

            return { !ReadMissingReferences, page };
        }

        const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override
        {
            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            return Lookup(pageId, partStore->PageCollections.at(groupId.Index).Get());
        }

        void EnableReadMissingReferences() {
            ReadMissingReferences = true;
        }

        void DisableReadMissingReferences() {
            ReadMissingReferences = false;
            MissingReferencesSize_ = 0;
        }

        ui64 MissingReferencesSize() const
        { 
            return MissingReferencesSize_;
        }

    private:
        void ToLoadPage(TPageId pageId, TInfo *info) {
            if (ToLoad[info->Id].insert(pageId).second) {
                Stats.ToLoadPages++;
                Y_ASSERT(!info->IsStickyPage(pageId));
                Stats.ToLoadBytes += info->GetPageSize(pageId);
            }
        }

        const TSharedData* Lookup(TPageId pageId, TInfo *info)
        {
            auto& pinnedCollection = Seat.Pinned[info->Id];
            auto* pinnedPage = pinnedCollection.FindPtr(pageId);
            if (pinnedPage) {
                // pinned pages do not need to be counted again
                return &pinnedPage->PinnedBody;
            }

            auto sharedBody = Cache.TryGetPage(pageId, info);

            if (!sharedBody) {
                ToLoadPage(pageId, info);
                return nullptr;
            }

            auto emplaced = pinnedCollection.emplace(pageId, TPrivatePageCache::TPinnedPage(std::move(sharedBody)));
            Y_ENSURE(emplaced.second);
            auto& pinnedBody = emplaced.first->second.PinnedBody;

            Stats.NewlyPinnedPages++;
            if (!info->IsStickyPage(pageId)) {
                Stats.NewlyPinnedBytes += pinnedBody.size();
            }
            
            return &pinnedBody;
        }

    public:
        auto ObtainToLoad() {
            THashMap<TLogoBlobID, TVector<TPageId>> result;
            for (auto& [pageCollectionId, pages_] : ToLoad) {
                TVector<TPageId> pages(pages_.begin(), pages_.end());
                std::sort(pages.begin(), pages.end());
                result.emplace(pageCollectionId, std::move(pages));
            }
            ToLoad.clear();
            return result;
        }
        
        auto ObtainSharedCacheTouches() {
            THashMap<TLogoBlobID, THashSet<TPageId>> result;
            for (const auto& [pageCollectionId, pages] : Seat.Pinned) {
                if (Cache.FindPageCollection(pageCollectionId)) {
                    auto& touches = result[pageCollectionId];
                    for (const auto& [pageId, pinnedPageRef] : pages) {
                        touches.insert(pageId);
                    }
                }
            }
            return result;
        }

    private:
        TPrivatePageCache& Cache;
        TSeat& Seat;

        THashMap<TLogoBlobID, THashSet<TPageId>> ToLoad;
    
        bool ReadMissingReferences = false;
        ui64 MissingReferencesSize_ = 0;

        TStats Stats;
    };

    struct TPageCollectionTxEnv : public TPageCollectionReadEnv, public IExecuting {
        TPageCollectionTxEnv(NTable::TDatabase& db, TPrivatePageCache& cache, TSeat& seat)
            : TPageCollectionReadEnv(cache, seat)
            , DB(db)
        { }

        using TLogoId = TLogoBlobID;

        struct TBorrowSnap {
            TIntrusivePtr<TTableSnapshotContext> SnapContext;
        };

        struct TBorrowUpdate {
            TDeque<ui64> StoppedLoans;
        };

        struct TLoanConfirmation {
            const TLogoId BorrowId;
        };

        struct TLoanBundle {
            TLoanBundle(ui32 sourceTableId, ui32 localTableId, ui64 lender, NTable::TPartComponents&& pc)
                : SourceTableId(sourceTableId)
                , LocalTableId(localTableId)
                , Lender(lender)
                , PartComponents(std::move(pc))
            {}

            const ui32 SourceTableId;
            const ui32 LocalTableId;
            const ui64 Lender;

            NTable::TPartComponents PartComponents;
        };

        struct TLoanTxStatus {
            TLoanTxStatus(ui32 sourceTableId, ui32 localTableId, ui64 lender,
                          const NPageCollection::TLargeGlobId& dataId, NTable::TEpoch epoch,
                          const TString& data)
                : SourceTableId(sourceTableId)
                , LocalTableId(localTableId)
                , Lender(lender)
                , DataId(dataId)
                , Epoch(epoch)
                , Data(data)
            {}

            const ui32 SourceTableId;
            const ui32 LocalTableId;
            const ui64 Lender;
            const NPageCollection::TLargeGlobId DataId;
            const NTable::TEpoch Epoch;
            const TString Data;
        };

        struct TSnapshot {
            TVector<TIntrusivePtr<TTableSnapshotContext>> Context;
            std::optional<NTable::TEpoch> Epoch;
        };

        using TPageCollectionReadEnv::TPageCollectionReadEnv;

        bool HasChanges() const
        {
            return
                DropSnap
                || MakeSnap
                || LoanBundle
                || LoanTxStatus
                || BorrowUpdates
                || LoanConfirmation;
        }

    protected:
        void OnRollbackChanges() override {
            MakeSnap.clear();
            DropSnap.Reset();
            BorrowUpdates.clear();
            LoanBundle.clear();
            LoanTxStatus.clear();
            LoanConfirmation.clear();
        }

    protected: /* IExecuting, tx stage func implementation */
        void MakeSnapshot(TIntrusivePtr<TTableSnapshotContext> snap) override;

        void DropSnapshot(TIntrusivePtr<TTableSnapshotContext> snap) override
        {
            Y_ENSURE(!DropSnap, "only one snapshot per transaction");

            DropSnap.Reset(new TBorrowSnap{ snap });
        }

        void MoveSnapshot(const TTableSnapshotContext &snap, ui32 src, ui32 dst) override
        {
            snap.Impl->Moved(src, dst);
        }

        void ClearSnapshot(const TTableSnapshotContext &snap) override
        {
            snap.Impl->Clear();
        }

        // NOTE: It's allowed to add parts in the same Tx where the table gets created (and is not visible yet)
        void LoanTable(ui32 tableId, const TString &raw) override
        {
            TProtoBox<NKikimrExecutorFlat::TDatabaseBorrowPart> proto(raw);

            const ui64 lender = proto.GetLenderTablet();
            const ui32 source = proto.GetSourceTable();

            for (auto &part : proto.GetParts()) {
                Y_ENSURE(part.HasBundle(), "Cannot find attached hotdogs in borrow");

                LoanBundle.emplace_back(new TLoanBundle(source, tableId, lender,
                        TPageCollectionProtoHelper::MakePageCollectionComponents(part.GetBundle(), /* unsplit */ true)));
            }

            for (auto &part : proto.GetTxStatusParts()) {
                LoanTxStatus.emplace_back(new TLoanTxStatus(
                    source, tableId, lender,
                    TLargeGlobIdProto::Get(part.GetDataId()),
                    NTable::TEpoch(part.GetEpoch()),
                    part.GetData()));
            }
        }

        void CleanupLoan(const TLogoId &bundle, ui64 from) override
        {
            Y_ENSURE(!DropSnap, "must not drop snapshot and update loan in same transaction");
            BorrowUpdates[bundle].StoppedLoans.push_back(from);
        }

        void ConfirmLoan(const TLogoId &bundle, const TLogoId &borrow) override
        {
            LoanConfirmation.insert(std::make_pair(bundle, TLoanConfirmation{borrow}));
        }

        void EnableReadMissingReferences() override
        {
            TPageCollectionReadEnv::EnableReadMissingReferences();
        }

        void DisableReadMissingReferences() override
        {
            TPageCollectionReadEnv::DisableReadMissingReferences();
        }

        ui64 MissingReferencesSize() const override
        {
            return TPageCollectionReadEnv::MissingReferencesSize();
        }
    protected:
        NTable::TDatabase& DB;

    public:
        /*_ Pending database snapshots      */

        TMap<ui32, TSnapshot> MakeSnap;

        /*_ In tx tables borrow proto API   */

        THolder<TBorrowSnap> DropSnap;
        THashMap<TLogoId, TBorrowUpdate> BorrowUpdates;
        TVector<THolder<TLoanBundle>> LoanBundle;
        TVector<THolder<TLoanTxStatus>> LoanTxStatus;
        THashMap<TLogoId, TLoanConfirmation> LoanConfirmation;
    };

}
}
