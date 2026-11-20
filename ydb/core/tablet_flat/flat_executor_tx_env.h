#pragma once
#include "defs.h"
#include "flat_exec_seat.h"
#include "flat_transaction_pages.h"
#include "flat_table_misc.h"
#include "flat_part_store.h"
#include "flat_sausagecache.h"
#include <util/generic/hash_set.h>
#include "tablet_flat_executor.h"
#include "flat_executor_snapshot.h"
#include "flat_direct_part_writer.h"
#include "flat_store_hotdog.h"
#include "flat_store_solid.h"
#include <ydb/core/util/pb.h>
#include <ydb/core/tablet_flat/protos/flat_executor.pb.h>
#include <optional>

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TPrivatePageCacheAccess {
        using TBackend = TPrivatePageCache;
        using TSeat = NTabletFlatExecutor::TSeat;
        using TPageCollection = TPrivatePageCache::TPageCollection;

        TPrivatePageCacheAccess(TPrivatePageCache& cache, TSeat& seat)
            : Cache(cache)
            , Seat(seat)
        {}

        static TPageCollection* Locate(const NTable::TPartStore* partStore, NTable::ELargeObj lob, ui64 ref) {
            return partStore->Locate(lob, ref);
        }

        static TPageCollection* GetPageCollection(const NTable::TPartStore* partStore, ui32 slot) {
            return partStore->PageCollections.at(slot).Get();
        }

        static const TLogoBlobID& GetCollectionId(const TPageCollection* pageCollection) noexcept {
            return pageCollection->Id;
        }

        static void CheckMissingPage(
            const NTable::TPageLocation& location, const TPageCollection* pageCollection) noexcept {
            Y_ASSERT(!pageCollection->IsStickyPage(location.Offset));
        }

        template <class TEnv>
        const TSharedData* TryGetPage(const NTable::TPageLocation& location, TPageCollection* pageCollection, TEnv& env)
        {
            auto& pinnedCollection = Seat.Pinned[pageCollection->Id];
            auto* pinnedPage = pinnedCollection.FindPtr(location.Offset);
            if (pinnedPage) {
                // pinned pages do not need to be counted again
                return &pinnedPage->PinnedBody;
            }

            auto sharedBody = Cache.TryGetPage(location.Offset, pageCollection);

            if (!sharedBody) {
                env.ToLoadPage(location, pageCollection);
                return nullptr;
            }

            sharedBody.IncrementFrequency();
            auto emplaced = pinnedCollection.emplace(location.Offset, TPrivatePageCache::TPinnedPage(std::move(sharedBody)));
            Y_ENSURE(emplaced.second);
            auto& pinnedBody = emplaced.first->second.PinnedBody;

            env.Stats.NewlyPinnedPages++;
            if (!pageCollection->IsStickyPage(location.Offset)) {
                env.Stats.NewlyPinnedBytes += pinnedBody.size();
            }

            return &pinnedBody;
        }

    private:
        TPrivatePageCache& Cache;
        TSeat& Seat;
    };

    template <class TPageAccess>
    struct TPageCollectionReadEnvImpl : public ITransactionPages {
        TPageCollectionReadEnvImpl(typename TPageAccess::TBackend& cache, typename TPageAccess::TSeat& seat)
            : Cache(cache, seat)
        {}

        using TPageCollection = typename TPageAccess::TPageCollection;

        using TStats = TPageReadStats;

        const TStats& GetStats() const override {
            return Stats;
        }

    protected: /* NTable::IPages, page collection backend implementation */
        TResult Locate(const TMemTable *memTable, ui64 ref, ui32 tag) override
        {
            return NTable::MemTableRefLookup(memTable, ref, tag);
        }

        TResult Locate(const TPart *part, ui64 ref, ELargeObj lob) override
        {
            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            auto *info = Cache.Locate(partStore, lob, ref);
            const TSharedData* page = TryGetPage(info->GetLocation(ref), info);

            if (!page && ReadMissingReferences) {
                MissingReferencesSize_ += Max<ui64>(1, part->GetPageSize(lob, ref));
            }

            return { !ReadMissingReferences, page };
        }

        const TSharedData* TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override
        {
            Y_ENSURE(!NTable::NPage::IsIndexPage(location.Type),
                "Index page " << location << " is requested as a data page");

            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            return TryGetPage(location, Cache.GetPageCollection(partStore, part->DataSlot(groupId)));
        }

        const TSharedData* TryGetIndexPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override
        {
            Y_ENSURE(location.Type == NTable::NPage::EPage::Undef || NTable::NPage::IsIndexPage(location.Type),
                "Page " << location << " is requested as an index page");

            auto *partStore = CheckedCast<const NTable::TPartStore*>(part);

            return TryGetPage(location, Cache.GetPageCollection(partStore, part->IndexSlot(groupId)));
        }

        void EnableReadMissingReferences() override {
            ReadMissingReferences = true;
        }

        void DisableReadMissingReferences() override {
            ReadMissingReferences = false;
            MissingReferencesSize_ = 0;
        }

        ui64 MissingReferencesSize() const override
        {
            return MissingReferencesSize_;
        }

    private:
        using THashSetOfLocation = THashSet<TPageLocation, NTable::NPage::TPageLocationByOffsetHash>;

        void ToLoadPage(const TPageLocation& location, TPageCollection *pageCollection) {
            auto res = ToLoad[Cache.GetCollectionId(pageCollection)].insert(location);
            if (res.second) {
                Stats.ToLoadPages++;
                Cache.CheckMissingPage(location, pageCollection);
                Stats.ToLoadBytes += location.Size;
            } else {
                Y_ASSERT(res.first->Type == location.Type && res.first->Size == location.Size && res.first->Crc32 == location.Crc32);
            }
        }

        const TSharedData* TryGetPage(const TPageLocation& location, TPageCollection *pageCollection)
        {
            return Cache.TryGetPage(location, pageCollection, *this);
        }

    public:
        TPageRequests ObtainToLoad() override {
            THashMap<TLogoBlobID, TVector<TPageLocation>> result;
            for (auto& [pageCollectionId, pages_] : ToLoad) {
                TVector<TPageLocation> pages(pages_.begin(), pages_.end());
                std::sort(pages.begin(), pages.end());
                result.emplace(pageCollectionId, std::move(pages));
            }
            ToLoad.clear();
            return result;
        }

    protected:
        TPageAccess Cache;

    private:
        friend TPageAccess;

        THashMap<TLogoBlobID, THashSetOfLocation> ToLoad;

        bool ReadMissingReferences = false;
        ui64 MissingReferencesSize_ = 0;

        TStats Stats;
    };

    struct TPageCollectionReadEnv : public TPageCollectionReadEnvImpl<TPrivatePageCacheAccess> {
        using TPageCollectionReadEnvImpl<TPrivatePageCacheAccess>::TPageCollectionReadEnvImpl;
    };

    struct TTransactionEnv : public IExecuting {
        TTransactionEnv(NTable::TDatabase& db)
            : DB(db)
        {}

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

        bool HasChanges() const
        {
            return
                DropSnap
                || MakeSnap
                || LoanBundle
                || LoanTxStatus
                || BorrowUpdates
                || LoanConfirmation
                || AttachedParts;
        }

        void Clear() {
            MakeSnap.clear();
            DropSnap.Reset();
            BorrowUpdates.clear();
            LoanBundle.clear();
            LoanTxStatus.clear();
            LoanConfirmation.clear();
            AttachedParts.clear();
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

        void AttachPart(ui32 tableId, THolder<TDirectPartResult> result) override
        {
            Y_ENSURE(result, "AttachPart called with an empty result");
            AttachedParts.emplace_back(TAttachedPart{ tableId, std::move(result) });
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

        /*_ Directly written parts to attach as bottom layers */

        struct TAttachedPart {
            ui32 TableId;
            THolder<TDirectPartResult> Result;
        };
        TVector<TAttachedPart> AttachedParts;
    };

    struct TPageCollectionTxEnv : public TPageCollectionReadEnv, public TTransactionEnv {
        TPageCollectionTxEnv(NTable::TDatabase& db, TPrivatePageCache& cache, TSeat& seat)
            : TPageCollectionReadEnv(cache, seat)
            , TTransactionEnv(db)
        {}

        using TPageCollectionReadEnv::TPageCollectionReadEnv;

    protected:
        void OnRollbackChanges() override {
            Clear();
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
    };

}
}
