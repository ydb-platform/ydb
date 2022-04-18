#pragma once
#include "defs.h"
#include "flat_fwd_sieve.h"
#include "flat_exec_commit.h"
#include "flat_executor_tx_env.h"
#include "flat_sausage_slicer.h"
#include <ydb/core/tablet_flat/flat_executor.pb.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

class TExecutorBorrowLogic {
    const ui64 SelfTabletId;
    TAutoPtr<NPageCollection::TSteppedCookieAllocator> Cookies;
    NPageCollection::TSlicer Slicer;
    ui64 KeepBytes = 0;

    struct TBorrowedPartInfo {
        TLogoBlobID BorrowBlobId;

        struct { // out
            TVector<ui64> FullBorrow;
            TVector<TLogoBlobID> Keep;
            ui64 KeepBytes = 0;

            bool HasKeep(const TLogoBlobID &blob) const noexcept
            {
                return std::find(Keep.begin(), Keep.end(), blob) != Keep.end();
            }

            ui64 UpdateKeepBytes() {
                ui64 oldKeepBytes = std::exchange(KeepBytes, 0);
                for (const auto &id : Keep) {
                    KeepBytes += id.BlobSize();
                }
                return KeepBytes - oldKeepBytes;
            }

            // todo: per-borrower keep lists
        } BorrowInfo;

        struct { // in
            ui64 Lender = 0;
            TIntrusivePtr<TTabletStorageInfo> StorageInfo; // todo: must be list with info for every referenced tablet

            bool Collected = false;
            TVector<TLogoBlobID> Keep;
        } LoanInfo;
    };

    struct TCompactedPart {
        ui32 BindStep = Max<ui32>();
        TVector<TCompactedPartLoans> Parts;
    };

    THashMap<TLogoBlobID, TBorrowedPartInfo> BorrowedInfo;
    TDeque<TLogoBlobID> Garbage;

    THashMap<TLogoBlobID, TCompactedPartLoans> CompactedPartLoans;
    TDeque<TCompactedPart> PendingCompactedPartLoans;

    bool HasFlag;

    THashMap<ui64, TIntrusivePtr<TTabletStorageInfo>> ReferencedStorageInfos;

    void UpdateStorageInfo(TTabletStorageInfo *update);

    void FillBorrowProto(
            const TLogoBlobID &metaId,
            NKikimrExecutorFlat::TBorrowedPart &proto,
            const TBorrowedPartInfo &storedInfo);

    void StoreBorrowProto(
            const TLogoBlobID &metaId,
            TBorrowedPartInfo &storedInfo,
            TLogCommit *commit);

    bool CheckLoanCompletion(const TLogoBlobID &metaId, TBorrowedPartInfo &storedInfo, ui32 step);

public:
    TExecutorBorrowLogic(TAutoPtr<NPageCollection::TSteppedCookieAllocator>);

    TTabletStorageInfo* StorageInfoFor(const TLogoBlobID &blobId) {
        auto it = ReferencedStorageInfos.find(blobId.TabletID());
        if (it != ReferencedStorageInfos.end())
            return it->second.Get();
        else
            return nullptr;
    }

    ui64 GetKeepBytes() const {
        return KeepBytes;
    }

    // called on lender at moment of sharing part
    void BorrowBundle(
        const TLogoBlobID &bundleId,
        const TSet<ui64> &loaners,
        TLogCommit *commit);

    // called on loaner at moment of attaching part
    void LoanBundle(
        const TLogoBlobID &bundleId,
        TPageCollectionTxEnv::TLoanBundle &loaned,
        TLogCommit *commit);

    // called on loaner at moment of attaching part
    void LoanTxStatus(
        const TLogoBlobID &bundleId,
        TPageCollectionTxEnv::TLoanTxStatus &loaned,
        TLogCommit *commit);

    // called on lender
    void UpdateBorrow(
        const TLogoBlobID &bundleId,
        TPageCollectionTxEnv::TBorrowUpdate &borrowUpdate,
        TLogCommit *commit);

    // confirmation for loaner
    void ConfirmUpdateLoan(
        const TLogoBlobID &bundleId,
        const TLogoBlobID &borrowId,
        TLogCommit *commit);

    void SnapToLog(NKikimrExecutorFlat::TLogSnapshot&, TLogCommit&);

    // returns true if sieve blobs could be collected
    // returning false means those blobs are still in use
    bool BundlePartiallyCompacted(
        const NTable::IBundle &bundle,
        const NTable::NFwd::TSieve &sieve,
        TLogCommit *commit);

    // returns true if part blobs could be collected
    // returning false means part is still in use
    bool BundleCompacted(
        const NTable::IBundle &bundle,
        const NTable::NFwd::TSieve &sieve,
        TLogCommit *commit);

    // returns true if bundle blobs could be collected
    // returns false when part is still in use
    bool BundleCompacted(
        const NTable::IBorrowBundle &bundle,
        TLogCommit *commit);

    // returns true if part blobs could be collected
    // returning false means part is still in use
    bool BundleCompacted(
        const TLogoBlobID &bundleId,
        TLogCommit *commit);

    // returns true if smth moved to CompactedPartLoands so we need to signal for user tablet
    bool SetGcBarrier(ui32 step);

    const THashMap<TLogoBlobID, TCompactedPartLoans>* GetCompactedLoansList();
    const bool* GetHasFlag();

    // on bootstrap
    void RestoreBorrowedInfo(const TLogoBlobID &blobId, const NKikimrExecutorFlat::TBorrowedPart &proto);

    // for followers
    void RestoreFollowerBorrowedInfo(const TLogoBlobID &blobId, const NKikimrExecutorFlat::TBorrowedPart &proto);

    // for monitoring
    void OutputHtml(IOutputStream &out);
    TString DebugCheckBorrowConsistency(THashSet<TLogoBlobID> &knownBundles);

    // for cleanup
    THashMap<TLogoBlobID, TVector<ui64>> GetBorrowedParts() const;

    // i.e. parts we own, but loaned to others
    bool HasLoanedParts() const;
};

}}
