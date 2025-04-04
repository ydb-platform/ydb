#include "flat_executor_borrowlogic.h"
#include "flat_bio_eggs.h"

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

TExecutorBorrowLogic::TExecutorBorrowLogic(TAutoPtr<NPageCollection::TSteppedCookieAllocator> cookies)
    : SelfTabletId(cookies ? cookies->Tablet : 0)
    , Cookies(cookies)
    , Slicer(1, Cookies.Get(), NBlockIO::BlockSize)
    , HasFlag(false)
{}

void TExecutorBorrowLogic::UpdateStorageInfo(TTabletStorageInfo *update) {
    auto itpair = ReferencedStorageInfos.insert(std::make_pair(update->TabletID, update));
    if (!itpair.second) {
        TIntrusivePtr<TTabletStorageInfo> &stored = itpair.first->second;
        if (*stored < *update)
            stored = update;
    }
}

void TExecutorBorrowLogic::FillBorrowProto(
    const TLogoBlobID &metaId,
    NKikimrExecutorFlat::TBorrowedPart &proto,
    const TBorrowedPartInfo &storedInfo)
{
    LogoBlobIDFromLogoBlobID(metaId, proto.MutableMetaId());

    if (storedInfo.BorrowInfo.FullBorrow) {
        proto.MutableLoaners()->Reserve(storedInfo.BorrowInfo.FullBorrow.size());
        for (ui64 x : storedInfo.BorrowInfo.FullBorrow)
            proto.AddLoaners(x);
    }

    if (storedInfo.BorrowInfo.Keep) {
        proto.MutableBorrowKeepList()->Reserve(storedInfo.BorrowInfo.Keep.size());
        for (const auto &x : storedInfo.BorrowInfo.Keep)
            LogoBlobIDFromLogoBlobID(x, proto.AddBorrowKeepList());
    }

    // we don't fill borrowed list for now (as we don't keep referenced blobs)

    if (storedInfo.LoanInfo.Lender) {
        proto.SetLender(storedInfo.LoanInfo.Lender);

        if (auto *info = storedInfo.LoanInfo.StorageInfo.Get())
            TabletStorageInfoToProto(*info, proto.AddStorageInfo());

        if (storedInfo.LoanInfo.Collected) {
            proto.SetLoanCollected(true);
            if (storedInfo.LoanInfo.Keep) {
                proto.MutableLoanKeepList()->Reserve(storedInfo.LoanInfo.Keep.size());
                for (const auto &x : storedInfo.LoanInfo.Keep)
                    LogoBlobIDFromLogoBlobID(x, proto.AddLoanKeepList());
            }
        }
    }
}

void TExecutorBorrowLogic::StoreBorrowProto(
        const TLogoBlobID &metaId,
        TBorrowedPartInfo &info,
        TLogCommit *commit)
{
    Cookies->Switch(commit->Step, false /* could reuse step */);

    NKikimrExecutorFlat::TBorrowedPart proto;
    FillBorrowProto(metaId, proto, info);

    auto glob = Slicer.One(commit->Refs, proto.SerializeAsString(), true);

    commit->GcDelta.Created.push_back(glob.Logo);

    if (auto gone = std::exchange(info.BorrowBlobId, glob.Logo))
        Garbage.push_back(gone);
}

bool TExecutorBorrowLogic::BundlePartiallyCompacted(
    const NTable::IBundle &bundle,
    const NTable::NFwd::TSieve &sieve,
    TLogCommit *commit)
{
    const TLogoBlobID &metaId = bundle.BundleId();
    TBorrowedPartInfo *info = BorrowedInfo.FindPtr(metaId);
    if (info == nullptr)
        return true;

    bool haveChanges = false;
    if (info->BorrowInfo.FullBorrow && SelfTabletId == metaId.TabletID()) {
        auto size = info->BorrowInfo.Keep.size();
        sieve.MaterializeTo(info->BorrowInfo.Keep);

        if (info->BorrowInfo.Keep.size() != size) {
            std::sort(info->BorrowInfo.Keep.begin(), info->BorrowInfo.Keep.end());
            auto end = std::unique(info->BorrowInfo.Keep.begin(), info->BorrowInfo.Keep.end());
            Y_ENSURE(end == info->BorrowInfo.Keep.end(),
                "Unexpected duplicates in compacted blobs");
            KeepBytes += info->BorrowInfo.UpdateKeepBytes();
            haveChanges = true;
        }
    }

    Y_ENSURE(commit->WaitFollowerGcAck);

    if (haveChanges) {
        StoreBorrowProto(metaId, *info, commit);
    }

    // must be loaned or borrowed, cannot collect blobs
    return false;
}

bool TExecutorBorrowLogic::BundleCompacted(
    const NTable::IBundle &bundle,
    const NTable::NFwd::TSieve &sieve,
    TLogCommit *commit)
{
    const TLogoBlobID &metaId = bundle.BundleId();
    TBorrowedPartInfo *info = BorrowedInfo.FindPtr(metaId);
    if (info == nullptr)
        return true;

    // 1. if bundle borrowed - keep blobs non-collected
    if (info->BorrowInfo.FullBorrow) {
        Y_ENSURE(!info->BorrowInfo.HasKeep(bundle.BundleId()),
            "Trying to compact the same page collection twice");

        if (SelfTabletId == metaId.TabletID()) {
            sieve.MaterializeTo(info->BorrowInfo.Keep);
            bundle.SaveAllBlobIdsTo(info->BorrowInfo.Keep);

            std::sort(info->BorrowInfo.Keep.begin(), info->BorrowInfo.Keep.end());
            auto end = std::unique(info->BorrowInfo.Keep.begin(), info->BorrowInfo.Keep.end());
            Y_ENSURE(end == info->BorrowInfo.Keep.end(),
                "Unexpected duplicates in compacted blobs");
            KeepBytes += info->BorrowInfo.UpdateKeepBytes();
        }
    }

    if (info->LoanInfo.Lender) {
        info->LoanInfo.Collected = true;
    }

    CheckLoanCompletion(metaId, *info, commit->Step);
    Y_ENSURE(commit->WaitFollowerGcAck);

    // must be loaned or borrowed (otherwise would be not on list)
    // in this case - changes must be propagated to lender before cleanup
    // in that case - loaner must detach from borrowed bundle and we must keep blobs non-collect awhile
    StoreBorrowProto(metaId, *info, commit);
    return false;
}

bool TExecutorBorrowLogic::BundleCompacted(
    const NTable::IBorrowBundle &bundle,
    TLogCommit *commit)
{
    const TLogoBlobID &metaId = bundle.BundleId();
    TBorrowedPartInfo *info = BorrowedInfo.FindPtr(metaId);
    if (info == nullptr)
        return true;

    // if bundle borrowed - keep blobs non-collected
    if (info->BorrowInfo.FullBorrow) {
        Y_ENSURE(!info->BorrowInfo.HasKeep(metaId),
            "Trying to compact the same bundle twice");

        if (SelfTabletId == metaId.TabletID()) {
            bundle.SaveAllBlobIdsTo(info->BorrowInfo.Keep);

            std::sort(info->BorrowInfo.Keep.begin(), info->BorrowInfo.Keep.end());
            auto end = std::unique(info->BorrowInfo.Keep.begin(), info->BorrowInfo.Keep.end());
            Y_ENSURE(end == info->BorrowInfo.Keep.end(),
                "Unexpected duplicates in compacted blobs");
            KeepBytes += info->BorrowInfo.UpdateKeepBytes();
        }
    }

    if (info->LoanInfo.Lender) {
        info->LoanInfo.Collected = true;
    }

    CheckLoanCompletion(metaId, *info, commit->Step);
    Y_ENSURE(commit->WaitFollowerGcAck);

    // must be loaned or borrowed (otherwise would be not on list)
    // in this case - changes must be propagated to lender before cleanup
    // in that case - loaner must detach from borrowed bundle and we must keep blobs non-collect awhile
    StoreBorrowProto(metaId, *info, commit);
    return false;
}

bool TExecutorBorrowLogic::BundleCompacted(
    const TLogoBlobID &bundleId,
    TLogCommit *commit)
{
    Y_ENSURE(SelfTabletId != bundleId.TabletID());
    TBorrowedPartInfo *info = BorrowedInfo.FindPtr(bundleId);
    if (info == nullptr)
        return true;

    if (info->LoanInfo.Lender) {
        info->LoanInfo.Collected = true;
    }

    CheckLoanCompletion(bundleId, *info, commit->Step);
    Y_ENSURE(commit->WaitFollowerGcAck);

    // must be loaned or borrowed (otherwise would be not on list)
    // in this case - changes must be propagated to lender before cleanup
    // in that case - loaner must detach from borrowed bundle and we must keep blobs non-collect awhile
    StoreBorrowProto(bundleId, *info, commit);
    return false;
}

void TExecutorBorrowLogic::BorrowBundle(
    const TLogoBlobID &bundleId,
    const TSet<ui64> &loaners,
    TLogCommit *commit)
{
    auto storedInfoItPair = BorrowedInfo.insert(std::make_pair(bundleId, TBorrowedPartInfo()));
    HasFlag = true;

    TBorrowedPartInfo &storedInfo = storedInfoItPair.first->second;

    Y_ENSURE(!(storedInfo.LoanInfo.Lender && storedInfo.LoanInfo.Collected),
        "Sanity check: trying to borrow a compacted bundle");

    // It is possible to borrow partially compacted bundles, in which case
    // keep list might not be empty. However, if bundle has been fully
    // compacted keep list would contain bundleId and it is possible
    // to check for that.
    Y_ENSURE(!storedInfo.BorrowInfo.HasKeep(bundleId),
        "Sanity check: trying to borrow a compacted bundle");

    auto &fullBorrow = storedInfo.BorrowInfo.FullBorrow;
    fullBorrow.insert( // not replace as some borrows could exist
        fullBorrow.end(),
        loaners.begin(),
        loaners.end());

    Sort(fullBorrow);
    // !!HACK: Allow to borrow the same bundle multiple times
    //Y_ENSURE(std::adjacent_find(fullBorrow.begin(), fullBorrow.end()) == fullBorrow.end());
    fullBorrow.erase(std::unique(fullBorrow.begin(), fullBorrow.end()), fullBorrow.end());

    StoreBorrowProto(bundleId, storedInfo, commit);
}

void TExecutorBorrowLogic::LoanBundle(
    const TLogoBlobID &bundleId,
    TPageCollectionTxEnv::TLoanBundle &loaned,
    TLogCommit *commit)
{
    auto storedInfoItPair = BorrowedInfo.insert(std::make_pair(bundleId, TBorrowedPartInfo()));
    Y_ENSURE(storedInfoItPair.second,
        "must not back-borrow parts at " << SelfTabletId
        << " part owner " << bundleId.TabletID()
        << " existing loan from " << storedInfoItPair.first->second.LoanInfo.Lender
        << " new loan from " << loaned.Lender);
    HasFlag = true;

    TBorrowedPartInfo &storedInfo = storedInfoItPair.first->second;
    storedInfo.LoanInfo.Lender = loaned.Lender;

    StoreBorrowProto(bundleId, storedInfo, commit);
}

void TExecutorBorrowLogic::LoanTxStatus(
    const TLogoBlobID &bundleId,
    TPageCollectionTxEnv::TLoanTxStatus &loaned,
    TLogCommit *commit)
{
    auto storedInfoItPair = BorrowedInfo.insert(std::make_pair(bundleId, TBorrowedPartInfo()));
    Y_ENSURE(storedInfoItPair.second,
        "must not back-borrow parts at " << SelfTabletId
        << " part owner " << bundleId.TabletID()
        << " existing loan from " << storedInfoItPair.first->second.LoanInfo.Lender
        << " new loan from " << loaned.Lender);
    HasFlag = true;

    TBorrowedPartInfo &storedInfo = storedInfoItPair.first->second;
    storedInfo.LoanInfo.Lender = loaned.Lender;

    StoreBorrowProto(bundleId, storedInfo, commit);
}

void TExecutorBorrowLogic::SnapToLog(NKikimrExecutorFlat::TLogSnapshot &snap, TLogCommit &commit)
{
    snap.MutableBorrowInfoIds()->Reserve(BorrowedInfo.size());
    for (auto &xpair : BorrowedInfo)
        LogoBlobIDFromLogoBlobID(xpair.second.BorrowBlobId, snap.AddBorrowInfoIds());

    commit.GcDelta.Deleted.insert(
            commit.GcDelta.Deleted.end(), Garbage.begin(),  Garbage.end());

    Garbage.clear();
}

const THashMap<TLogoBlobID, TCompactedPartLoans>* TExecutorBorrowLogic::GetCompactedLoansList() {
    return &CompactedPartLoans;
}

const bool* TExecutorBorrowLogic::GetHasFlag() {
    return &HasFlag;
}

bool TExecutorBorrowLogic::CheckLoanCompletion(const TLogoBlobID &metaId, TBorrowedPartInfo &storedInfo, ui32 step) {
    if (metaId.TabletID() == SelfTabletId) // produced by ourselves, could not be loaned
        return false;

    if (storedInfo.BorrowInfo.FullBorrow
        || storedInfo.BorrowInfo.Keep
        || !storedInfo.LoanInfo.Lender
        || !storedInfo.LoanInfo.Collected)
        return false;

    // zero to signal "from restore, apply right now
    // for new changes wait for step commit
    if (step == 0) {
        auto &x = CompactedPartLoans[metaId];
        x.MetaInfoId = metaId;
        x.Lender = storedInfo.LoanInfo.Lender;
    } else {
        if (PendingCompactedPartLoans.empty() || PendingCompactedPartLoans.back().BindStep != step) {
            PendingCompactedPartLoans.emplace_back();
            auto &x = PendingCompactedPartLoans.back();
            x.BindStep = step;
        }
        PendingCompactedPartLoans.back().Parts.emplace_back(metaId, storedInfo.LoanInfo.Lender);
    }

    return true;
}

bool TExecutorBorrowLogic::SetGcBarrier(ui32 step) {
    bool somethingChanged = false;
    while (!PendingCompactedPartLoans.empty()) {
        auto &x = PendingCompactedPartLoans.front();
        if (x.BindStep > step)
            break;

        for (auto &compactedPart : x.Parts)
            CompactedPartLoans.emplace(compactedPart.MetaInfoId, std::move(compactedPart));

        somethingChanged = true;
        PendingCompactedPartLoans.pop_front();
    }

    HasFlag = !BorrowedInfo.empty();
    return somethingChanged;
}

void TExecutorBorrowLogic::UpdateBorrow(
    const TLogoBlobID &metaInfoId,
    TPageCollectionTxEnv::TBorrowUpdate &borrowUpdate,
    TLogCommit *commit)
{
    auto storedInfoIt = BorrowedInfo.find(metaInfoId);
    if (storedInfoIt == BorrowedInfo.end())
        return;

    bool smthChanged = false;

    TBorrowedPartInfo &storedInfo = storedInfoIt->second;

    for (auto &loaner : borrowUpdate.StoppedLoans) {
        const auto it = std::find(storedInfo.BorrowInfo.FullBorrow.begin(), storedInfo.BorrowInfo.FullBorrow.end(), loaner);
        if (it == storedInfo.BorrowInfo.FullBorrow.end())
            continue;

        smthChanged = true;
        if (storedInfo.BorrowInfo.FullBorrow.size() == 1) {
            TVector<ui64>().swap(storedInfo.BorrowInfo.FullBorrow);
            if (storedInfo.BorrowInfo.Keep) {
                commit->GcDelta.Deleted.insert(
                    commit->GcDelta.Deleted.end(),
                    storedInfo.BorrowInfo.Keep.begin(),
                    storedInfo.BorrowInfo.Keep.end());
                TVector<TLogoBlobID>().swap(storedInfo.BorrowInfo.Keep);
                KeepBytes += storedInfo.BorrowInfo.UpdateKeepBytes();
            }
        } else {
            *it = storedInfo.BorrowInfo.FullBorrow.back();
            storedInfo.BorrowInfo.FullBorrow.crop(storedInfo.BorrowInfo.FullBorrow.size() - 1);
        }
    }

    if (!smthChanged) // if nothing changes - nothing to change
        return;

    if (metaInfoId.TabletID() == SelfTabletId) {
    // could not be loaned, so if complete - could be erased
        StoreBorrowProto(metaInfoId, storedInfo, commit);

        if (storedInfo.BorrowInfo.FullBorrow.empty()) {
            Garbage.push_back(storedInfo.BorrowBlobId);
            BorrowedInfo.erase(storedInfoIt);
            HasFlag = !BorrowedInfo.empty();
        }
    } else {
        // if not local - must be loaned
        CheckLoanCompletion(metaInfoId, storedInfo, commit->Step);
        StoreBorrowProto(metaInfoId, storedInfo, commit);
        Y_ENSURE(commit->WaitFollowerGcAck);
    }
}

void TExecutorBorrowLogic::ConfirmUpdateLoan(
    const TLogoBlobID &metaInfoId,
    const TLogoBlobID &borrowId,
    TLogCommit *commit)
{
    auto storedInfoIt = BorrowedInfo.find(metaInfoId);
    if (storedInfoIt == BorrowedInfo.end())
        return;

    Y_UNUSED(borrowId);

    TBorrowedPartInfo &storedInfo = storedInfoIt->second;

    //if (storedInfo.BorrowBlobId != borrowId)
    //    return;

    //if (storedInfo.LoanInfo.Lender == 0) // already confirmed, nothing to update
    //    return;

    Y_ENSURE(storedInfo.LoanInfo.Collected, "must not stop loan for non-collected parts");
    Y_ENSURE(!storedInfo.BorrowInfo.FullBorrow, "must not stop loan for borrowed parts");

    // todo: merge - in such case we could loan part from different lenders.
    // so naive approach would not work

    storedInfo.LoanInfo.Lender = 0;
    StoreBorrowProto(metaInfoId, storedInfo, commit);

    Garbage.push_back(storedInfo.BorrowBlobId);
    BorrowedInfo.erase(storedInfoIt);
    CompactedPartLoans.erase(metaInfoId);

    HasFlag = !BorrowedInfo.empty();
}

void TExecutorBorrowLogic::RestoreFollowerBorrowedInfo(const TLogoBlobID &blobId, const NKikimrExecutorFlat::TBorrowedPart &proto) {
    Y_UNUSED(blobId);
    const TLogoBlobID metaInfoId = LogoBlobIDFromLogoBlobID(proto.GetMetaId());

    if (proto.HasLender()) {
        auto storedInfoItPair = BorrowedInfo.insert(std::make_pair(metaInfoId, TBorrowedPartInfo()));
        TBorrowedPartInfo &storedInfo = storedInfoItPair.first->second;

        storedInfo.LoanInfo.Lender = proto.GetLender();

        if (proto.StorageInfoSize() > 0) {
            Y_ENSURE(proto.StorageInfoSize() == 1);
            storedInfo.LoanInfo.StorageInfo = TabletStorageInfoFromProto(proto.GetStorageInfo(0));
            UpdateStorageInfo(storedInfo.LoanInfo.StorageInfo.Get());
        }
    }

    // as part switch could be pending - we could not drop info right now
    // and in reality we never drop info until reloading - it's simpler to keep some (limited in number) deprecated infos
}

void TExecutorBorrowLogic::RestoreBorrowedInfo(const TLogoBlobID &blobId, const NKikimrExecutorFlat::TBorrowedPart &proto) {
    const TLogoBlobID metaInfoId = LogoBlobIDFromLogoBlobID(proto.GetMetaId());

    auto storedInfoItPair = BorrowedInfo.insert(std::make_pair(metaInfoId, TBorrowedPartInfo()));
    TBorrowedPartInfo &storedInfo = storedInfoItPair.first->second;

    if (!storedInfoItPair.second) {
        Y_ENSURE(blobId > storedInfo.BorrowBlobId);
        Garbage.push_back(storedInfo.BorrowBlobId);
        KeepBytes -= storedInfo.BorrowInfo.KeepBytes;
        storedInfo = TBorrowedPartInfo();
    }

    storedInfo.BorrowBlobId = blobId;

    if (proto.LoanersSize()) {
        storedInfo.BorrowInfo.FullBorrow.insert(
            storedInfo.BorrowInfo.FullBorrow.end(),
            proto.GetLoaners().begin(),
            proto.GetLoaners().end());
    }

    // we don't fill and use borrowed list for now

    if (proto.BorrowKeepListSize()) {
        storedInfo.BorrowInfo.Keep.reserve(proto.BorrowKeepListSize());
        for (const auto &x : proto.GetBorrowKeepList())
            storedInfo.BorrowInfo.Keep.emplace_back(LogoBlobIDFromLogoBlobID(x));
        KeepBytes += storedInfo.BorrowInfo.UpdateKeepBytes();
    }

    if (proto.HasLender()) {
        storedInfo.LoanInfo.Lender = proto.GetLender();

        if (proto.StorageInfoSize() > 0) {
            Y_ENSURE(proto.StorageInfoSize() == 1);
            storedInfo.LoanInfo.StorageInfo = TabletStorageInfoFromProto(proto.GetStorageInfo(0));
            UpdateStorageInfo(storedInfo.LoanInfo.StorageInfo.Get());
        }
    }

    storedInfo.LoanInfo.Collected = proto.HasLoanCollected() && proto.GetLoanCollected();
    if (proto.LoanKeepListSize()) {
        storedInfo.LoanInfo.Keep.reserve(proto.LoanKeepListSize());
        for (const auto &x : proto.GetLoanKeepList())
            storedInfo.LoanInfo.Keep.emplace_back(LogoBlobIDFromLogoBlobID(x));
    }

    if (!storedInfo.BorrowInfo.FullBorrow
        // todo: partial borrow
        && !storedInfo.LoanInfo.Lender)
    {
        Garbage.push_back(storedInfo.BorrowBlobId);
        BorrowedInfo.erase(storedInfoItPair.first);
        CompactedPartLoans.erase(metaInfoId);
    } else {
        CheckLoanCompletion(metaInfoId, storedInfo, 0);
    }

    HasFlag = !BorrowedInfo.empty();
}

void TExecutorBorrowLogic::OutputHtml(IOutputStream &out) {
    HTML(out) {
        TAG(TH4) {out << "Borrowed parts";}
        PRE() {
            for (const auto &xpair : BorrowedInfo) {
                if (xpair.second.BorrowInfo.FullBorrow) {
                    out << xpair.first << ":";
                    for (ui64 targetTablet : xpair.second.BorrowInfo.FullBorrow)
                        out << " " << targetTablet;
                    out << Endl;
                }
            }
        }

        TAG(TH4) {out << "Loaned parts";}
        PRE() {
            for (const auto &xpair : BorrowedInfo) {
                if (xpair.second.LoanInfo.Lender)
                    out << xpair.first << ": " << xpair.second.LoanInfo.Lender;
                if (xpair.second.LoanInfo.Collected)
                    out << " - collected";
                out << Endl;
            }
        }
    }
}

TString TExecutorBorrowLogic::DebugCheckBorrowConsistency(THashSet<TLogoBlobID> &knownBundles) {
    TStringBuilder out;
    // every non-collected bundle must be known

    for (const auto &xpair : BorrowedInfo) {
        bool collected;
        if (xpair.second.LoanInfo.Lender) {
            collected = xpair.second.LoanInfo.Collected;
        } else if (xpair.second.BorrowInfo.FullBorrow) {
            collected = xpair.second.BorrowInfo.HasKeep(xpair.first);
        } else {
            out << xpair.first << ": neither borrowed nor loaned. ";
            continue;
        }

        if (collected) {
            if (knownBundles.contains(xpair.first))
                out << xpair.first << ": collected bundle still present in database. ";
        } else {
            if (!knownBundles.contains(xpair.first))
                out << xpair.first << ": non-collected bundle not present in database. ";
        }
    }

    // must be no foreign bundles not tracked by borrow logic
    return out;
}

THashMap<TLogoBlobID, TVector<ui64>> TExecutorBorrowLogic::GetBorrowedParts() const {
    THashMap<TLogoBlobID, TVector<ui64>> result;
    for (const auto &xpair : BorrowedInfo) {
        if (xpair.second.BorrowInfo.FullBorrow) {
            auto& tablets = result[xpair.first];
            for (ui64 tablet : xpair.second.BorrowInfo.FullBorrow) {
                tablets.push_back(tablet);
            }
        }
    }
    return result;
}

bool TExecutorBorrowLogic::HasLoanedParts() const {
    for (const auto &xpair : BorrowedInfo) {
        if (xpair.second.BorrowInfo.FullBorrow) {
            return true;
        }
    }
    return false;
}

}}
