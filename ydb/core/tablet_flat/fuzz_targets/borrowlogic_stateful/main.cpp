#include <ydb/core/tablet_flat/flat_bio_eggs.h>
#include <ydb/core/tablet_flat/flat_executor_borrowlogic.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace {

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

constexpr ui64 SelfTabletId = 9200001;
constexpr ui64 ForeignTabletBase = 9300000;
constexpr ui32 Generation = 9;
constexpr ui32 MaxOps = 256;

TAutoPtr<NPageCollection::TSteppedCookieAllocator> MakeCookies() {
    TVector<NPageCollection::TSlot> slots;
    for (ui32 channel = 0; channel < 4; ++channel) {
        slots.emplace_back(channel, 123 + channel);
    }
    return new NPageCollection::TSteppedCookieAllocator(
        SelfTabletId,
        ui64(Generation) << 32,
        {0, 1u << 20},
        slots);
}

TLogoBlobID MakeBlob(ui64 tablet, ui32 step, ui32 cookie) {
    return TLogoBlobID(tablet, Generation, step, 0, 4096, cookie);
}

TLogCommit MakeCommit(ui32 step) {
    return TLogCommit(false, step, ECommit::Misc, {});
}

struct TBorrowBundle final : public NTable::IBorrowBundle {
    TLogoBlobID Id;
    TVector<TLogoBlobID> Blobs;

    explicit TBorrowBundle(const TLogoBlobID& id)
        : Id(id)
        , Blobs{id}
    {}

    const TLogoBlobID& BundleId() const noexcept override {
        return Id;
    }

    ui64 BackingSize() const noexcept override {
        return 4096 * Blobs.size();
    }

    void SaveAllBlobIdsTo(TVector<TLogoBlobID>& vec) const override {
        vec.insert(vec.end(), Blobs.begin(), Blobs.end());
    }
};

struct TLocalBorrow {
    TSet<ui64> Loaners;
    bool Compacted = false;
};

struct TRemoteLoan {
    ui64 Lender = 0;
    bool Collected = false;
    bool Published = false;
};

NKikimrExecutorFlat::TBorrowedPart MakeBorrowProto(
        const TLogoBlobID& metaId,
        const TSet<ui64>& loaners,
        ui64 lender,
        bool collected)
{
    NKikimrExecutorFlat::TBorrowedPart proto;
    LogoBlobIDFromLogoBlobID(metaId, proto.MutableMetaId());
    for (ui64 loaner : loaners) {
        proto.AddLoaners(loaner);
    }
    if (lender) {
        proto.SetLender(lender);
        proto.SetLoanCollected(collected);
    }
    return proto;
}

template <class TMap>
TLogoBlobID PickKey(FuzzedDataProvider& provider, const TMap& map) {
    auto it = map.begin();
    std::advance(it, provider.ConsumeIntegralInRange<size_t>(0, map.size() - 1));
    return it->first;
}

void CheckInvariants(TExecutorBorrowLogic& logic) {
    const auto borrowed = logic.GetBorrowedParts();
    const auto* compacted = logic.GetCompactedLoansList();
    const bool hasFlag = *logic.GetHasFlag();

    Y_ABORT_UNLESS(logic.GetKeepBytes() < (ui64(1) << 40));
    Y_ABORT_UNLESS(logic.HasLoanedParts() == !borrowed.empty());
    if (!hasFlag) {
        Y_ABORT_UNLESS(borrowed.empty());
    }
    for (const auto& [metaId, part] : *compacted) {
        Y_ABORT_UNLESS(metaId == part.MetaInfoId);
        Y_ABORT_UNLESS(part.Lender != 0);
    }

    TStringStream html;
    logic.OutputHtml(html);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    TExecutorBorrowLogic logic(MakeCookies());
    THashMap<TLogoBlobID, TLocalBorrow> local;
    THashMap<TLogoBlobID, TRemoteLoan> remote;

    ui32 step = 1;
    ui32 nextLocalCookie = 1;
    ui32 nextRemoteCookie = 10000;

    const ui32 ops = provider.ConsumeIntegralInRange<ui32>(0, MaxOps);
    for (ui32 opIndex = 0; opIndex < ops && provider.remaining_bytes(); ++opIndex) {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 9)) {
            case 0: {
                const TLogoBlobID id = MakeBlob(SelfTabletId, step, nextLocalCookie++);
                TSet<ui64> loaners;
                const ui32 count = provider.ConsumeIntegralInRange<ui32>(1, 4);
                for (ui32 i = 0; i < count; ++i) {
                    loaners.insert(ForeignTabletBase + provider.ConsumeIntegralInRange<ui32>(1, 16));
                }
                auto commit = MakeCommit(step++);
                logic.BorrowBundle(id, loaners, &commit);
                local[id].Loaners.insert(loaners.begin(), loaners.end());
                break;
            }

            case 1: {
                if (local.empty()) {
                    break;
                }
                const TLogoBlobID id = PickKey(provider, local);
                auto& state = local[id];
                if (state.Loaners.empty()) {
                    break;
                }
                TPageCollectionTxEnv::TBorrowUpdate update;
                const ui32 count = provider.ConsumeIntegralInRange<ui32>(1, state.Loaners.size());
                for (ui32 i = 0; i < count && !state.Loaners.empty(); ++i) {
                    auto it = state.Loaners.begin();
                    std::advance(it, provider.ConsumeIntegralInRange<size_t>(0, state.Loaners.size() - 1));
                    update.StoppedLoans.push_back(*it);
                    state.Loaners.erase(it);
                }
                auto commit = MakeCommit(step++);
                logic.UpdateBorrow(id, update, &commit);
                if (state.Loaners.empty()) {
                    local.erase(id);
                }
                break;
            }

            case 2: {
                if (local.empty()) {
                    break;
                }
                const TLogoBlobID id = PickKey(provider, local);
                auto& state = local[id];
                if (state.Compacted) {
                    break;
                }
                TBorrowBundle bundle(id);
                auto commit = MakeCommit(step++);
                commit.WaitFollowerGcAck = true;
                const bool collected = logic.BundleCompacted(static_cast<const NTable::IBorrowBundle&>(bundle), &commit);
                Y_ABORT_UNLESS(!collected);
                state.Compacted = true;
                break;
            }

            case 3: {
                const ui64 lender = ForeignTabletBase + provider.ConsumeIntegralInRange<ui32>(1, 16);
                const TLogoBlobID id = MakeBlob(lender, step, nextRemoteCookie++);
                NTable::TPartComponents components{{}, {}, {}, NTable::TEpoch::Max()};
                TPageCollectionTxEnv::TLoanBundle loaned(1, 2, lender, std::move(components));
                auto commit = MakeCommit(step++);
                logic.LoanBundle(id, loaned, &commit);
                remote.emplace(id, TRemoteLoan{lender, false, false});
                break;
            }

            case 4: {
                if (remote.empty()) {
                    break;
                }
                const TLogoBlobID id = PickKey(provider, remote);
                auto& state = remote[id];
                if (state.Collected) {
                    break;
                }
                auto commit = MakeCommit(step++);
                commit.WaitFollowerGcAck = true;
                const bool collected = logic.BundleCompacted(id, &commit);
                Y_ABORT_UNLESS(!collected);
                state.Collected = true;
                break;
            }

            case 5: {
                const bool changed = logic.SetGcBarrier(provider.ConsumeIntegralInRange<ui32>(0, step + 8));
                if (changed) {
                    for (auto& [_, state] : remote) {
                        if (state.Collected) {
                            state.Published = true;
                        }
                    }
                }
                break;
            }

            case 6: {
                TVector<TLogoBlobID> candidates;
                for (const auto& [id, state] : remote) {
                    if (state.Collected && state.Published) {
                        candidates.push_back(id);
                    }
                }
                if (candidates.empty()) {
                    break;
                }
                const TLogoBlobID id = candidates[provider.ConsumeIntegralInRange<size_t>(0, candidates.size() - 1)];
                auto commit = MakeCommit(step++);
                logic.ConfirmUpdateLoan(id, MakeBlob(SelfTabletId, step, nextLocalCookie++), &commit);
                remote.erase(id);
                break;
            }

            case 7: {
                const TLogoBlobID id = MakeBlob(SelfTabletId, step, nextLocalCookie++);
                TSet<ui64> loaners;
                loaners.insert(ForeignTabletBase + provider.ConsumeIntegralInRange<ui32>(1, 16));
                const auto proto = MakeBorrowProto(id, loaners, 0, false);
                logic.RestoreBorrowedInfo(MakeBlob(SelfTabletId, step, nextLocalCookie++), proto);
                local[id].Loaners.insert(loaners.begin(), loaners.end());
                ++step;
                break;
            }

            case 8: {
                const ui64 lender = ForeignTabletBase + provider.ConsumeIntegralInRange<ui32>(1, 16);
                const TLogoBlobID id = MakeBlob(lender, step, nextRemoteCookie++);
                const bool collected = provider.ConsumeBool();
                const auto proto = MakeBorrowProto(id, {}, lender, collected);
                logic.RestoreBorrowedInfo(MakeBlob(SelfTabletId, step, nextLocalCookie++), proto);
                remote.emplace(id, TRemoteLoan{lender, collected, collected});
                ++step;
                break;
            }

            case 9: {
                NKikimrExecutorFlat::TLogSnapshot snap;
                auto commit = MakeCommit(step++);
                logic.SnapToLog(snap, commit);
                Y_ABORT_UNLESS(snap.BorrowInfoIdsSize() <= local.size() + remote.size() + 8);
                break;
            }
        }

        CheckInvariants(logic);
    }

    return 0;
}
