#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <optional>

namespace {

using namespace NKikimr::NOlap;
using namespace NKikimr::NOlap::NDataSharing;

constexpr TTabletId SelfTabletId = (TTabletId)100;
constexpr size_t BlobSpace = 48;
constexpr size_t TabletSpace = 12;
constexpr size_t MaxSteps = 260;

enum class EKind : ui8 {
    None,
    Direct,
    Borrowed,
    Sharing,
};

struct TBlobState {
    std::optional<TTabletId> BorrowedOwner;
    THashSet<TTabletId> SharedWith;
};

struct TCategoryState {
    THashSet<TTabletId> Direct;
    THashSet<TTabletId> Borrowed;
    THashSet<TTabletId> Sharing;
};

TUnifiedBlobId MakeBlob(ui64 idx) {
    idx %= BlobSpace;
    return TUnifiedBlobId(
        1 + idx % 4,
        1000 + idx,
        1 + idx % 17,
        1 + idx % 251,
        idx % 97,
        idx % 8,
        1 + idx % 4096);
}

TTabletId MakeTablet(ui64 idx) {
    return (TTabletId)(200 + idx % TabletSpace);
}

TUnifiedBlobId PickBlob(FuzzedDataProvider& provider) {
    return MakeBlob(provider.ConsumeIntegralInRange<ui64>(0, BlobSpace - 1));
}

TTabletId PickTablet(FuzzedDataProvider& provider) {
    return MakeTablet(provider.ConsumeIntegralInRange<ui64>(0, TabletSpace - 1));
}

TBlobState& StateFor(THashMap<TUnifiedBlobId, TBlobState>& model, const TUnifiedBlobId& blobId) {
    return model[blobId];
}

void AddCategory(
    TCategoryState& state,
    const TTabletId tabletId,
    const EKind kind)
{
    switch (kind) {
        case EKind::Direct:
            Y_ABORT_UNLESS(state.Direct.emplace(tabletId).second);
            break;
        case EKind::Borrowed:
            Y_ABORT_UNLESS(state.Borrowed.emplace(tabletId).second);
            break;
        case EKind::Sharing:
            Y_ABORT_UNLESS(state.Sharing.emplace(tabletId).second);
            break;
        case EKind::None:
            Y_ABORT("unexpected empty category");
    }
}

void AddCategory(
    THashMap<TUnifiedBlobId, TCategoryState>& categories,
    const TUnifiedBlobId& blobId,
    const TTabletId tabletId,
    const EKind kind)
{
    AddCategory(categories[blobId], tabletId, kind);
}

THashMap<TUnifiedBlobId, TCategoryState> CollectCategories(const TBlobsCategories& categories) {
    THashMap<TUnifiedBlobId, TCategoryState> result;
    for (auto it = categories.GetDirect().GetIterator(); it.IsValid(); ++it) {
        AddCategory(result, it.GetBlobId(), it.GetTabletId(), EKind::Direct);
    }
    for (auto it = categories.GetBorrowed().GetIterator(); it.IsValid(); ++it) {
        AddCategory(result, it.GetBlobId(), it.GetTabletId(), EKind::Borrowed);
    }
    for (auto it = categories.GetSharing().GetIterator(); it.IsValid(); ++it) {
        AddCategory(result, it.GetBlobId(), it.GetTabletId(), EKind::Sharing);
    }
    return result;
}

void CheckCategories(
    const TBlobsCategories& actualCategories,
    const THashMap<TUnifiedBlobId, TCategoryState>& expected)
{
    const auto checkTablets = [](const THashSet<TTabletId>& actual, const THashSet<TTabletId>& expected) {
        Y_ABORT_UNLESS(actual.size() == expected.size());
        for (const TTabletId tabletId : expected) {
            Y_ABORT_UNLESS(actual.contains(tabletId));
        }
    };

    const auto actual = CollectCategories(actualCategories);
    Y_ABORT_UNLESS(actual.size() == expected.size());
    for (const auto& [blobId, expectedState] : expected) {
        const auto* actualState = actual.FindPtr(blobId);
        Y_ABORT_UNLESS(actualState);
        checkTablets(actualState->Direct, expectedState.Direct);
        checkTablets(actualState->Borrowed, expectedState.Borrowed);
        checkTablets(actualState->Sharing, expectedState.Sharing);
    }
}

THashMap<TUnifiedBlobId, TCategoryState> ExpectedCurrentCategories(
    const THashMap<TUnifiedBlobId, TBlobState>& model)
{
    THashMap<TUnifiedBlobId, TCategoryState> expected;
    for (const auto& [blobId, state] : model) {
        if (state.BorrowedOwner) {
            AddCategory(expected, blobId, *state.BorrowedOwner, EKind::Borrowed);
        } else if (!state.SharedWith.empty()) {
            for (const TTabletId tabletId : state.SharedWith) {
                AddCategory(expected, blobId, tabletId, EKind::Sharing);
            }
        }
    }
    return expected;
}

THashMap<TUnifiedBlobId, TCategoryState> ExpectedStoreCategories(
    const THashSet<TUnifiedBlobId>& blobs,
    const THashMap<TUnifiedBlobId, TBlobState>& model)
{
    THashMap<TUnifiedBlobId, TCategoryState> expected;
    for (const TUnifiedBlobId& blobId : blobs) {
        const TBlobState* state = model.FindPtr(blobId);
        if (state && state->BorrowedOwner) {
            AddCategory(expected, blobId, *state->BorrowedOwner, EKind::Borrowed);
        } else if (state && !state->SharedWith.empty()) {
            for (const TTabletId tabletId : state->SharedWith) {
                AddCategory(expected, blobId, tabletId, EKind::Sharing);
            }
        } else {
            AddCategory(expected, blobId, SelfTabletId, EKind::Direct);
        }
    }
    return expected;
}

TCategoryState ExpectedRemoveCategory(
    const TUnifiedBlobId& blobId,
    const TTabletId tabletId,
    const THashMap<TUnifiedBlobId, TBlobState>& model)
{
    TCategoryState expected;
    const TBlobState* state = model.FindPtr(blobId);
    const bool hasSharing = state && !state->SharedWith.empty();
    bool doRemove = false;
    if (hasSharing) {
        Y_ABORT_UNLESS(state->SharedWith.contains(tabletId));
        AddCategory(expected, tabletId, EKind::Sharing);
        doRemove = state->SharedWith.size() == 1;
    } else {
        doRemove = true;
    }
    if (doRemove) {
        if (state && state->BorrowedOwner) {
            AddCategory(expected, *state->BorrowedOwner, EKind::Borrowed);
        } else {
            AddCategory(expected, tabletId, EKind::Direct);
        }
    }
    return expected;
}

TVector<std::pair<TUnifiedBlobId, TTabletId>> CurrentShared(
    const THashMap<TUnifiedBlobId, TBlobState>& model)
{
    TVector<std::pair<TUnifiedBlobId, TTabletId>> result;
    for (const auto& [blobId, state] : model) {
        for (const TTabletId tabletId : state.SharedWith) {
            result.emplace_back(blobId, tabletId);
        }
    }
    return result;
}

TVector<std::pair<TUnifiedBlobId, TTabletId>> CurrentBorrowed(
    const THashMap<TUnifiedBlobId, TBlobState>& model)
{
    TVector<std::pair<TUnifiedBlobId, TTabletId>> result;
    for (const auto& [blobId, state] : model) {
        if (state.BorrowedOwner) {
            result.emplace_back(blobId, *state.BorrowedOwner);
        }
    }
    return result;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    TStorageSharedBlobsManager manager("fuzz-storage", SelfTabletId);
    THashMap<TUnifiedBlobId, TBlobState> model;

    const size_t steps = provider.ConsumeIntegralInRange<size_t>(0, MaxSteps);
    for (size_t step = 0; step < steps && provider.remaining_bytes(); ++step) {
        switch (provider.ConsumeIntegralInRange<ui8>(0, 9)) {
            case 0: {
                const TUnifiedBlobId blobId = PickBlob(provider);
                const TTabletId tabletId = PickTablet(provider);
                auto& state = StateFor(model, blobId);
                if (state.BorrowedOwner) {
                    break;
                }
                TTabletsByBlob shared;
                Y_ABORT_UNLESS(shared.Add(tabletId, blobId));
                const bool expectedNew = !state.SharedWith.contains(tabletId);
                Y_ABORT_UNLESS(manager.AddSharedBlobs(shared) == expectedNew);
                state.SharedWith.emplace(tabletId);
                break;
            }
            case 1: {
                const auto shared = CurrentShared(model);
                if (shared.empty()) {
                    break;
                }
                const auto& [blobId, tabletId] = shared[provider.ConsumeIntegralInRange<size_t>(0, shared.size() - 1)];
                TTabletsByBlob removed;
                Y_ABORT_UNLESS(removed.Add(tabletId, blobId));
                manager.RemoveSharedBlobs(removed);
                auto& state = StateFor(model, blobId);
                Y_ABORT_UNLESS(state.SharedWith.erase(tabletId) == 1);
                break;
            }
            case 2: {
                const TUnifiedBlobId blobId = PickBlob(provider);
                auto& state = StateFor(model, blobId);
                if (!state.SharedWith.empty()) {
                    break;
                }
                const TTabletId owner = state.BorrowedOwner.value_or(PickTablet(provider));
                TTabletByBlob borrowed;
                borrowed->emplace(blobId, owner);
                manager.AddBorrowedBlobs(borrowed);
                state.BorrowedOwner = owner;
                break;
            }
            case 3: {
                const auto borrowed = CurrentBorrowed(model);
                if (borrowed.empty()) {
                    break;
                }
                const auto& [blobId, from] = borrowed[provider.ConsumeIntegralInRange<size_t>(0, borrowed.size() - 1)];
                const TTabletId to = provider.ConsumeBool() ? SelfTabletId : PickTablet(provider);
                THashSet<TUnifiedBlobId> blobs = { blobId };
                manager.CASBorrowedBlobs(from, to, blobs);
                auto& state = StateFor(model, blobId);
                Y_ABORT_UNLESS(state.BorrowedOwner == from);
                if (to == SelfTabletId) {
                    state.BorrowedOwner.reset();
                } else {
                    state.BorrowedOwner = to;
                }
                break;
            }
            case 4: {
                THashSet<TUnifiedBlobId> blobs;
                const size_t count = provider.ConsumeIntegralInRange<size_t>(1, 8);
                for (size_t i = 0; i < count; ++i) {
                    blobs.emplace(PickBlob(provider));
                }
                CheckCategories(manager.BuildStoreCategories(blobs), ExpectedStoreCategories(blobs, model));
                break;
            }
            case 5: {
                const TUnifiedBlobId blobId = PickBlob(provider);
                const TBlobState* state = model.FindPtr(blobId);
                TTabletId tabletId = PickTablet(provider);
                if (state && !state->SharedWith.empty()) {
                    auto it = state->SharedWith.begin();
                    const size_t skip = provider.ConsumeIntegralInRange<size_t>(0, state->SharedWith.size() - 1);
                    for (size_t i = 0; i < skip; ++i) {
                        ++it;
                    }
                    tabletId = *it;
                }
                TTabletsByBlob removed;
                Y_ABORT_UNLESS(removed.Add(tabletId, blobId));
                THashMap<TUnifiedBlobId, TCategoryState> expected;
                expected.emplace(blobId, ExpectedRemoveCategory(blobId, tabletId, model));
                CheckCategories(manager.BuildRemoveCategories(removed), expected);
                break;
            }
            case 6:
                CheckCategories(manager.GetBlobCategories(), ExpectedCurrentCategories(model));
                break;
            case 7:
                manager.Clear();
                model.clear();
                break;
            case 8: {
                const TUnifiedBlobId blobId = PickBlob(provider);
                const TTabletId tabletId = PickTablet(provider);
                auto& state = StateFor(model, blobId);
                if (state.BorrowedOwner) {
                    break;
                }
                const bool expectedNew = !state.SharedWith.contains(tabletId);
                Y_ABORT_UNLESS(manager.UpsertSharedBlobOnLoad(blobId, tabletId) == expectedNew);
                state.SharedWith.emplace(tabletId);
                break;
            }
            case 9: {
                const TUnifiedBlobId blobId = PickBlob(provider);
                auto& state = StateFor(model, blobId);
                if (state.BorrowedOwner || !state.SharedWith.empty()) {
                    break;
                }
                const TTabletId owner = PickTablet(provider);
                Y_ABORT_UNLESS(manager.UpsertBorrowedBlobOnLoad(blobId, owner));
                state.BorrowedOwner = owner;
                break;
            }
        }
        CheckCategories(manager.GetBlobCategories(), ExpectedCurrentCategories(model));
    }

    return 0;
}
