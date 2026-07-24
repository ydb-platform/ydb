#include <ydb/core/tablet_flat/flat_range_cache.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/array_ref.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/align.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>


namespace {

using namespace NKikimr;
using namespace NKikimr::NTable;

constexpr size_t MaxRanges = 32;
constexpr size_t MaxSteps = 256;
constexpr size_t MaxPoolAllocs = 64;

enum class EKeyMode {
    Native,
    Pg,
};

enum class EPgKeyType : ui8 {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
};

struct TKeyMode {
    EKeyMode Mode = EKeyMode::Native;
    EPgKeyType PgType = EPgKeyType::Int4;

    bool IsPg() const {
        return Mode == EKeyMode::Pg;
    }
};

TStringBuf PgTypeName(EPgKeyType type) {
    switch (type) {
    case EPgKeyType::Bool:
        return "pgbool";
    case EPgKeyType::Int2:
        return "pgint2";
    case EPgKeyType::Int4:
        return "pgint4";
    case EPgKeyType::Int8:
        return "pgint8";
    case EPgKeyType::Float4:
        return "pgfloat4";
    case EPgKeyType::Float8:
        return "pgfloat8";
    }
    Y_ABORT("unreachable");
}

ui32 KeyGroupCount(const TKeyMode& mode) {
    if (mode.IsPg() && mode.PgType == EPgKeyType::Bool) {
        return 2;
    }
    return 4;
}

ui32 MaxKeyForMode(const TKeyMode& mode) {
    return KeyGroupCount(mode) * 128 - 1;
}

struct TKeyHolder {
    ui64 First = 0;
    alignas(8) char PgFirst[8] = {};
    TString Second;
    TCell Cells[2];

    TKeyHolder(ui32 index, TKeyMode mode)
        : First(index / 128)
        , Second(Sprintf("%03u%s", index % 128, (index % 7 == 0) ? "-long-string-payload" : ""))
    {
        if (mode.IsPg()) {
            const ui32 group = index / 128;
            switch (mode.PgType) {
            case EPgKeyType::Bool: {
                const ui8 value = group != 0;
                std::memcpy(PgFirst, &value, sizeof(value));
                Cells[0] = TCell(PgFirst, sizeof(value));
                break;
            }
            case EPgKeyType::Int2: {
                const i16 value = group;
                std::memcpy(PgFirst, &value, sizeof(value));
                Cells[0] = TCell(PgFirst, sizeof(value));
                break;
            }
            case EPgKeyType::Int4: {
                const i32 value = group;
                std::memcpy(PgFirst, &value, sizeof(value));
                Cells[0] = TCell(PgFirst, sizeof(value));
                break;
            }
            case EPgKeyType::Int8: {
                const i64 value = group;
                std::memcpy(PgFirst, &value, sizeof(value));
                Cells[0] = TCell(PgFirst, sizeof(value));
                break;
            }
            case EPgKeyType::Float4: {
                const float value = group;
                std::memcpy(PgFirst, &value, sizeof(value));
                Cells[0] = TCell(PgFirst, sizeof(value));
                break;
            }
            case EPgKeyType::Float8: {
                const double value = group;
                std::memcpy(PgFirst, &value, sizeof(value));
                Cells[0] = TCell(PgFirst, sizeof(value));
                break;
            }
            }
        } else {
            Cells[0] = TCell(reinterpret_cast<const char*>(&First), sizeof(First));
        }
        Cells[1] = TCell(Second.data(), Second.size());
    }

    TArrayRef<const TCell> Ref() const {
        return { Cells, 2 };
    }
};

struct TRangeModel {
    ui32 From = 0;
    ui32 To = 0;
    bool ToInfinity = false;
    bool FromInclusive = true;
    bool ToInclusive = true;
    TRowVersion Version = TRowVersion::Min();
};

struct TPoolAlloc {
    void* Ptr = nullptr;
    size_t Len = 0;
};

ui32 ConsumeKey(FuzzedDataProvider& fdp, ui32 maxKey) {
    return fdp.ConsumeIntegralInRange<ui32>(0, maxKey);
}

TRowVersion ConsumeVersion(FuzzedDataProvider& fdp) {
    const ui64 step = fdp.ConsumeIntegral<ui16>();
    const ui64 txId = fdp.ConsumeIntegral<ui16>();
    return TRowVersion(step, txId);
}

bool ValidRange(const TRangeModel& range) {
    if (range.ToInfinity) {
        return true;
    }
    return range.From < range.To || (range.From == range.To && range.FromInclusive && range.ToInclusive);
}

bool LeftOf(const TRangeModel& left, const TRangeModel& right) {
    if (left.ToInfinity) {
        return false;
    }
    if (left.To != right.From) {
        return left.To < right.From;
    }
    return !left.ToInclusive || !right.FromInclusive;
}

bool Overlap(const TRangeModel& left, const TRangeModel& right) {
    return !LeftOf(left, right) && !LeftOf(right, left);
}

bool Contains(const TRangeModel& range, ui32 key) {
    const bool afterLeft = key > range.From || (key == range.From && range.FromInclusive);
    const bool beforeRight = range.ToInfinity || key < range.To || (key == range.To && range.ToInclusive);
    return afterLeft && beforeRight;
}

bool CanPlace(const TVector<TRangeModel>& model, const TRangeModel& candidate, size_t skipA = Max<size_t>(), size_t skipB = Max<size_t>()) {
    if (!ValidRange(candidate)) {
        return false;
    }
    for (size_t i = 0; i < model.size(); ++i) {
        if (i == skipA || i == skipB) {
            continue;
        }
        if (Overlap(model[i], candidate)) {
            return false;
        }
    }
    return true;
}

void SortModel(TVector<TRangeModel>& model) {
    std::sort(model.begin(), model.end(), [](const TRangeModel& left, const TRangeModel& right) {
        if (left.From != right.From) {
            return left.From < right.From;
        }
        if (left.FromInclusive != right.FromInclusive) {
            return left.FromInclusive;
        }
        if (left.To != right.To) {
            if (left.ToInfinity != right.ToInfinity) {
                return !left.ToInfinity;
            }
            return left.To < right.To;
        }
        if (left.ToInfinity != right.ToInfinity) {
            return !left.ToInfinity;
        }
        return left.ToInclusive && !right.ToInclusive;
    });
}

ui32 DecodePgGroup(const TCell& cell, EPgKeyType type) {
    switch (type) {
    case EPgKeyType::Bool: {
        Y_ABORT_UNLESS(cell.Size() == sizeof(ui8));
        ui8 value = 0;
        std::memcpy(&value, cell.Data(), sizeof(value));
        Y_ABORT_UNLESS(value <= 1);
        return value;
    }
    case EPgKeyType::Int2: {
        Y_ABORT_UNLESS(cell.Size() == sizeof(i16));
        i16 value = 0;
        std::memcpy(&value, cell.Data(), sizeof(value));
        Y_ABORT_UNLESS(value >= 0);
        return value;
    }
    case EPgKeyType::Int4: {
        Y_ABORT_UNLESS(cell.Size() == sizeof(i32));
        i32 value = 0;
        std::memcpy(&value, cell.Data(), sizeof(value));
        Y_ABORT_UNLESS(value >= 0);
        return value;
    }
    case EPgKeyType::Int8: {
        Y_ABORT_UNLESS(cell.Size() == sizeof(i64));
        i64 value = 0;
        std::memcpy(&value, cell.Data(), sizeof(value));
        Y_ABORT_UNLESS(value >= 0);
        return value;
    }
    case EPgKeyType::Float4: {
        Y_ABORT_UNLESS(cell.Size() == sizeof(float));
        float value = 0;
        std::memcpy(&value, cell.Data(), sizeof(value));
        Y_ABORT_UNLESS(value >= 0);
        return ui32(value);
    }
    case EPgKeyType::Float8: {
        Y_ABORT_UNLESS(cell.Size() == sizeof(double));
        double value = 0;
        std::memcpy(&value, cell.Data(), sizeof(value));
        Y_ABORT_UNLESS(value >= 0);
        return ui32(value);
    }
    }
    Y_ABORT("unreachable");
}

ui32 DecodeKey(TArrayRef<const TCell> key, TKeyMode mode) {
    Y_ABORT_UNLESS(key.size() == 2);
    Y_ABORT_UNLESS(!key[0].IsNull());
    Y_ABORT_UNLESS(!key[1].IsNull() && key[1].Size() >= 3);

    ui64 first = 0;
    if (mode.IsPg()) {
        first = DecodePgGroup(key[0], mode.PgType);
    } else {
        Y_ABORT_UNLESS(key[0].Size() == sizeof(ui64));
        std::memcpy(&first, key[0].Data(), sizeof(first));
    }

    ui32 second = 0;
    for (size_t i = 0; i < 3; ++i) {
        const char c = key[1].Data()[i];
        Y_ABORT_UNLESS(c >= '0' && c <= '9');
        second = second * 10 + ui32(c - '0');
    }
    Y_ABORT_UNLESS(second < 128);
    const ui64 index = first * 128 + second;
    Y_ABORT_UNLESS(index <= MaxKeyForMode(mode));
    return ui32(index);
}

TKeyRangeCache::const_iterator IteratorAt(const TKeyRangeCache& cache, size_t index) {
    auto it = cache.begin();
    while (index-- && it != cache.end()) {
        ++it;
    }
    return it;
}

size_t IteratorIndex(const TKeyRangeCache& cache, TKeyRangeCache::const_iterator target) {
    size_t index = 0;
    for (auto it = cache.begin(); it != cache.end(); ++it, ++index) {
        if (it == target) {
            return index;
        }
    }
    Y_ABORT_UNLESS(target == cache.end());
    return index;
}

TVector<TRangeModel> SnapshotModel(const TKeyRangeCache& cache, TKeyMode mode) {
    TVector<TRangeModel> model;
    for (const auto& entry : cache) {
        Y_ABORT_UNLESS(entry.FromKey);
        model.push_back({
            DecodeKey(entry.FromKey, mode),
            entry.ToKey ? DecodeKey(entry.ToKey, mode) : MaxKeyForMode(mode),
            !entry.ToKey,
            entry.FromInclusive,
            entry.ToInclusive,
            entry.MaxVersion,
        });
    }
    return model;
}

void CheckStatsMonotonic(const TKeyRangeCache::TStats& previous, const TKeyRangeCache::TStats& current) {
    Y_ABORT_UNLESS(current.Allocations >= previous.Allocations);
    Y_ABORT_UNLESS(current.Deallocations >= previous.Deallocations);
    Y_ABORT_UNLESS(current.Evictions >= previous.Evictions);
    Y_ABORT_UNLESS(current.GarbageCollections >= previous.GarbageCollections);
}

void CheckFindForward(const TKeyRangeCache& cache, const TVector<TRangeModel>& model, ui32 key, TKeyMode mode) {
    TKeyHolder keyHolder(key, mode);
    const auto result = cache.FindKey(keyHolder.Ref());

    size_t expectedIndex = model.size();
    bool expectedFound = false;
    for (size_t i = 0; i < model.size(); ++i) {
        const TRangeModel& range = model[i];
        if (range.ToInfinity || range.To > key || (range.To == key && range.ToInclusive)) {
            expectedIndex = i;
            expectedFound = Contains(range, key);
            break;
        }
    }

    Y_ABORT_UNLESS(result.second == expectedFound);
    Y_ABORT_UNLESS(IteratorIndex(cache, result.first) == expectedIndex);
}

void CheckFindReverse(const TKeyRangeCache& cache, const TVector<TRangeModel>& model, ui32 key, TKeyMode mode) {
    TKeyHolder keyHolder(key, mode);
    const auto result = cache.FindKeyReverse(keyHolder.Ref());

    size_t expectedIndex = model.size();
    bool expectedFound = false;
    for (size_t i = model.size(); i > 0; --i) {
        const TRangeModel& range = model[i - 1];
        if (range.From < key || (range.From == key && range.FromInclusive)) {
            expectedIndex = i - 1;
            expectedFound = Contains(range, key);
            break;
        }
    }

    Y_ABORT_UNLESS(result.second == expectedFound);
    Y_ABORT_UNLESS(IteratorIndex(cache, result.first) == expectedIndex);
}

void CheckCache(const TKeyRangeCache& cache, const TVector<TRangeModel>& model, TArrayRef<const NScheme::TTypeInfoOrder> types, TKeyMode mode) {
    Y_ABORT_UNLESS(cache.GetTotalUsed() <= cache.GetTotalAllocated());

    size_t index = 0;
    for (const auto& entry : cache) {
        Y_ABORT_UNLESS(index < model.size());
        const TRangeModel& expected = model[index++];
        TKeyHolder from(expected.From, mode);
        Y_ABORT_UNLESS(entry.FromKey);
        Y_ABORT_UNLESS(CompareTypedCellVectors(entry.FromKey.data(), from.Cells, types.data(), types.size()) == 0);
        if (expected.ToInfinity) {
            Y_ABORT_UNLESS(!entry.ToKey);
        } else {
            TKeyHolder to(expected.To, mode);
            Y_ABORT_UNLESS(entry.ToKey);
            Y_ABORT_UNLESS(CompareTypedCellVectors(entry.ToKey.data(), to.Cells, types.data(), types.size()) == 0);
        }
        Y_ABORT_UNLESS(entry.FromInclusive == expected.FromInclusive);
        Y_ABORT_UNLESS(entry.ToInclusive == expected.ToInclusive);
        Y_ABORT_UNLESS(entry.MaxVersion == expected.Version);
        if (index > 1) {
            Y_ABORT_UNLESS(LeftOf(model[index - 2], expected));
        }
    }
    Y_ABORT_UNLESS(index == model.size());

    const ui32 maxKey = MaxKeyForMode(mode);
    for (ui32 key : { ui32(0), ui32(1), ui32(7), ui32(31), ui32(127), Min<ui32>(128, maxKey), Min<ui32>(255, maxKey), maxKey }) {
        CheckFindForward(cache, model, key, mode);
        CheckFindReverse(cache, model, key, mode);
    }
}

void AddRange(TKeyRangeCache& cache, TVector<TRangeModel>& model, FuzzedDataProvider& fdp, TKeyMode mode) {
    if (model.size() >= MaxRanges) {
        return;
    }

    TRangeModel candidate;
    const ui32 maxKey = MaxKeyForMode(mode);
    candidate.From = ConsumeKey(fdp, maxKey);
    candidate.ToInfinity = fdp.ConsumeIntegralInRange<unsigned>(0, 7) == 0;
    if (candidate.ToInfinity) {
        candidate.To = maxKey;
        candidate.ToInclusive = false;
    } else {
        candidate.To = ConsumeKey(fdp, maxKey);
        if (candidate.To < candidate.From) {
            std::swap(candidate.From, candidate.To);
        }
        candidate.ToInclusive = fdp.ConsumeBool();
    }
    candidate.FromInclusive = fdp.ConsumeBool();
    candidate.Version = ConsumeVersion(fdp);

    if (!candidate.ToInfinity && candidate.From == candidate.To) {
        candidate.FromInclusive = true;
        candidate.ToInclusive = true;
    }
    if (!CanPlace(model, candidate)) {
        return;
    }

    TKeyHolder from(candidate.From, mode);
    TArrayRef<TCell> fromCopy = cache.AllocateKey(from.Ref());
    TArrayRef<TCell> toCopy;
    if (!candidate.ToInfinity) {
        TKeyHolder to(candidate.To, mode);
        toCopy = cache.AllocateKey(to.Ref());
    }
    cache.Add(TKeyRangeEntry(fromCopy, toCopy, candidate.FromInclusive, candidate.ToInclusive, candidate.Version));
    model.push_back(candidate);
    SortModel(model);
}

void TouchRange(TKeyRangeCache& cache, const TVector<TRangeModel>& model, FuzzedDataProvider& fdp) {
    if (model.empty()) {
        return;
    }
    cache.Touch(IteratorAt(cache, fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1)));
}

void ExtendLeft(TKeyRangeCache& cache, TVector<TRangeModel>& model, FuzzedDataProvider& fdp, TKeyMode mode) {
    if (model.empty()) {
        return;
    }
    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1);
    TRangeModel candidate = model[index];
    candidate.From = fdp.ConsumeIntegralInRange<ui32>(0, candidate.From);
    candidate.FromInclusive = fdp.ConsumeBool();
    candidate.Version = Max(candidate.Version, ConsumeVersion(fdp));
    if (!CanPlace(model, candidate, index)) {
        return;
    }

    TKeyHolder from(candidate.From, mode);
    TArrayRef<TCell> fromCopy = cache.AllocateKey(from.Ref());
    cache.ExtendLeft(IteratorAt(cache, index), fromCopy, candidate.FromInclusive, candidate.Version);
    model[index] = candidate;
    SortModel(model);
}

void ExtendRight(TKeyRangeCache& cache, TVector<TRangeModel>& model, FuzzedDataProvider& fdp, TKeyMode mode) {
    if (model.empty()) {
        return;
    }
    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1);
    TRangeModel candidate = model[index];
    if (candidate.ToInfinity) {
        return;
    }
    candidate.ToInfinity = fdp.ConsumeIntegralInRange<unsigned>(0, 3) == 0;
    if (candidate.ToInfinity) {
        const ui32 maxKey = MaxKeyForMode(mode);
        candidate.To = maxKey;
        candidate.ToInclusive = false;
    } else {
        candidate.To = fdp.ConsumeIntegralInRange<ui32>(candidate.To, MaxKeyForMode(mode));
        candidate.ToInclusive = fdp.ConsumeBool();
    }
    candidate.Version = Max(candidate.Version, ConsumeVersion(fdp));
    if (!CanPlace(model, candidate, index)) {
        return;
    }

    TArrayRef<TCell> toCopy;
    if (!candidate.ToInfinity) {
        TKeyHolder to(candidate.To, mode);
        toCopy = cache.AllocateKey(to.Ref());
    }
    cache.ExtendRight(IteratorAt(cache, index), toCopy, candidate.ToInclusive, candidate.Version);
    model[index] = candidate;
    SortModel(model);
}

void MergeRanges(TKeyRangeCache& cache, TVector<TRangeModel>& model, FuzzedDataProvider& fdp) {
    if (model.size() < 2) {
        return;
    }
    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 2);
    TRangeModel merged = model[index];
    merged.To = model[index + 1].To;
    merged.ToInfinity = model[index + 1].ToInfinity;
    merged.ToInclusive = model[index + 1].ToInclusive;
    merged.Version = Max(Max(merged.Version, model[index + 1].Version), ConsumeVersion(fdp));
    if (!CanPlace(model, merged, index, index + 1)) {
        return;
    }

    cache.Merge(IteratorAt(cache, index), IteratorAt(cache, index + 1), merged.Version);
    model[index] = merged;
    model.erase(model.begin() + index + 1);
}

void InvalidateRange(TKeyRangeCache& cache, TVector<TRangeModel>& model, FuzzedDataProvider& fdp) {
    if (model.empty()) {
        return;
    }
    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1);
    cache.Invalidate(IteratorAt(cache, index));
    model.erase(model.begin() + index);
}

void InvalidateKey(TKeyRangeCache& cache, TVector<TRangeModel>& model, FuzzedDataProvider& fdp, TKeyMode mode) {
    if (model.empty()) {
        return;
    }
    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1);
    TRangeModel range = model[index];
    ui32 key = fdp.ConsumeIntegralInRange<ui32>(range.From, range.ToInfinity ? MaxKeyForMode(mode) : range.To);

    if (!Contains(range, key)) {
        key = range.From;
        if (!range.FromInclusive) {
            return;
        }
    }

    TKeyHolder keyHolder(key, mode);
    cache.InvalidateKey(IteratorAt(cache, index), keyHolder.Ref());

    if (key == range.From) {
        model.erase(model.begin() + index);
    } else {
        range.To = key;
        range.ToInfinity = false;
        range.ToInclusive = false;
        model[index] = range;
    }
}

void QueryRange(TKeyRangeCache& cache, const TVector<TRangeModel>& model, FuzzedDataProvider& fdp, TKeyMode mode) {
    const ui32 key = ConsumeKey(fdp, MaxKeyForMode(mode));
    CheckFindForward(cache, model, key, mode);
    CheckFindReverse(cache, model, key, mode);
    const auto it = IteratorAt(cache, fdp.ConsumeIntegralInRange<size_t>(0, model.size()));
    if (it != cache.end()) {
        TKeyHolder keyHolder(key, mode);
        (void)cache.InsideLeft(it, keyHolder.Ref());
        (void)cache.InsideRight(it, keyHolder.Ref());
    }
}

void CheckNullOrdering(TArrayRef<const NScheme::TTypeInfoOrder> types, TKeyMode mode) {
    TKeyHolder zero(0, mode);
    for (size_t i = 0; i < types.size(); ++i) {
        const TCell nullCell;
        Y_ABORT_UNLESS(CompareTypedCells(nullCell, nullCell, types[i]) == 0);
        Y_ABORT_UNLESS(CompareTypedCells(nullCell, zero.Cells[i], types[i]) < 0);
        Y_ABORT_UNLESS(CompareTypedCells(zero.Cells[i], nullCell, types[i]) > 0);
    }
}

EPgKeyType ConsumePgKeyType(FuzzedDataProvider& fdp) {
    switch (fdp.ConsumeIntegralInRange<unsigned>(0, 5)) {
    case 0:
        return EPgKeyType::Bool;
    case 1:
        return EPgKeyType::Int2;
    case 2:
        return EPgKeyType::Int4;
    case 3:
        return EPgKeyType::Int8;
    case 4:
        return EPgKeyType::Float4;
    case 5:
        return EPgKeyType::Float8;
    }
    Y_ABORT("unreachable");
}

void RunCache(FuzzedDataProvider& fdp) {
    TKeyMode mode;
    mode.Mode = fdp.ConsumeBool() ? EKeyMode::Pg : EKeyMode::Native;
    if (mode.IsPg()) {
        mode.PgType = ConsumePgKeyType(fdp);
    }

    TVector<NScheme::TTypeInfoOrder> types;
    if (mode.IsPg()) {
        types.emplace_back(NScheme::TTypeInfo(NPg::TypeDescFromPgTypeName(PgTypeName(mode.PgType))));
        types.emplace_back(NScheme::TTypeInfo(NPg::TypeDescFromPgTypeName("pgtext")));
    } else {
        types.emplace_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
        types.emplace_back(NScheme::TTypeInfo(NScheme::NTypeIds::String));
    }

    TVector<TCell> defaults(2);
    const TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults = TKeyCellDefaults::Make(types, defaults);
    const TIntrusivePtr<TKeyRangeCacheNeedGCList> gcList = new TKeyRangeCacheNeedGCList();

    TKeyRangeCacheConfig config;
    config.MinRows = fdp.ConsumeIntegralInRange<size_t>(1, 16);
    config.MaxBytes = fdp.ConsumeIntegralInRange<size_t>(512, 8192);

    TKeyRangeCache cache(*keyDefaults, config, gcList);
    TVector<TRangeModel> model;
    TKeyRangeCache::TStats previousStats = cache.Stats();
    CheckNullOrdering(types, mode);

    for (size_t step = 0; step < MaxSteps && fdp.remaining_bytes() > 0; ++step) {
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 10)) {
            case 0:
            case 1:
                AddRange(cache, model, fdp, mode);
                break;
            case 2:
                TouchRange(cache, model, fdp);
                break;
            case 3:
                ExtendLeft(cache, model, fdp, mode);
                break;
            case 4:
                ExtendRight(cache, model, fdp, mode);
                break;
            case 5:
                MergeRanges(cache, model, fdp);
                break;
            case 6:
                InvalidateRange(cache, model, fdp);
                break;
            case 7:
                InvalidateKey(cache, model, fdp, mode);
                break;
            case 8:
                cache.EvictOld();
                model = SnapshotModel(cache, mode);
                break;
            case 9:
                cache.CollectGarbage();
                model = SnapshotModel(cache, mode);
                break;
            case 10:
                gcList->RunGC();
                model = SnapshotModel(cache, mode);
                break;
        }

        if (fdp.ConsumeBool()) {
            QueryRange(cache, model, fdp, mode);
        }
        CheckStatsMonotonic(previousStats, cache.Stats());
        previousStats = cache.Stats();
        CheckCache(cache, model, types, mode);
    }
}

void CheckPool(const TSpecialMemoryPool& pool, size_t expectedUsed) {
    Y_ABORT_UNLESS(pool.TotalUsed() == expectedUsed);
    Y_ABORT_UNLESS(pool.TotalUsed() <= pool.TotalAllocated());
    Y_ABORT_UNLESS(pool.TotalGarbage() <= pool.TotalAllocated());
}

void RunPool(FuzzedDataProvider& fdp) {
    TSpecialMemoryPool pool;
    TVector<TPoolAlloc> allocs;
    size_t expectedUsed = 0;

    for (size_t step = 0; step < MaxSteps && fdp.remaining_bytes() > 0; ++step) {
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 4)) {
            case 0: {
                if (allocs.size() >= MaxPoolAllocs) {
                    break;
                }
                const size_t len = fdp.ConsumeBool()
                    ? fdp.ConsumeIntegralInRange<size_t>(1, 512)
                    : fdp.ConsumeIntegralInRange<size_t>(513, 4096);
                void* ptr = pool.Allocate(len);
                Y_ABORT_UNLESS(ptr);
                Y_ABORT_UNLESS(reinterpret_cast<uintptr_t>(ptr) % alignof(void*) == 0);
                std::memset(ptr, int(step & 0xff), len);
                allocs.push_back({ ptr, len });
                expectedUsed += AlignUp(len);
                break;
            }
            case 1: {
                if (allocs.empty()) {
                    break;
                }
                const size_t index = fdp.ConsumeBool()
                    ? allocs.size() - 1
                    : fdp.ConsumeIntegralInRange<size_t>(0, allocs.size() - 1);
                expectedUsed -= AlignUp(allocs[index].Len);
                pool.Deallocate(allocs[index].Ptr, allocs[index].Len);
                allocs.erase(allocs.begin() + index);
                break;
            }
            case 2: {
                const size_t len = fdp.ConsumeIntegralInRange<size_t>(0, 4096);
                pool.Reserve(len);
                break;
            }
            case 3:
                pool.Clear();
                allocs.clear();
                expectedUsed = 0;
                break;
            case 4:
                CheckPool(pool, expectedUsed);
                break;
        }
        CheckPool(pool, expectedUsed);
    }

    pool.Clear();
    CheckPool(pool, 0);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider poolFdp(data, size);
    RunPool(poolFdp);

    FuzzedDataProvider cacheFdp(data, size);
    RunCache(cacheFdp);
    return 0;
}
