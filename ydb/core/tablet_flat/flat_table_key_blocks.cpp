#include "flat_table_key_blocks.h"

#include "flat_part_index_iter_iface.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/digest/city.h>

#include <algorithm>
#include <cstring>
#include <set>
#include <tuple>

namespace NKikimr {
namespace NTable {

namespace {

enum class EBoundarySide : ui8 {
    Before = 0,
    After = 1,
};

enum class EInf : i8 {
    Neg = -1,
    Fin = 0,
    Pos = 1,
};

struct TMark {
    EInf Inf = EInf::Neg;
    TSerializedCellVec Key;
    EBoundarySide Side = EBoundarySide::Before;
    // Missing search cells denote +inf; stored keys use schema defaults.
    bool SearchKey = false;
};

TMark NegInf() {
    return {};
}

TMark PosInf() {
    TMark pos;
    pos.Inf = EInf::Pos;
    return pos;
}

TMark BeforeKey(TSerializedCellVec key) {
    return {EInf::Fin, std::move(key), EBoundarySide::Before};
}

int CmpPos(const TMark& left, const TMark& right, const TKeyCellDefaults& keys) {
    if (left.Inf != right.Inf) {
        return int(left.Inf) < int(right.Inf) ? -1 : 1;
    }
    if (left.Inf != EInf::Fin) {
        return 0;
    }
    const auto leftCells = left.Key.GetCells();
    const auto rightCells = right.Key.GetCells();
    Y_ENSURE(leftCells.size() <= keys.Size() && rightCells.size() <= keys.Size());
    for (size_t i = 0; i < keys.Size(); ++i) {
        const bool leftInf = left.SearchKey && i >= leftCells.size();
        const bool rightInf = right.SearchKey && i >= rightCells.size();
        if (leftInf || rightInf) {
            if (leftInf != rightInf) {
                return leftInf ? 1 : -1;
            }
            break;
        }
        const auto& leftCell = i < leftCells.size() ? leftCells[i] : keys[i];
        const auto& rightCell = i < rightCells.size() ? rightCells[i] : keys[i];
        if (int cmp = CompareTypedCells(leftCell, rightCell, keys.Types[i])) {
            return cmp;
        }
    }
    return int(left.Side) - int(right.Side);
}

TBounds BoundsOf(const TMark& start, const TMark& end) {
    TBounds bounds;
    if (start.Inf == EInf::Fin) {
        bounds.FirstKey = start.Key;
        bounds.FirstInclusive = start.Side == EBoundarySide::Before;
    }
    if (end.Inf == EInf::Fin) {
        bounds.LastKey = end.Key;
        bounds.LastInclusive = end.Side == EBoundarySide::After;
    }
    return bounds;
}

TMark StartOf(const TBounds& bounds) {
    if (!bounds.FirstKey) {
        return NegInf();
    }
    return {EInf::Fin, bounds.FirstKey,
        bounds.FirstInclusive ? EBoundarySide::Before : EBoundarySide::After};
}

TMark EndOf(const TBounds& bounds) {
    if (!bounds.LastKey) {
        return PosInf();
    }
    return {EInf::Fin, bounds.LastKey,
        bounds.LastInclusive ? EBoundarySide::After : EBoundarySide::Before};
}

TString SelectionOf(const TMark& start) {
    if (start.Inf != EInf::Fin) {
        return TString(1, '\0');
    }
    TString out;
    out.push_back(start.Side == EBoundarySide::Before ? char(0x01) : char(0x02));
    out += start.Key.GetBuffer();
    return out;
}

TMark FromSeek(TArrayRef<const TCell> key, bool inclusive) {
    if (!key) {
        return NegInf();
    }
    return {EInf::Fin, TSerializedCellVec(key),
        inclusive ? EBoundarySide::Before : EBoundarySide::After, true};
}

void PutU8(TString& out, ui8 value) {
    out.push_back(char(value));
}

void PutU32(TString& out, ui32 value) {
    char buf[sizeof(value)];
    memcpy(buf, &value, sizeof(value));
    out.append(buf, sizeof(buf));
}

void PutU64(TString& out, ui64 value) {
    char buf[sizeof(value)];
    memcpy(buf, &value, sizeof(value));
    out.append(buf, sizeof(buf));
}

void PutBuf(TString& out, TStringBuf bytes) {
    PutU32(out, ui32(bytes.size()));
    out.append(bytes);
}

TIntrusiveConstPtr<TSlices> SlicesOf(const TPartView& view) {
    Y_ENSURE(view.Slices, "part has no slices");
    return view.Slices;
}

struct TPageRef {
    TLogoBlobID Label;
    ui32 Group = 0;
    TPageOffset Offset;

    bool operator==(const TPageRef&) const = default;

    template <typename H>
    friend H AbslHashValue(H h, const TPageRef& ref) {
        const ui64* raw = ref.Label.GetRaw();
        return H::combine(std::move(h), raw[0], raw[1], raw[2], ref.Group, static_cast<size_t>(ref.Offset));
    }
};

using TPageSet = absl::flat_hash_set<TPageRef>;

class TIndexEnv : public IPages {
public:
    IPages* Inner = nullptr;
    TPageSet Seen;

    TResult Locate(const TMemTable* memTable, ui64 ref, ui32 tag) override {
        return Inner->Locate(memTable, ref, tag);
    }

    TResult Locate(const TPart* part, ui64 ref, ELargeObj lob) override {
        return Inner->Locate(part, ref, lob);
    }

    const TSharedData* TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) override {
        const auto type = location.Type;
        Y_ENSURE(type == EPage::FlatIndex || type == EPage::BTreeIndex || type == EPage::BTreeIndexV2,
            "key-block iterator requested a non-index page");
        const TSharedData* page = Inner->TryGetPage(part, location, groupId);
        if (page) {
            Seen.insert({ part->Label, groupId.Raw(), location.Offset });
        }
        return page;
    }
};

TSerializedCellVec ExtendKey(TArrayRef<const TCell> cells, const TKeyCellDefaults& keys) {
    Y_ENSURE(cells.size() <= keys.Size());
    TSmallVec<TCell> full(cells.begin(), cells.end());
    while (full.size() < keys.Size()) {
        full.push_back(keys[full.size()]);
    }
    return TSerializedCellVec(full);
}

TMark PageBegin(IPartGroupIndexIter& iter, const TKeyCellDefaults& keys) {
    if (!iter.GetKeyCellsCount() || iter.GetRowId() == 0) {
        return NegInf();
    }
    TSmallVec<TCell> cells;
    iter.GetKeyCells(cells);
    return BeforeKey(ExtendKey(cells, keys));
}

struct TPageSpan {
    TMark Begin;
    TMark End;
    TPageOffset Offset;
    TRowId Rows = 0;
    ui64 Bytes = 0;
};

// The cursor keeps its iterator on the next page so that Page.End is known.
struct TPageCursor {
    TPageSpan Page;

    EReady Position(const TMark& target, IPages* env, const TPart* part,
            const TKeyCellDefaults& keys)
    {
        if (target.Inf == EInf::Pos) {
            return EReady::Gone;
        }
        if (!Iter) {
            Iter = CreateIndexIter(part, env, NPage::TGroupId(0));
        }
        if (Valid && CmpPos(Page.Begin, target, keys) <= 0 && CmpPos(target, Page.End, keys) < 0) {
            return EReady::Data;
        }
        const bool next = Valid && Page.End.Inf == EInf::Fin && CmpPos(target, Page.End, keys) == 0;
        Valid = false;
        if (!next) {
            EReady ready = target.Inf == EInf::Neg ? Iter->Seek(TRowId(0))
                : Iter->Seek(ESeek::Lower, target.Key.GetCells(), &keys);
            if (ready == EReady::Gone && target.Inf == EInf::Fin) {
                // A flat index can report Gone past its last row key,
                // although the final page span extends to +inf.
                ready = Iter->SeekLast();
            }
            if (ready != EReady::Data) {
                return ready;
            }
        }
        Page = {};
        const auto location = Iter->GetLocation();
        Page.Offset = location.Offset;
        Page.Rows = Iter->GetNextRowId() - Iter->GetRowId();
        Page.Bytes = location.Size;
        Page.Begin = PageBegin(*Iter, keys);
        const EReady ready = Iter->Next();
        if (ready == EReady::Page) {
            return ready;
        }
        Page.End = ready == EReady::Data ? PageBegin(*Iter, keys) : PosInf();
        Y_ENSURE(CmpPos(Page.Begin, target, keys) <= 0
            && CmpPos(target, Page.End, keys) < 0,
            "index seek did not find the containing page span");
        Valid = true;
        return EReady::Data;
    }

private:
    THolder<IPartGroupIndexIter> Iter;
    bool Valid = false;
};

struct TOwnerLess {
    bool operator()(const TPart* left, const TPart* right) const {
        if (left->Stat.Rows != right->Stat.Rows) {
            return left->Stat.Rows > right->Stat.Rows;
        }
        return left->Label < right->Label;
    }
};

} // namespace

struct TKeyBlockIterator::TState {
    const TSubset* Subset = nullptr;
    TIntrusiveConstPtr<TKeyCellDefaults> Keys;
    TConf Conf;
    const TKeyBlocksLayout* Layout = nullptr;

    TKeyBlocksTelemetry Telemetry;
    TPageSet SeenData;
    TIndexEnv Env;
    absl::flat_hash_map<const TPart*, TPageCursor> Cursors;
    TMark Target;
    TKeyBlock Block;
    EReady Ready = EReady::Gone;

    const TKeyCellDefaults& KeyDefaults() const {
        return *Keys;
    }

    TMark Canonical(TMark mark) const {
        if (mark.Inf == EInf::Fin && !mark.SearchKey) {
            mark.Key = ExtendKey(mark.Key.GetCells(), KeyDefaults());
        }
        return mark;
    }

    TMark Earlier(const TMark& a, const TMark& b) const {
        return CmpPos(a, b, KeyDefaults()) < 0 ? a : b;
    }

    TMark Later(const TMark& a, const TMark& b) const {
        return CmpPos(a, b, KeyDefaults()) < 0 ? b : a;
    }

    ui32 FindRegion(const TMark& target) const {
        const auto& regions = Layout->Regions;
        ui32 lo = 0;
        ui32 hi = regions.size();
        while (lo + 1 < hi) {
            const ui32 mid = (lo + hi) / 2;
            if (CmpPos(StartOf(regions[mid].Bounds), target, KeyDefaults()) <= 0) {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    TMark MemtableCut(const TMemTableSnapshot& mem, const NMem::TTreeIterator& iter) {
        auto key = ExtendKey({iter.GetKey(), mem->Scheme->Keys->Size()}, KeyDefaults());
        return BeforeKey(std::move(key));
    }

    bool IsAnchor(const TMark& cut) const {
        const auto& buf = cut.Key.GetBuffer();
        return CityHash64WithSeed(buf.data(), buf.size(), Conf.AnchorSalt) % Conf.MemtableStride == 0;
    }

    // Search outward from the target; anchors found in one memtable bound the others.
    void PlaceMemtableUnit(TMark& start, TMark& end) {
        const NMem::TPoint point{Target.Key.GetCells(), KeyDefaults()};
        for (const auto& mem : Subset->Frozen) {
            auto iter = mem.Snapshot.Iterator();
            if (Target.Inf != EInf::Neg && iter.SeekUpperBound(point, /* backwards */ true)) {
                do {
                    ++Telemetry.MemtableKeysVisited;
                    TMark cut = MemtableCut(mem, iter);
                    if (CmpPos(cut, start, KeyDefaults()) <= 0) {
                        break;
                    }
                    if (IsAnchor(cut)) {
                        start = std::move(cut);
                        break;
                    }
                } while (iter.Prev());
            }
            if (Target.Inf == EInf::Neg ? iter.SeekFirst() : iter.SeekUpperBound(point)) {
                do {
                    ++Telemetry.MemtableKeysVisited;
                    TMark cut = MemtableCut(mem, iter);
                    if (CmpPos(cut, end, KeyDefaults()) >= 0) {
                        break;
                    }
                    if (IsAnchor(cut)) {
                        end = std::move(cut);
                        break;
                    }
                } while (iter.Next());
            }
        }
    }

    EReady ReadUnit() {
        if (Target.Inf == EInf::Pos) {
            return Ready = EReady::Gone;
        }
        const auto& region = Layout->Regions[FindRegion(Target)];
        TMark start = Canonical(StartOf(region.Bounds));
        TMark end = Canonical(EndOf(region.Bounds));
        Block.OwnerRows = 0;
        if (const auto* owner = region.Owner) {
            auto& cursor = Cursors[owner];
            Ready = cursor.Position(Target, &Env, owner, KeyDefaults());
            if (Ready != EReady::Data) {
                return Ready;
            }
            const auto& page = cursor.Page;
            start = Later(start, page.Begin);
            end = Earlier(end, page.End);
            Block.OwnerRows = page.Rows;
            if (SeenData.insert({ owner->Label, 0, page.Offset }).second) {
                Telemetry.OwnerMainGroupBytes += page.Bytes;
            }
        } else {
            PlaceMemtableUnit(start, end);
        }
        Y_ENSURE(CmpPos(start, Target, KeyDefaults()) <= 0);
        Y_ENSURE(CmpPos(Target, end, KeyDefaults()) < 0);
        Block.Bounds = BoundsOf(start, end);
        Block.SelectionKey = SelectionOf(start);
        Block.FromMemtable = !region.Owner;
        ++Telemetry.UnitsTotal;
        Telemetry.UnitsMemtable += Block.FromMemtable;
        Telemetry.OwnerRowsPerUnitMax = Max(Telemetry.OwnerRowsPerUnitMax, Block.OwnerRows);
        return Ready = EReady::Data;
    }
};

TKeyBlockIterator::~TKeyBlockIterator() = default;

TKeyBlocksLayout TKeyBlockIterator::BuildLayout(
        const TSubset& subset,
        TConf conf,
        TIntrusiveConstPtr<TKeyCellDefaults> keys)
{
    Y_ENSURE(keys, "key defaults are required to build a key-block layout");
    Y_ENSURE(conf.MemtableStride > 0, "memtable stride must be positive");

    struct TEvent {
        TMark Pos;
        bool Open = false;
        const TPart* Part;
    };

    TVector<TEvent> events;
    for (const auto& view : subset.Flatten) {
        Y_ENSURE(view.Part, "flatten part is empty");
        const auto slices = SlicesOf(view);
        for (const auto& slice : *slices) {
            events.push_back({StartOf(slice), true, view.Part.Get()});
            events.push_back({EndOf(slice), false, view.Part.Get()});
        }
    }
    std::sort(events.begin(), events.end(), [&](const TEvent& left, const TEvent& right) {
        return CmpPos(left.Pos, right.Pos, *keys) < 0;
    });

    // Disjoint row ranges may still overlap in key space: [1,4) and (3,6].
    // Keep one active entry per slice, even when they belong to the same part.
    std::multiset<const TPart*, TOwnerLess> active;
    auto apply = [&](const TEvent& event) {
        if (event.Open) {
            active.insert(event.Part);
        } else {
            const auto it = active.find(event.Part);
            Y_ENSURE(it != active.end(), "closing a slice that is not active");
            active.erase(it);
        }
    };
    auto best = [&]() -> const TPart* {
        return active.empty() ? nullptr : *active.begin();
    };

    TKeyBlocksLayout layout;
    auto emit = [&](const TMark& start, const TMark& end, const TPart* owner) {
        if (CmpPos(start, end, *keys) >= 0) {
            return;
        }
        layout.Regions.push_back({BoundsOf(start, end), owner});
    };

    size_t index = 0;
    while (index < events.size() && events[index].Pos.Inf == EInf::Neg) {
        apply(events[index++]);
    }
    const TPart* owner = best();
    TMark start = NegInf();
    while (index < events.size()) {
        if (events[index].Pos.Inf == EInf::Pos) {
            break;
        }
        const TMark pos = events[index].Pos;
        while (index < events.size() && CmpPos(events[index].Pos, pos, *keys) == 0) {
            apply(events[index++]);
        }
        const TPart* after = best();
        if (owner != after) {
            emit(start, pos, owner);
            start = pos;
            owner = after;
        }
    }
    emit(start, PosInf(), owner);
    Y_ENSURE(!layout.Regions.empty(), "key-block layout did not cover the key space");

    TString canon;
    PutU32(canon, 3);
    PutU32(canon, conf.MemtableStride);
    PutU64(canon, conf.AnchorSalt);
    // The effective schema controls separator comparison and the extension of
    // old memtable keys before hashing them into anchors.
    PutU32(canon, ui32(keys->Size()));
    for (const auto& order : keys->Types) {
        const auto type = order.ToTypeInfo();
        PutU32(canon, type.GetTypeId());
        PutU8(canon, order.IsDescending() ? 1 : 0);
        switch (type.GetTypeId()) {
            case NScheme::NTypeIds::Pg:
                PutU32(canon, NPg::PgTypeIdFromTypeDesc(type.GetPgTypeDesc()));
                break;
            case NScheme::NTypeIds::Decimal:
                PutU32(canon, type.GetDecimalType().GetPrecision());
                PutU32(canon, type.GetDecimalType().GetScale());
                break;
            default:
                break;
        }
    }
    PutBuf(canon, TSerializedCellVec(keys->Defs).GetBuffer());

    TVector<const TPartView*> parts;
    parts.reserve(subset.Flatten.size());
    for (const auto& view : subset.Flatten) {
        parts.push_back(&view);
    }
    std::sort(parts.begin(), parts.end(), [](const TPartView* left, const TPartView* right) {
        return std::tie(left->Part->Label, left->Part->Epoch) < std::tie(right->Part->Label, right->Part->Epoch);
    });
    PutU32(canon, ui32(parts.size()));
    for (const auto* view : parts) {
        canon.append(view->Part->Label.AsBinaryString());
        PutU64(canon, static_cast<ui64>(view->Part->Epoch.ToProto()));
        const auto slices = SlicesOf(*view);
        PutU32(canon, ui32(slices->size()));
        for (const auto& slice : *slices) {
            PutBuf(canon, slice.FirstKey.GetBuffer());
            PutU8(canon, slice.FirstInclusive ? 1 : 0);
            PutBuf(canon, slice.LastKey.GetBuffer());
            PutU8(canon, slice.LastInclusive ? 1 : 0);
        }
    }

    TVector<std::pair<TLogoBlobID, i64>> cold;
    cold.reserve(subset.ColdParts.size());
    for (const auto& part : subset.ColdParts) {
        cold.push_back({part->Label, part->Epoch.ToProto()});
    }
    std::sort(cold.begin(), cold.end());
    PutU32(canon, ui32(cold.size()));
    for (const auto& [label, epoch] : cold) {
        canon.append(label.AsBinaryString());
        PutU64(canon, static_cast<ui64>(epoch));
        PutU8(canon, 1);
    }

    PutU32(canon, ui32(subset.Frozen.size()));
    for (const auto& mem : subset.Frozen) {
        PutU64(canon, static_cast<ui64>(mem->Epoch.ToProto()));
        // ScanSnapshot excludes uncommitted table changes, so keys only
        // accumulate within an epoch. Value updates do not change anchors.
        PutU64(canon, mem.Snapshot.Iterator().Size());
    }

    const uint128 hash = CityHash128(canon.data(), canon.size());
    char raw[16];
    memcpy(raw, &hash.first, 8);
    memcpy(raw + 8, &hash.second, 8);
    layout.LayoutId.assign(raw, 16);
    return layout;
}

TKeyBlockIterator::TKeyBlockIterator(
        const TSubset& subset,
        IPages* env,
        TIntrusiveConstPtr<TKeyCellDefaults> keys,
        TConf conf,
        const TKeyBlocksLayout& layout)
    : State(new TState)
{
    Y_ENSURE(keys, "key defaults are required");
    Y_ENSURE(env, "page env is required");
    Y_ENSURE(conf.MemtableStride > 0, "memtable stride must be positive");
    Y_ENSURE(!layout.Regions.empty(), "key-block layout is empty");

    State->Subset = &subset;
    State->Keys = std::move(keys);
    State->Conf = conf;
    State->Layout = &layout;
    State->Env.Inner = env;
    State->Telemetry.Parts = subset.Flatten.size();
    State->Telemetry.Memtables = subset.Frozen.size();
    for (const auto& view : subset.Flatten) {
        State->Telemetry.Slices += SlicesOf(view)->size();
    }
}

bool TKeyBlockIterator::IsValid() const {
    return State->Ready == EReady::Data;
}

const TKeyBlock& TKeyBlockIterator::Get() const {
    Y_ENSURE(IsValid(), "key-block iterator is not positioned");
    return State->Block;
}

TKeyBlocksTelemetry TKeyBlockIterator::Telemetry() const {
    auto telemetry = State->Telemetry;
    telemetry.IndexPagesTouched = State->Env.Seen.size();
    return telemetry;
}

bool TKeyBlockIterator::HasColdParts() const {
    return !State->Subset->ColdParts.empty();
}

EReady TKeyBlockIterator::Seek(TArrayRef<const TCell> key, bool inclusive) {
    State->Target = FromSeek(key, inclusive);
    State->Telemetry.UnitsTotal = 0;
    State->Telemetry.UnitsMemtable = 0;
    return State->ReadUnit();
}

EReady TKeyBlockIterator::Next() {
    Y_ENSURE(State->Ready != EReady::Page, "Next after Page requires Seek at the saved position");
    if (State->Ready == EReady::Gone) {
        return EReady::Gone;
    }
    State->Target = EndOf(State->Block.Bounds);
    return State->ReadUnit();
}

}
}
