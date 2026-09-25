#include "flat_table_key_blocks.h"

#include "flat_part_index_iter_iface.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/digest/city.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <set>
#include <tuple>

namespace NKikimr {
namespace NTable {

namespace {

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

TKeyBoundary ToBoundary(const TMark& pos) {
    TKeyBoundary boundary;
    if (pos.Inf == EInf::Fin) {
        boundary.Key = pos.Key;
        boundary.Side = pos.Side;
    } else if (pos.Inf == EInf::Neg) {
        boundary.Side = EBoundarySide::Before;
    } else {
        boundary.Side = EBoundarySide::After;
    }
    return boundary;
}

TMark FromSeek(TArrayRef<const TCell> key, bool inclusive) {
    if (!key) {
        return NegInf();
    }
    return {EInf::Fin, TSerializedCellVec(key),
        inclusive ? EBoundarySide::Before : EBoundarySide::After, true};
}

TMark RangeEndOf(const TSplitRequest& request) {
    if (!request.EndKey) {
        return PosInf();
    }
    return {EInf::Fin, request.EndKey,
        request.EndInclusive ? EBoundarySide::After : EBoundarySide::Before, true};
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
    uintptr_t Part = 0;
    ui32 Group = 0;
    ui32 PageId = 0;

    bool operator==(const TPageRef&) const = default;

    template <typename H>
    friend H AbslHashValue(H h, const TPageRef& ref) {
        return H::combine(std::move(h), ref.Part, ref.Group, ref.PageId);
    }
};

struct TPageUse {
    ui64 Bytes = 0;
    ui64 Units = 0;
    bool Certain = false;
};

using TPageUseMap = absl::flat_hash_map<TPageRef, TPageUse>;
using TPageSet = absl::flat_hash_set<TPageRef>;

class TIndexEnv : public IPages {
public:
    IPages* Inner = nullptr;
    TPageSet* AllSeen = nullptr;
    ui64 MaxPages = Max<ui64>();
    bool BudgetHit = false;
    TPageSet Seen;

    TResult Locate(const TMemTable* memTable, ui64 ref, ui32 tag) override {
        return Inner->Locate(memTable, ref, tag);
    }

    TResult Locate(const TPart* part, ui64 ref, ELargeObj lob) override {
        return Inner->Locate(part, ref, lob);
    }

    const TSharedData* TryGetPage(const TPart* part, TPageId pageId, TGroupId groupId) override {
        const TPageRef ref{uintptr_t(part), groupId.Raw(), pageId};
        const bool limited = MaxPages != Max<ui64>();
        if (limited && !Seen.contains(ref) && Seen.size() >= MaxPages) {
            BudgetHit = true;
            return nullptr;
        }
        const auto type = part->GetPageType(pageId, groupId);
        Y_ENSURE(type == EPage::FlatIndex || type == EPage::BTreeIndex,
            "key-block iterator requested a non-index page");
        const TSharedData* page = Inner->TryGetPage(part, pageId, groupId);
        if (page) {
            if (limited) {
                Seen.insert(ref);
            }
            AllSeen->insert(ref);
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
    const TPart* Part = nullptr;
    TPageId PageId = Max<TPageId>();
    TRowId BeginRow = 0;
    TRowId EndRow = 0;
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
            const auto key = target.Inf != EInf::Fin ? TSerializedCellVec()
                : target.SearchKey ? target.Key : ExtendKey(target.Key.GetCells(), keys);
            EReady ready = target.Inf == EInf::Neg ? Iter->Seek(TRowId(0))
                : Iter->Seek(ESeek::Lower, key.GetCells(), &keys);
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
        Page.Part = part;
        Page.PageId = Iter->GetPageId();
        Page.BeginRow = Iter->GetRowId();
        Page.EndRow = Iter->GetNextRowId();
        Page.Bytes = part->GetPageSize(Page.PageId, NPage::TGroupId(0));
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
    struct TWalk {
        TMark Target;
        TMark Start;
        TMark End;
        ui32 Region = 0;
        absl::flat_hash_map<const TPart*, TPageCursor> Main;
    };

    struct TAnchors {
        bool Ready = false;
        TVector<TMark> Cuts;
    };

    struct TRunCursor {
        const TRun* Run;
        TRun::const_iterator First;
    };

    struct TSplitWalk {
        TWalk Walk;
        const TSplitRequest& Request;
        TIndexEnv Env;
        TSplitResult Result;
        TMark Start;
        TMark End;
        ui64 PieceUnits = 0;
        double PieceBytes = 0;
        TPageUseMap PiecePages;
        TPageUseMap UnitPages;
        TVector<TRunCursor> Runs;
    };

    const TSubset* Subset = nullptr;
    TIntrusiveConstPtr<TKeyCellDefaults> Keys;
    TConf Conf;
    const TKeyBlocksLayout* Layout = nullptr;
    TVector<TAnchors> Anchors;
    THolder<TLevels> Levels;

    TKeyBlocksTelemetry Telemetry;
    TPageSet SeenIndex;
    TPageSet SeenData;
    TIndexEnv MainEnv;
    TWalk Read;
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

    void BuildAnchors(ui32 region) {
        auto& data = Anchors[region];
        if (data.Ready) {
            return;
        }
        const TMark start = Canonical(StartOf(Layout->Regions[region].Bounds));
        const TMark end = EndOf(Layout->Regions[region].Bounds);
        for (const auto& mem : Subset->Frozen) {
            auto iter = mem.Snapshot.Iterator();
            const bool found = start.Inf == EInf::Neg
                ? iter.SeekFirst()
                : iter.SeekLowerBound(NMem::TPoint{start.Key.GetCells(), KeyDefaults()});
            if (!found) {
                continue;
            }
            do {
                auto key = ExtendKey({iter.GetKey(), mem->Scheme->Keys->Size()}, KeyDefaults());
                TMark cut = BeforeKey(std::move(key));
                if (CmpPos(cut, end, KeyDefaults()) >= 0) {
                    break;
                }
                if (CmpPos(start, cut, KeyDefaults()) >= 0) {
                    continue;
                }
                const auto& buf = cut.Key.GetBuffer();
                if (CityHash64WithSeed(buf.data(), buf.size(), Conf.AnchorSalt) % Conf.MemtableStride == 0) {
                    data.Cuts.push_back(std::move(cut));
                }
            } while (iter.Next());
        }
        std::sort(data.Cuts.begin(), data.Cuts.end(), [&](const TMark& a, const TMark& b) {
            return CmpPos(a, b, KeyDefaults()) < 0;
        });
        data.Cuts.erase(std::unique(data.Cuts.begin(), data.Cuts.end(), [&](const TMark& a, const TMark& b) {
            return CmpPos(a, b, KeyDefaults()) == 0;
        }), data.Cuts.end());
        data.Ready = true;
    }

    EReady Locate(TWalk& walk, IPages* env) {
        if (walk.Target.Inf == EInf::Pos) {
            return EReady::Gone;
        }
        walk.Region = FindRegion(walk.Target);
        const auto& region = Layout->Regions[walk.Region];
        walk.Start = Canonical(StartOf(region.Bounds));
        walk.End = Canonical(EndOf(region.Bounds));
        if (region.Owner) {
            auto& cursor = walk.Main[region.Owner];
            const EReady ready = cursor.Position(walk.Target, env, region.Owner, KeyDefaults());
            if (ready != EReady::Data) {
                return ready;
            }
            walk.Start = Later(walk.Start, cursor.Page.Begin);
            walk.End = Earlier(walk.End, cursor.Page.End);
        } else {
            BuildAnchors(walk.Region);
            const auto& cuts = Anchors[walk.Region].Cuts;
            auto it = std::upper_bound(cuts.begin(), cuts.end(), walk.Target,
                [&](const TMark& target, const TMark& cut) { return CmpPos(target, cut, KeyDefaults()) < 0; });
            if (it != cuts.begin()) {
                walk.Start = *std::prev(it);
            }
            if (it != cuts.end()) {
                walk.End = *it;
            }
        }
        Y_ENSURE(CmpPos(walk.Start, walk.Target, KeyDefaults()) <= 0);
        Y_ENSURE(CmpPos(walk.Target, walk.End, KeyDefaults()) < 0);
        return EReady::Data;
    }

    void AddDataBytes(const TPageSpan& page, const TPart* owner) {
        if (!SeenData.insert({uintptr_t(page.Part), 0, page.PageId}).second) {
            return;
        }
        if (page.Part == owner) {
            Telemetry.OwnerMainGroupBytes += page.Bytes;
        } else {
            Telemetry.OtherMainGroupBytes += page.Bytes;
        }
    }

    void ResetVisits() {
        Telemetry.UnitsTotal = 0;
        Telemetry.UnitsMemtable = 0;
        Telemetry.OwnerRowsPerUnitMax = 0;
        Telemetry.OwnerMainGroupBytes = 0;
        Telemetry.OtherMainGroupBytes = 0;
        SeenData.clear();
    }

    EReady FinishRead() {
        Ready = Locate(Read, &MainEnv);
        if (Ready != EReady::Data) {
            return Ready;
        }
        const auto* owner = Layout->Regions[Read.Region].Owner;
        Block.Bounds = BoundsOf(Read.Start, Read.End);
        Block.SelectionKey = SelectionOf(Read.Start);
        Block.FromMemtable = !owner;
        Block.OwnerRows = 0;
        if (owner) {
            const auto& page = Read.Main[owner].Page;
            Block.OwnerRows = page.EndRow - page.BeginRow;
            AddDataBytes(page, owner);
        }
        ++Telemetry.UnitsTotal;
        Telemetry.UnitsMemtable += Block.FromMemtable;
        Telemetry.OwnerRowsPerUnitMax = Max(Telemetry.OwnerRowsPerUnitMax, Block.OwnerRows);
        return EReady::Data;
    }

    void StartChargeRuns(TSplitWalk& split) {
        if (!Levels) {
            // Match the runs used by the data reader. Disjoint parts share a
            // run, and a read in a gap still probes that run's next slice.
            TVector<size_t> order;
            for (size_t i = 0; i < Subset->Flatten.size(); ++i) {
                order.push_back(i);
            }
            std::sort(order.begin(), order.end(), [&](size_t a, size_t b) {
                const auto& left = Subset->Flatten[a];
                const auto& right = Subset->Flatten[b];
                return std::tie(left->Epoch, left->Label) < std::tie(right->Epoch, right->Label);
            });
            Levels = MakeHolder<TLevels>(Keys);
            for (size_t i : order) {
                const auto& view = Subset->Flatten[i];
                Levels->Add(view.Part, SlicesOf(view));
            }
        }
        for (const auto& run : *Levels) {
            const auto first = split.Start.Inf == EInf::Neg ? run.begin()
                : split.Start.Side == EBoundarySide::After
                    ? run.UpperBound(split.Start.Key.GetCells())
                    : run.LowerBound(split.Start.Key.GetCells());
            if (first != run.end()) {
                split.Runs.push_back({&run, first});
            }
        }
    }

    void AddMainPage(TSplitWalk& split, const TPageSpan& page, bool certain) {
        auto& use = split.UnitPages[{uintptr_t(page.Part), 0, page.PageId}];
        use.Bytes = page.Bytes;
        use.Certain |= certain;
        AddDataBytes(page, Layout->Regions[split.Walk.Region].Owner);
    }

    // Walk each run over [start, end), including one range-end probe.
    EReady ChargeRange(TSplitWalk& split, const TMark& start, const TMark& end, bool certain) {
        if (CmpPos(start, end, KeyDefaults()) >= 0) {
            return EReady::Data;
        }
        for (size_t i = 0; i < split.Runs.size();) {
            auto& run = split.Runs[i];
            auto sliceIt = run.First;
            while (sliceIt != run.Run->end()
                && CmpPos(EndOf(sliceIt->Slice), start, KeyDefaults()) <= 0)
            {
                ++sliceIt;
            }
            if (!certain) {
                run.First = sliceIt;
            }
            if (sliceIt == run.Run->end()) {
                if (!certain) {
                    // This run cannot contribute to any later unit.
                    run = split.Runs.back();
                    split.Runs.pop_back();
                } else {
                    ++i;
                }
                continue;
            }
            TMark target = Later(start, Canonical(StartOf(sliceIt->Slice)));
            for (;;) {
                const auto& slice = sliceIt->Slice;
                auto& main = split.Walk.Main[sliceIt->Part.Get()];
                const EReady ready = main.Position(target, &split.Env, sliceIt->Part.Get(), KeyDefaults());
                if (ready == EReady::Page) {
                    return ready;
                }
                Y_ENSURE(ready == EReady::Data, "nonempty slice has no main page");
                if (main.Page.EndRow <= slice.BeginRowId()) {
                    // An exclusive first key can sit on the preceding page.
                    // The reader starts at the slice's first physical row.
                    target = main.Page.End;
                    continue;
                }
                AddMainPage(split, main.Page, certain);
                if (CmpPos(target, end, KeyDefaults()) >= 0) {
                    break;
                }

                // The last overlapping page may have no row reaching end.
                // Charge a successor even across a sparse gap or slice edge.
                const auto sliceEndRow = slice.LastRowId == Max<TRowId>()
                    ? Max<TRowId>() : slice.EndRowId();
                if (main.Page.End.Inf != EInf::Pos
                    && main.Page.EndRow < sliceEndRow
                    && CmpPos(main.Page.End, EndOf(slice), KeyDefaults()) < 0)
                {
                    target = main.Page.End;
                } else {
                    if (++sliceIt == run.Run->end()) {
                        break;
                    }
                    target = Canonical(StartOf(sliceIt->Slice));
                }
            }
            ++i;
        }
        return EReady::Data;
    }

    EReady ChargeUnit(TSplitWalk& split) {
        split.UnitPages.clear();
        const TMark start = Later(split.Walk.Start, split.Start);
        const TMark end = Earlier(split.Walk.End, split.End);
        const EReady ready = ChargeRange(split, start, end, false);
        if (ready != EReady::Data || !split.Request.Certain) {
            return ready;
        }
        return ChargeRange(split, Later(start, StartOf(*split.Request.Certain)),
            Earlier(end, EndOf(*split.Request.Certain)), true);
    }

    static double Contribution(const TPageUse& use, double rate) {
        // A page shared by m independent units is read with probability 1-(1-rate)^m.
        const double probability = use.Certain || rate == 1.0
            ? 1.0 : -std::expm1(double(use.Units) * std::log1p(-rate));
        return double(use.Bytes) * probability;
    }

    // Only pages touched by this unit change their marginal probability.
    static void AddUnit(TSplitWalk& split) {
        for (const auto& [ref, unitUse] : split.UnitPages) {
            auto& use = split.PiecePages[ref];
            split.PieceBytes -= Contribution(use, split.Request.Rate);
            use.Bytes = unitUse.Bytes;
            ++use.Units;
            use.Certain |= unitUse.Certain;
            split.PieceBytes += Contribution(use, split.Request.Rate);
        }
    }

    void EmitSplit(TSplitWalk& split, const TMark& pos) {
        if (CmpPos(split.Start, pos, KeyDefaults()) < 0
            && CmpPos(pos, split.End, KeyDefaults()) < 0)
        {
            split.Result.Keys.push_back(ToBoundary(pos));
        }
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
    State->MainEnv.Inner = env;
    State->MainEnv.AllSeen = &State->SeenIndex;
    State->Anchors.resize(layout.Regions.size());
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
    telemetry.IndexPagesTouched = State->SeenIndex.size();
    return telemetry;
}

bool TKeyBlockIterator::HasColdParts() const {
    return !State->Subset->ColdParts.empty();
}

EReady TKeyBlockIterator::Seek(TArrayRef<const TCell> key, bool inclusive) {
    State->Read.Target = FromSeek(key, inclusive);
    State->ResetVisits();
    return State->FinishRead();
}

EReady TKeyBlockIterator::Next() {
    Y_ENSURE(State->Ready != EReady::Page, "Next after Page requires Seek at the saved position");
    if (State->Ready == EReady::Gone) {
        return EReady::Gone;
    }
    State->Read.Target = State->Read.End;
    return State->FinishRead();
}

EReady TKeyBlockIterator::SplitPoints(const TSplitRequest& request, TSplitResult& out) {
    Y_ENSURE(std::isfinite(request.Rate) && request.Rate > 0.0 && request.Rate <= 1.0,
        "sampling rate must be in (0, 1]");
    Y_ENSURE(IsValid(), "SplitPoints requires a positioned iterator");
    TState::TSplitWalk split{.Request = request};
    split.Start = State->Read.Target;
    split.End = RangeEndOf(request);
    Y_ENSURE(CmpPos(split.Start, split.End, State->KeyDefaults()) <= 0,
        "SplitPoints end is before the current position");
    if (CmpPos(split.End, State->Read.End, State->KeyDefaults()) <= 0) {
        // A singleton has no interior boundary and is exempt from both budgets.
        out = {};
        return EReady::Data;
    }
    split.Walk.Target = split.Start;
    split.Env.Inner = State->MainEnv.Inner;
    split.Env.AllSeen = &State->SeenIndex;
    split.Env.MaxPages = request.MaxIndexPages;
    State->StartChargeRuns(split);

    while (CmpPos(split.Walk.Target, split.End, State->KeyDefaults()) < 0) {
        EReady ready = State->Locate(split.Walk, &split.Env);
        if (ready == EReady::Data) {
            ready = State->ChargeUnit(split);
        }
        if (ready == EReady::Page) {
            if (!split.Env.BudgetHit) {
                return EReady::Page;
            }
            // Keep the first unit's suffix as one exempt piece. Seek already
            // found its end, even when this walk has a zero index budget.
            State->EmitSplit(split, CmpPos(split.Walk.Target, split.Start, State->KeyDefaults()) == 0
                ? State->Read.End : split.Walk.Target);
            split.Result.Truncated = true;
            break;
        }
        if (ready == EReady::Gone) {
            break;
        }
        State->AddUnit(split);
        if (split.PieceUnits && split.PieceBytes > double(request.MaxExpectedBytes)) {
            State->EmitSplit(split, split.Walk.Start);
            split.PiecePages.clear();
            split.PieceBytes = 0;
            split.PieceUnits = 0;
            State->AddUnit(split);
        }
        ++split.PieceUnits;
        split.Walk.Target = split.Walk.End;
    }
    out = std::move(split.Result);
    return EReady::Data;
}

}
}
