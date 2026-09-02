#include "system_columns_filter.h"

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/actors/core/log.h>

#include <util/memory/pool.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NOlap {

namespace {
constexpr ui32 PartitionColumnId = static_cast<ui32>(TKeyDesc::EColumnIdDataShard);
constexpr ui32 PortionColumnId = static_cast<ui32>(TKeyDesc::EColumnIdPortion);
const NScheme::TTypeInfo Uint64Type{ NScheme::NTypeIds::Uint64 };

bool ConvertTypedValueToCell(const Ydb::TypedValue& value, TCell& cell, NScheme::TTypeInfo& type, TString& owned, TString& err) {
    NScheme::TTypeInfoMod typeMod;
    if (!NScheme::TypeInfoFromProto(value.type(), typeMod, err)) {
        return false;
    }
    type = typeMod.TypeInfo;
    TMemoryPool pool(256);
    if (!CellFromProtoVal(type, 0, &value.value(), false, cell, err, pool)) {
        return false;
    }
    if (!cell.IsNull() && !cell.IsInline()) {
        owned.assign(cell.Data(), cell.Size());
        cell = TCell(owned.data(), owned.size());
    }
    return true;
}

const Ydb::TypedValue* GetLiteralValue(const NKqpProto::TKqpPhyValue& value) {
    if (value.GetKindCase() == NKqpProto::TKqpPhyValue::kLiteralValue) {
        return &value.GetLiteralValue();
    }
    return nullptr;
}

std::optional<int> CompareBounded(const TSystemColumnBound& l, const TSystemColumnBound& r) {
    TCell leftCell;
    TCell rightCell;
    NScheme::TTypeInfo leftType;
    NScheme::TTypeInfo rightType;
    TString leftOwned;
    TString rightOwned;
    TString err;
    if (!l.ToCell(leftCell, leftType, leftOwned, err) || !r.ToCell(rightCell, rightType, rightOwned, err)) {
        return std::nullopt;
    }
    if (leftType.GetTypeId() != rightType.GetTypeId()) {
        return std::nullopt;
    }
    return CompareTypedCells(leftCell, rightCell, leftType);
}

TSystemColumnBound MaxFrom(const TSystemColumnBound& l, const TSystemColumnBound& r) {
    if (l.IsUnbounded()) {
        return r;
    }
    if (r.IsUnbounded()) {
        return l;
    }
    const auto cmp = CompareBounded(l, r);
    if (!cmp) {
        return l;
    }
    if (*cmp < 0) {
        return r;
    }
    if (*cmp > 0) {
        return l;
    }
    return TSystemColumnBound::Make(l.GetValue(), l.IsInclusive() && r.IsInclusive());
}

TSystemColumnBound MinTo(const TSystemColumnBound& l, const TSystemColumnBound& r) {
    if (l.IsUnbounded()) {
        return r;
    }
    if (r.IsUnbounded()) {
        return l;
    }
    const auto cmp = CompareBounded(l, r);
    if (!cmp) {
        return l;
    }
    if (*cmp < 0) {
        return l;
    }
    if (*cmp > 0) {
        return r;
    }
    return TSystemColumnBound::Make(l.GetValue(), l.IsInclusive() && r.IsInclusive());
}

bool RangeIsEmpty(const TSystemColumnBound& from, const TSystemColumnBound& to) {
    if (from.IsUnbounded() || to.IsUnbounded()) {
        return false;
    }
    const auto cmp = CompareBounded(from, to);
    if (!cmp) {
        return true;
    }
    if (*cmp > 0) {
        return true;
    }
    if (*cmp < 0) {
        return false;
    }
    return !from.IsInclusive() || !to.IsInclusive();
}

}   // namespace

TSystemColumnBound TSystemColumnBound::Make(const Ydb::TypedValue& value, const bool inclusive) {
    TSystemColumnBound result;
    result.Unbounded = false;
    result.Inclusive = inclusive;
    result.Value = value;
    return result;
}

bool TSystemColumnBound::ToCell(TCell& cell, NScheme::TTypeInfo& type, TString& owned, TString& err) const {
    if (Unbounded) {
        err = "unbounded";
        return false;
    }
    return ConvertTypedValueToCell(Value, cell, type, owned, err);
}

TString TSystemColumnBound::DebugString() const {
    if (Unbounded) {
        return "*";
    }
    return TStringBuilder() << Value.ShortDebugString() << (Inclusive ? "=" : "");
}

TSystemColumnRange::TSystemColumnRange(TSystemColumnBound from, TSystemColumnBound to)
    : From(std::move(from))
    , To(std::move(to))
{
}

bool TSystemColumnRange::Contains(const TCell& value, const NScheme::TTypeInfo type) const {
    if (!From.IsUnbounded()) {
        TCell fromCell;
        NScheme::TTypeInfo fromType;
        TString owned;
        TString err;
        if (!From.ToCell(fromCell, fromType, owned, err) || fromType.GetTypeId() != type.GetTypeId()) {
            return false;
        }
        const int cmp = CompareTypedCells(value, fromCell, type);
        if (cmp < 0 || (cmp == 0 && !From.IsInclusive())) {
            return false;
        }
    }
    if (!To.IsUnbounded()) {
        TCell toCell;
        NScheme::TTypeInfo toType;
        TString owned;
        TString err;
        if (!To.ToCell(toCell, toType, owned, err) || toType.GetTypeId() != type.GetTypeId()) {
            return false;
        }
        const int cmp = CompareTypedCells(value, toCell, type);
        if (cmp > 0 || (cmp == 0 && !To.IsInclusive())) {
            return false;
        }
    }
    return true;
}

bool TSystemColumnRange::IsPoint() const {
    if (From.IsUnbounded() || To.IsUnbounded() || !From.IsInclusive() || !To.IsInclusive()) {
        return false;
    }
    const auto cmp = CompareBounded(From, To);
    return cmp && *cmp == 0;
}

TString TSystemColumnRange::DebugString() const {
    return TStringBuilder() << (From.IsInclusive() || From.IsUnbounded() ? "[" : "(") << From.DebugString() << ";" << To.DebugString()
                            << (To.IsInclusive() || To.IsUnbounded() ? "]" : ")");
}

std::optional<TSystemColumnRange> TSystemColumnRange::Intersect(const TSystemColumnRange& l, const TSystemColumnRange& r) {
    auto from = MaxFrom(l.From, r.From);
    auto to = MinTo(l.To, r.To);
    if (RangeIsEmpty(from, to)) {
        return std::nullopt;
    }
    return TSystemColumnRange(std::move(from), std::move(to));
}

bool TSystemColumnRange::Overlaps(const TSystemColumnRange& l, const TSystemColumnRange& r) {
    return !!Intersect(l, r);
}

TSystemColumnRange TSystemColumnRange::MergeOverlapping(const TSystemColumnRange& l, const TSystemColumnRange& r) {
    TSystemColumnBound from;
    if (l.From.IsUnbounded() || r.From.IsUnbounded()) {
        from = TSystemColumnBound::MakeUnbounded();
    } else {
        const auto cmp = CompareBounded(l.From, r.From);
        if (!cmp || *cmp < 0) {
            from = l.From;
        } else if (*cmp > 0) {
            from = r.From;
        } else {
            from = TSystemColumnBound::Make(l.From.GetValue(), l.From.IsInclusive() || r.From.IsInclusive());
        }
    }
    TSystemColumnBound to;
    if (l.To.IsUnbounded() || r.To.IsUnbounded()) {
        to = TSystemColumnBound::MakeUnbounded();
    } else {
        const auto cmp = CompareBounded(l.To, r.To);
        if (!cmp || *cmp > 0) {
            to = l.To;
        } else if (*cmp < 0) {
            to = r.To;
        } else {
            to = TSystemColumnBound::Make(l.To.GetValue(), l.To.IsInclusive() || r.To.IsInclusive());
        }
    }
    return TSystemColumnRange(std::move(from), std::move(to));
}

TSystemColumnConstraint::TSystemColumnConstraint(const ui32 columnId, TString name, std::vector<TSystemColumnRange> ranges)
    : ColumnId(columnId)
    , Name(std::move(name))
    , Ranges(std::move(ranges))
{
    Normalize();
}

void TSystemColumnConstraint::Normalize() {
    std::vector<TSystemColumnRange> valid;
    valid.reserve(Ranges.size());
    for (auto& range : Ranges) {
        if (!RangeIsEmpty(range.GetFrom(), range.GetTo())) {
            valid.emplace_back(std::move(range));
        }
    }
    std::sort(valid.begin(), valid.end(), [](const TSystemColumnRange& l, const TSystemColumnRange& r) {
        if (l.GetFrom().IsUnbounded()) {
            return !r.GetFrom().IsUnbounded();
        }
        if (r.GetFrom().IsUnbounded()) {
            return false;
        }
        const auto cmp = CompareBounded(l.GetFrom(), r.GetFrom());
        return cmp && *cmp < 0;
    });
    std::vector<TSystemColumnRange> merged;
    for (auto& range : valid) {
        if (merged.empty() || !TSystemColumnRange::Overlaps(merged.back(), range)) {
            merged.emplace_back(std::move(range));
            continue;
        }
        merged.back() = TSystemColumnRange::MergeOverlapping(merged.back(), range);
    }
    Ranges = std::move(merged);
}

bool TSystemColumnConstraint::Contains(const TCell& value, const NScheme::TTypeInfo type) const {
    for (const auto& range : Ranges) {
        if (range.Contains(value, type)) {
            return true;
        }
    }
    return false;
}

TString TSystemColumnConstraint::DebugString() const {
    TStringBuilder sb;
    sb << (!Name.empty() ? Name : ToString(ColumnId)) << "{";
    bool first = true;
    for (const auto& range : Ranges) {
        if (!first) {
            sb << " U ";
        }
        first = false;
        sb << range.DebugString();
    }
    if (Ranges.empty()) {
        sb << "empty";
    }
    sb << "}";
    return sb;
}

TSystemColumnConstraint TSystemColumnConstraint::Intersect(const TSystemColumnConstraint& l, const TSystemColumnConstraint& r) {
    AFL_VERIFY(l.ColumnId == r.ColumnId);
    std::vector<TSystemColumnRange> ranges;
    for (const auto& left : l.Ranges) {
        for (const auto& right : r.Ranges) {
            if (auto item = TSystemColumnRange::Intersect(left, right)) {
                ranges.emplace_back(std::move(*item));
            }
        }
    }
    TString name = !l.Name.empty() ? l.Name : r.Name;
    return TSystemColumnConstraint(l.ColumnId, std::move(name), std::move(ranges));
}

namespace {
TSystemColumnBound BoundFromProto(const NKqpProto::TKqpPhySystemColumnBound& proto) {
    const auto* literal = GetLiteralValue(proto.GetValue());
    if (!literal) {
        return TSystemColumnBound::MakeUnbounded();
    }
    return TSystemColumnBound::Make(*literal, proto.GetInclusive());
}

void BoundToProto(const TSystemColumnBound& bound, NKqpProto::TKqpPhySystemColumnBound& proto) {
    if (bound.IsUnbounded()) {
        return;
    }
    *proto.MutableValue()->MutableLiteralValue() = bound.GetValue();
    proto.SetInclusive(bound.IsInclusive());
}
}   // namespace

TSystemColumnsFilter TSystemColumnsFilter::BuildFromProto(const NKqpProto::TKqpPhySystemColumnsFilter& proto) {
    TSystemColumnsFilter result;
    for (const auto& constraint : proto.GetConstraints()) {
        std::vector<TSystemColumnRange> ranges;
        ranges.reserve(constraint.RangesSize());
        for (const auto& range : constraint.GetRanges()) {
            TSystemColumnBound from = range.HasFrom() ? BoundFromProto(range.GetFrom()) : TSystemColumnBound::MakeUnbounded();
            TSystemColumnBound to = range.HasTo() ? BoundFromProto(range.GetTo()) : TSystemColumnBound::MakeUnbounded();
            ranges.emplace_back(std::move(from), std::move(to));
        }
        result.IntersectConstraint(TSystemColumnConstraint(constraint.GetColumnId(), TString(constraint.GetName()), std::move(ranges)));
    }
    return result;
}

void TSystemColumnsFilter::IntersectConstraint(TSystemColumnConstraint constraint) {
    auto it = Constraints.find(constraint.GetColumnId());
    if (it == Constraints.end()) {
        Constraints.emplace(constraint.GetColumnId(), std::move(constraint));
        return;
    }
    it->second = TSystemColumnConstraint::Intersect(it->second, constraint);
}

void TSystemColumnsFilter::SerializeToProto(NKqpProto::TKqpPhySystemColumnsFilter& proto) const {
    proto.Clear();
    for (const auto& [columnId, constraint] : Constraints) {
        auto* protoConstraint = proto.AddConstraints();
        protoConstraint->SetColumnId(columnId);
        protoConstraint->SetName(constraint.GetName());
        for (const auto& range : constraint.GetRanges()) {
            auto* protoRange = protoConstraint->AddRanges();
            if (!range.GetFrom().IsUnbounded()) {
                BoundToProto(range.GetFrom(), *protoRange->MutableFrom());
            }
            if (!range.GetTo().IsUnbounded()) {
                BoundToProto(range.GetTo(), *protoRange->MutableTo());
            }
        }
    }
}

bool TSystemColumnsFilter::Check(const ui32 columnId, const TCell& value, const NScheme::TTypeInfo type) const {
    auto it = Constraints.find(columnId);
    if (it == Constraints.end()) {
        return true;
    }
    return it->second.Contains(value, type);
}

bool TSystemColumnsFilter::CheckUint64(const ui32 columnId, const ui64 value) const {
    const ui64 copy = value;
    return Check(columnId, TCell::Make(copy), Uint64Type);
}

bool TSystemColumnsFilter::IsUsed(const ui64 portionId, const ui64 tabletId) const {
    return CheckUint64(PortionColumnId, portionId) && CheckUint64(PartitionColumnId, tabletId);
}

bool TSystemColumnsFilter::IsTabletUsed(const ui64 tabletId) const {
    return CheckUint64(PartitionColumnId, tabletId);
}

NArrow::TColumnFilter TSystemColumnsFilter::BuildFilter(const ui32 recordsCount, const ui64 portionId, const ui64 tabletId) const {
    Y_UNUSED(recordsCount);
    return IsUsed(portionId, tabletId) ? NArrow::TColumnFilter::BuildAllowFilter() : NArrow::TColumnFilter::BuildDenyFilter();
}

TString TSystemColumnsFilter::DebugString() const {
    TStringBuilder sb;
    sb << "system_columns_filter{";
    bool first = true;
    for (const auto& [_, constraint] : Constraints) {
        if (!first) {
            sb << ";";
        }
        first = false;
        sb << constraint.DebugString();
    }
    sb << "}";
    return sb;
}

}   // namespace NKikimr::NOlap
