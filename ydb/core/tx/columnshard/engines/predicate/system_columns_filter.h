#pragma once

#include <ydb/core/formats/arrow/filter/filter.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include <optional>
#include <vector>

namespace NKikimr::NOlap {

class TSystemColumnBound {
private:
    bool Unbounded = true;
    bool Inclusive = true;
    Ydb::TypedValue Value;

public:
    static TSystemColumnBound MakeUnbounded() {
        return TSystemColumnBound();
    }

    static TSystemColumnBound Make(const Ydb::TypedValue& value, const bool inclusive);

    bool IsUnbounded() const {
        return Unbounded;
    }

    bool IsInclusive() const {
        return Inclusive;
    }

    const Ydb::TypedValue& GetValue() const {
        return Value;
    }

    bool ToCell(TCell& cell, NScheme::TTypeInfo& type, TString& owned, TString& err) const;
    TString DebugString() const;
};

class TSystemColumnRange {
private:
    TSystemColumnBound From;
    TSystemColumnBound To;

public:
    TSystemColumnRange() = default;
    TSystemColumnRange(TSystemColumnBound from, TSystemColumnBound to);

    const TSystemColumnBound& GetFrom() const {
        return From;
    }

    const TSystemColumnBound& GetTo() const {
        return To;
    }

    bool Contains(const TCell& value, const NScheme::TTypeInfo type) const;
    bool IsPoint() const;
    TString DebugString() const;

    static std::optional<TSystemColumnRange> Intersect(const TSystemColumnRange& l, const TSystemColumnRange& r);
    static bool Overlaps(const TSystemColumnRange& l, const TSystemColumnRange& r);
    static TSystemColumnRange MergeOverlapping(const TSystemColumnRange& l, const TSystemColumnRange& r);
};

class TSystemColumnConstraint {
private:
    ui32 ColumnId = 0;
    TString Name;
    std::vector<TSystemColumnRange> Ranges;

    void Normalize();

public:
    TSystemColumnConstraint() = default;
    TSystemColumnConstraint(const ui32 columnId, TString name, std::vector<TSystemColumnRange> ranges);

    ui32 GetColumnId() const {
        return ColumnId;
    }

    const TString& GetName() const {
        return Name;
    }

    const std::vector<TSystemColumnRange>& GetRanges() const {
        return Ranges;
    }

    bool IsUnsatisfiable() const {
        return Ranges.empty();
    }

    bool Contains(const TCell& value, const NScheme::TTypeInfo type) const;
    TString DebugString() const;

    static TSystemColumnConstraint Intersect(const TSystemColumnConstraint& l, const TSystemColumnConstraint& r);
};

class TSystemColumnsFilter {
private:
    THashMap<ui32, TSystemColumnConstraint> Constraints;

public:
    static TSystemColumnsFilter BuildEmpty() {
        return TSystemColumnsFilter();
    }

    static TSystemColumnsFilter BuildFromProto(const NKqpProto::TKqpPhySystemColumnsFilter& proto);
    void IntersectConstraint(TSystemColumnConstraint constraint);
    void SerializeToProto(NKqpProto::TKqpPhySystemColumnsFilter& proto) const;

    bool IsEmpty() const {
        return Constraints.empty();
    }

    bool HasConstraint(const ui32 columnId) const {
        return Constraints.contains(columnId);
    }

    bool Check(const ui32 columnId, const TCell& value, const NScheme::TTypeInfo type) const;
    bool CheckUint64(const ui32 columnId, const ui64 value) const;
    bool IsUsed(const ui64 portionId, const ui64 tabletId) const;
    bool IsTabletUsed(const ui64 tabletId) const;

    NArrow::TColumnFilter BuildFilter(const ui32 recordsCount, const ui64 portionId, const ui64 tabletId) const;
    TString DebugString() const;
};

}   // namespace NKikimr::NOlap
