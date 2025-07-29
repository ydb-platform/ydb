#pragma once

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/range_treap/range_treap.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TRowRange {
private:
    YDB_READONLY_DEF(ui64, Begin);
    YDB_READONLY_DEF(ui64, End);

public:
    TRowRange(const ui64 begin, const ui64 end)
        : Begin(begin)
        , End(end)
    {
        AFL_VERIFY(end >= begin);
    }

    std::partial_ordering operator<=>(const TRowRange& other) const {
        return std::tie(Begin, End) <=> std::tie(other.Begin, other.End);
    }
    bool operator==(const TRowRange& other) const {
        return (*this <=> other) == std::partial_ordering::equivalent;
    }

    ui64 NumRows() const {
        return End - Begin;
    }

    operator size_t() const {
        return CombineHashes(Begin, End);
    }

    TString DebugString() const {
        return TStringBuilder() << "[" << Begin << ";" << End << ")";
    }
};

class TDuplicateMapInfo {
private:
    TSnapshot MaxVersion;
    TRowRange Rows;
    YDB_READONLY_DEF(ui64, SourceId);

public:
    TDuplicateMapInfo(const TSnapshot& maxVersion, const TRowRange& rows, const ui64 sourceId)
        : MaxVersion(maxVersion)
        , Rows(rows)
        , SourceId(sourceId)
    {
    }

    operator size_t() const {
        size_t h = (size_t)MaxVersion;
        h = CombineHashes(h, (size_t)Rows);
        h = CombineHashes(h, SourceId);
        return h;
    }
    bool operator==(const TDuplicateMapInfo& other) const {
        return std::tie(MaxVersion, Rows, SourceId) == std::tie(other.MaxVersion, other.Rows, other.SourceId);
    }

    TString DebugString() const {
        return TStringBuilder() << "MaxVersion=" << MaxVersion.DebugString() << ";Rows=" << Rows.DebugString() << ";SourceId=" << SourceId;
    }

    const TRowRange& GetRows() const {
        return Rows;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
