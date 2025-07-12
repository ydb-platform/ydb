#pragma once

#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TColumnsData {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, Data);
    YDB_READONLY_DEF(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>, MemoryGuard);

public:
    TColumnsData(const std::shared_ptr<NArrow::TGeneralContainer>& data, const std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>& memory)
        : Data(data)
        , MemoryGuard(memory) {
        AFL_VERIFY(MemoryGuard);
    }

    ui64 GetRawSize() const {
        return MemoryGuard->GetMemory();
    }
};

class TDuplicateMapInfo {
private:
    TSnapshot MaxVersion;
    YDB_READONLY_DEF(ui64, Offset);
    YDB_READONLY_DEF(ui64, RowsCount);
    YDB_READONLY_DEF(ui64, SourceId);

public:
    TDuplicateMapInfo(const TSnapshot& maxVersion, const ui64 offset, const ui64 rowsCount, const ui64 sourceId)
        : MaxVersion(maxVersion)
        , Offset(offset)
        , RowsCount(rowsCount)
        , SourceId(sourceId) {
    }

    operator size_t() const {
        ui64 h = 0;
        h = CombineHashes(h, (size_t)MaxVersion);
        h = CombineHashes(h, Offset);
        h = CombineHashes(h, RowsCount);
        h = CombineHashes(h, SourceId);
        return h;
    }
    bool operator==(const TDuplicateMapInfo& other) const {
        return std::tie(MaxVersion, Offset, RowsCount, SourceId) == std::tie(other.MaxVersion, other.Offset, other.RowsCount, other.SourceId);
    }

    TString DebugString() const {
        return TStringBuilder() << "MaxVersion=" << MaxVersion.DebugString() << ";Offset=" << Offset << ";RowsCount=" << RowsCount
                                << ";SourceId=" << SourceId;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
