#pragma once

#include <ydb/core/formats/arrow/common/container.h>
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

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
