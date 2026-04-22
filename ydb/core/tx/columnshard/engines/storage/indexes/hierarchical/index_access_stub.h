#pragma once

#include <memory>

#include <util/generic/string.h>
#include <util/system/types.h>

#include <ydb/core/formats/arrow/program/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/abstract.h>

namespace NKikimr::NOlap {

struct TIndexInfo;

namespace NIndexes {
class IIndexMeta;
class TSkipIndex;
class TSkipBitmapIndex;
}  // namespace NIndexes

struct TIndexData {
    TString Data;
    ui32 IndexId = 0;
    std::shared_ptr<NIndexes::IIndexMeta> IndexMeta;

    TIndexData(const TString& data, ui32 indexId, const std::shared_ptr<NIndexes::IIndexMeta>& indexMeta)
        : Data(data)
        , IndexId(indexId)
        , IndexMeta(indexMeta)
    {}

    TIndexData() = default;
};

class IIndexAccessStub {
public:
    virtual ~IIndexAccessStub() = default;

    virtual double RegisterPortion(ui64 portionId, const TIndexData& indexData) = 0;

    virtual void RegisterWithoutIndex(ui64 portionId) = 0;

    // if false, value is definitely absent from portion
    virtual bool CheckValue(ui64 portionId, const NIndexes::TSkipIndex& indexMeta, const TIndexInfo& indexInfo,
        const std::shared_ptr<arrow::Scalar>& value,
        const NKikimr::NArrow::NSSA::TIndexCheckOperation& operation) = 0;
};

class TDefaultIndexAccessStub : public IIndexAccessStub {
public:
    double RegisterPortion(ui64 portionId, const TIndexData& indexData) override;

    void RegisterWithoutIndex(ui64 portionId) override {
        AFL_VERIFY(PortionsWithoutIndex.insert(portionId).second);
    }

    bool CheckValue(ui64 portionId, const NIndexes::TSkipIndex& indexMeta, const TIndexInfo& indexInfo,
        const std::shared_ptr<arrow::Scalar>& value,
        const NKikimr::NArrow::NSSA::TIndexCheckOperation& operation) override;

    TDefaultIndexAccessStub(ui32 portionsPerNode)
        : PortionsPerNode(portionsPerNode)
        , CurrentCounter(0)
    {
    }

private:
    std::vector<std::shared_ptr<NIndexes::IBitsStorage>> Storages;
    const ui32 PortionsPerNode;
    ui32 CurrentCounter;
    THashMap<ui64, ui32> PortionId2Position;
    THashSet<ui64> PortionsWithoutIndex;

};

}  // namespace NKikimr::NOlap
