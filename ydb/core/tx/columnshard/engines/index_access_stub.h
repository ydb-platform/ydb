#pragma once

#include <memory>

#include <util/generic/string.h>
#include <util/system/types.h>

#include <ydb/core/formats/arrow/program/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/abstract.h>

namespace NKikimr::NOlap {

class TVersionedIndex;

struct TIndexData {
    TString Data;
    ui32 IndexId = 0;
    ui64 SchemaVersion = 0;

    TIndexData(const TString& data, ui32 indexId, ui64 schemaVersion)
        : Data(data)
        , IndexId(indexId)
        , SchemaVersion(schemaVersion)
    {}

    TIndexData() = default;
};

class IIndexAccessStub {
public:
    virtual ~IIndexAccessStub() = default;

    virtual double RegisterPortion(ui64 portionId, const TIndexData& indexData) = 0;

    virtual void RegisterWithoutIndex(ui64 portionId) = 0;

    // if false, value is definitely absent from portion
    virtual bool CheckValue(ui64 portionId, ui64 schemaVersion,
        const std::shared_ptr<arrow::Scalar>& value,
        const NKikimr::NArrow::NSSA::TIndexCheckOperation& operation) = 0;
};

class TDefaultIndexAccessStub : public IIndexAccessStub {
public:
    double RegisterPortion(ui64 portionId, const TIndexData& indexData) override;

    void RegisterWithoutIndex(ui64 portionId) override {
        AFL_VERIFY(PortionsWithoutIndex.insert(portionId).second);
    }

    bool CheckValue(ui64 portionId, ui64 schemaVersion,
        const std::shared_ptr<arrow::Scalar>& value,
        const NKikimr::NArrow::NSSA::TIndexCheckOperation& operation) override;

    void SetVersionedIndex(const TVersionedIndex& versionedIndex) {
        VersionedIndex = &versionedIndex;
    }

    TDefaultIndexAccessStub(ui32 portionsPerNode)
        : PortionsPerNode(portionsPerNode)
        , CurrentCounter(0)
    {
    }

private:
    const TVersionedIndex* VersionedIndex = nullptr;
    std::vector<std::shared_ptr<NIndexes::IBitsStorage>> Storages;
    const ui32 PortionsPerNode;
    ui32 CurrentCounter;
    THashMap<ui64, ui32> PortionId2Position;
    THashMap<ui64, ui32> PortionId2IndexId;
    THashSet<ui64> PortionsWithoutIndex;

};

}  // namespace NKikimr::NOlap
