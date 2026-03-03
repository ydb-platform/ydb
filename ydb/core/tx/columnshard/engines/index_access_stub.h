#pragma once

#include <memory>

#include <util/generic/string.h>
#include <util/system/types.h>

#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom_ngramm/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/string.h>

namespace NKikimr::NOlap {

struct TIndexData {
    TString Data;
    ui32 NGrammSize;

    TIndexData(const TString& data, ui32 nGrammSize)
        : Data(data)
        , NGrammSize(nGrammSize)
    {}

    TIndexData() = default;
};

class IIndexAccessStub {
public:
    virtual ~IIndexAccessStub() = default;

    virtual double RegisterPortion(ui64 portionId, const TIndexData& indexData) = 0;

    virtual void RegisterWithoutIndex(ui64 portionId) = 0;

    // if false, value is definitely absent from portion
    virtual bool CheckValue(ui64 portionId, const TString& value) = 0;
};

class TDefaultIndexAccessStub : public IIndexAccessStub {
public:
    double RegisterPortion(ui64 portionId, const TIndexData& indexData) override;

    void RegisterWithoutIndex(ui64 portionId) override {
        AFL_VERIFY(PortionsWithoutIndex.insert(portionId).second);
    }

    bool CheckValue(ui64 portionId, const TString& value) override;

    TDefaultIndexAccessStub(ui32 portionsPerNode)
        : Constructor(std::make_shared<NIndexes::TFixStringBitsStorageConstructor>())
        , PortionsPerNode(portionsPerNode)
        , CurrentCounter(0)
    {
        Index3 = std::make_unique<NIndexes::NBloomNGramm::TIndexMeta>(0, "", "", false, 0, NIndexes::TReadDataExtractorContainer(),
            2, 131072, 3, 100500, Constructor, false);
        Index4 = std::make_unique<NIndexes::NBloomNGramm::TIndexMeta>(0, "", "", false, 0, NIndexes::TReadDataExtractorContainer(),
            2, 131072, 4, 100500, Constructor, false);
        Index5 = std::make_unique<NIndexes::NBloomNGramm::TIndexMeta>(0, "", "", false, 0, NIndexes::TReadDataExtractorContainer(),
            2, 131072, 5, 100500, Constructor, false);
    }

private:
    // does not store actual data, only performs operations
    std::unique_ptr<NIndexes::NBloomNGramm::TIndexMeta> Index3;
    std::unique_ptr<NIndexes::NBloomNGramm::TIndexMeta> Index4;
    std::unique_ptr<NIndexes::NBloomNGramm::TIndexMeta> Index5;
    std::shared_ptr<NIndexes::TFixStringBitsStorageConstructor> Constructor;
    std::vector<std::shared_ptr<NIndexes::IBitsStorage>> Storages;
    const ui32 PortionsPerNode;
    ui32 CurrentCounter;
    THashMap<ui64, ui32> PortionId2Position;
    THashMap<ui64, ui32> PortionId2NGrammSize;
    THashSet<ui64> PortionsWithoutIndex;

};

}  // namespace NKikimr::NOlap
