#pragma once
#include "header.h"

namespace NKikimr::NOlap::NIndexes {

class TChunkIndexData {
private:
    using TFetchedData = THashMap<ui64, TString>;
    YDB_READONLY_DEF(std::shared_ptr<IIndexHeader>, Header);
    YDB_READONLY_DEF(TFetchedData, DataByCategory);
    std::optional<TString> UncategorizedData;

public:
    TChunkIndexData(const std::shared_ptr<IIndexHeader>& header)
        : Header(header) {
        AFL_VERIFY(Header);
    }

    void AddData(const std::optional<ui64> category, const TString& data) {
        if (!category) {
            AFL_VERIFY(!UncategorizedData);
            UncategorizedData = data;
        } else {
            AFL_VERIFY(DataByCategory.emplace(*category, data).second);
        }
    }

    void RemoveData(const std::optional<ui64> category) {
        if (!category) {
            AFL_VERIFY(!!UncategorizedData);
            UncategorizedData.reset();
        } else {
            AFL_VERIFY(DataByCategory.erase(*category));
        }
    }

    const TString& GetData(const std::optional<ui64> category) const {
        if (!category) {
            AFL_VERIFY(!!UncategorizedData);
            return *UncategorizedData;
        } else {
            auto it = DataByCategory.find(*category);
            AFL_VERIFY(it != DataByCategory.end());
            return it->second;
        }
    }

    TString ExtractData(const std::optional<ui64> category) {
        TString result;
        if (!category) {
            AFL_VERIFY(!!UncategorizedData);
            result = *UncategorizedData;
            UncategorizedData.reset();
        } else {
            auto it = DataByCategory.find(*category);
            AFL_VERIFY(it != DataByCategory.end());
            result = it->second;
            DataByCategory.erase(it);
        }
        return result;
    }
};

class TIndexColumnChunked {
private:
    YDB_READONLY_DEF(std::vector<TChunkIndexData>, Chunks);

public:
    ui32 GetChunksCount() const {
        return Chunks.size();
    }

    const std::shared_ptr<IIndexHeader>& GetHeader(const ui32 idx) const {
        AFL_VERIFY(idx < Chunks.size())("idx", idx)("chunks", Chunks.size());
        return Chunks[idx].GetHeader();
    }

    void AddData(const std::optional<ui64> category, const std::vector<TString>& data) {
        AFL_VERIFY(Chunks.size() == data.size());
        for (ui32 i = 0; i < data.size(); ++i) {
            Chunks[i].AddData(category, data[i]);
        }
    }

    void AddChunk(const std::shared_ptr<IIndexHeader>& header) {
        Chunks.emplace_back(TChunkIndexData(header));
    }

    bool HasCategoryData(const ui64 category) const {
        std::optional<bool> hasData;
        for (auto&& i : Chunks) {
            if (!hasData) {
                hasData = i.GetDataByCategory().contains(category);
            } else {
                AFL_VERIFY(*hasData == i.GetDataByCategory().contains(category));
            }
        }
        AFL_VERIFY(hasData);
        return *hasData;
    }
};

class TIndexesCollection {
private:
    THashMap<ui32, TIndexColumnChunked> Indexes;

public:
    TIndexesCollection() = default;

    bool HasIndex(const ui32 indexId) const {
        return !!GetIndexDataOptional(indexId);
    }

    const TIndexColumnChunked* GetIndexDataOptional(const ui32 indexId) const {
        auto it = Indexes.find(indexId);
        if (it == Indexes.end()) {
            return nullptr;
        } else {
            return &it->second;
        }
    }

    const TIndexColumnChunked& GetIndexDataVerified(const ui32 indexId) const {
        auto* result = GetIndexDataOptional(indexId);
        AFL_VERIFY(result);
        return *result;
    }

    TIndexColumnChunked* MutableIndexDataOptional(const ui32 indexId) {
        auto it = Indexes.find(indexId);
        if (it == Indexes.end()) {
            return nullptr;
        } else {
            return &it->second;
        }
    }

    TIndexColumnChunked& MutableIndexDataVerified(const ui32 indexId) {
        auto* result = MutableIndexDataOptional(indexId);
        AFL_VERIFY(result);
        return *result;
    }

    void StartChunk(const ui32 indexId, const std::shared_ptr<IIndexHeader>& header) {
        auto it = Indexes.find(indexId);
        if (it == Indexes.end()) {
            it = Indexes.emplace(indexId, TIndexColumnChunked()).first;
        }
        it->second.AddChunk(header);
    }

    void AddData(const ui32 indexId, const std::optional<ui32> category, const std::vector<TString>& data) {
        MutableIndexDataVerified(indexId).AddData(category, data);
    }

    bool HasIndexData(const TIndexDataAddress& address) const {
        auto* index = GetIndexDataOptional(address.GetIndexId());
        if (!index) {
            return false;
        }
        if (!address.GetCategory()) {
            return true;
        } else {
            return index->HasCategoryData(*address.GetCategory());
        }
    }
};

}   // namespace NKikimr::NOlap::NIndexes
