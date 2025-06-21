#pragma once
#include "abstract.h"
#include "functions.h"
#include "kernel_logic.h"
#include "execution.h"

namespace NKikimr::NArrow::NSSA {

class TOriginalColumnDataProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;

    THashMap<ui32, IDataSource::TDataAddress> DataAddresses;
    THashMap<ui32, IDataSource::TFetchIndexContext> IndexContext;
    THashMap<ui32, IDataSource::TFetchHeaderContext> HeaderContext;

    virtual bool HasSubColumns() const override {
        for (auto&& i : IndexContext) {
            if (i.second.GetOperationsBySubColumn().HasSubColumns()) {
                return true;
            }
        }
        for (auto&& i : HeaderContext) {
            if (i.second.GetSubColumnNames().size()) {
                return true;
            }
        }
        for (auto&& i : DataAddresses) {
            if (i.second.GetSubColumnNames(false).size()) {
                return true;
            }
        }
        return false;
    }

    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        if (DataAddresses.size()) {
            auto& arrAddr = result.InsertValue("data", NJson::JSON_ARRAY);
            for (auto&& i : DataAddresses) {
                arrAddr.AppendValue(i.second.DebugJson());
            }
        }
        if (IndexContext.size()) {
            auto& indexesArr = result.InsertValue("indexes", NJson::JSON_ARRAY);
            for (auto&& i : IndexContext) {
                indexesArr.AppendValue(i.second.DebugJson());
            }
        }
        if (HeaderContext.size()) {
            auto& headersArr = result.InsertValue("headers", NJson::JSON_ARRAY);
            for (auto&& i : HeaderContext) {
                headersArr.AppendValue(i.second.DebugJson());
            }
        }
        return result;
    }

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

    virtual ui64 DoGetWeight() const override {
        ui64 w = 0;
        for (auto&& i : DataAddresses) {
            w += i.second.GetWeight();
        }
        for (auto&& i : IndexContext) {
            w += i.second.GetWeight();
        }
        for (auto&& i : HeaderContext) {
            w += i.second.GetWeight();
        }
        return w;
    }

public:
    void Add(const IDataSource::TDataAddress& dataAddress) {
        auto it = DataAddresses.find(dataAddress.GetColumnId());
        if (it == DataAddresses.end()) {
            DataAddresses.emplace(dataAddress.GetColumnId(), dataAddress);
        } else {
            it->second.MergeFrom(dataAddress);
        }
    }

    const THashMap<ui32, IDataSource::TDataAddress>& GetDataAddresses() const {
        return DataAddresses;
    }
    const THashMap<ui32, IDataSource::TFetchIndexContext>& GetIndexContext() const {
        return IndexContext;
    }
    const THashMap<ui32, IDataSource::TFetchHeaderContext>& GetHeaderContext() const {
        return HeaderContext;
    }

    void Add(const IDataSource::TFetchIndexContext& indexContext) {
        auto it = IndexContext.find(indexContext.GetColumnId());
        if (it == IndexContext.end()) {
            IndexContext.emplace(indexContext.GetColumnId(), indexContext);
        } else {
            it->second.MergeFrom(indexContext);
        }
    }

    void Add(const IDataSource::TFetchHeaderContext& indexContext) {
        auto it = HeaderContext.find(indexContext.GetColumnId());
        if (it == HeaderContext.end()) {
            HeaderContext.emplace(indexContext.GetColumnId(), indexContext);
        } else {
            it->second.MergeFrom(indexContext);
        }
    }

    explicit TOriginalColumnDataProcessor(const std::vector<ui32>& outputIds)
        : TBase({}, TColumnChainInfo::BuildVector(outputIds), EProcessorType::FetchOriginalData) {
    }

    TOriginalColumnDataProcessor(const ui32 outputId, const IDataSource::TDataAddress& address)
        : TBase({}, TColumnChainInfo::BuildVector({ outputId }), EProcessorType::FetchOriginalData) {
        DataAddresses.emplace(address.GetColumnId(), address);
    }

    TOriginalColumnDataProcessor(const ui32 outputId, const IDataSource::TFetchIndexContext& indexContext)
        : TBase({}, TColumnChainInfo::BuildVector({ outputId }), EProcessorType::FetchOriginalData) {
        IndexContext.emplace(indexContext.GetColumnId(), indexContext);
    }

    TOriginalColumnDataProcessor(const ui32 outputId, const IDataSource::TFetchHeaderContext& indexContext)
        : TBase({}, TColumnChainInfo::BuildVector({ outputId }), EProcessorType::FetchOriginalData) {
        HeaderContext.emplace(indexContext.GetColumnId(), indexContext);
    }
};

class TOriginalColumnAccessorProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    IDataSource::TDataAddress DataAddress;
    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool HasSubColumns() const override {
        return DataAddress.GetSubColumnNames(false).size();
    }

    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("address", DataAddress.DebugJson());
        return result;
    }

    virtual bool IsAggregation() const override {
        return false;
    }

    virtual ui64 DoGetWeight() const override {
        return DataAddress.GetSubColumnNames(false).size() ? 2 : 5;
    }

public:
    const IDataSource::TDataAddress& GetDataAddress() const {
        return DataAddress;
    }

    TOriginalColumnAccessorProcessor(const ui32 inputId, const ui32 outputId, const IDataSource::TDataAddress& address)
        : TBase(TColumnChainInfo::BuildVector({ inputId }), TColumnChainInfo::BuildVector({ outputId }), EProcessorType::AssembleOriginalData)
        , DataAddress(address) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
