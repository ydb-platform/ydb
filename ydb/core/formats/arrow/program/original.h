#pragma once
#include "abstract.h"
#include "functions.h"
#include "kernel_logic.h"

namespace NKikimr::NArrow::NSSA {

class TOriginalColumnDataProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    const ui32 ColumnId;

    YDB_ACCESSOR_DEF(TString, ColumnName);
    YDB_ACCESSOR_DEF(TString, SubColumnName);

    virtual bool HasSubColumns() const override {
        return !!SubColumnName;
    }

    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("col", ColumnName);
        if (!!SubColumnName) {
            result.InsertValue("sub", SubColumnName);
        }
        return result;
    }

    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool IsAggregation() const override {
        return false;
    }

    virtual ui64 DoGetWeight() const override {
        return SubColumnName ? 5 : 10;
    }

public:
    TOriginalColumnDataProcessor(const ui32 outputId, const ui32 columnId, const TString& columnName, const TString& subColumnName)
        : TBase({}, { outputId }, EProcessorType::FetchOriginalData)
        , ColumnId(columnId)
        , ColumnName(columnName)
        , SubColumnName(subColumnName) {
        AFL_VERIFY(!!ColumnName);
    }
};

class TOriginalColumnAccessorProcessor: public IResourceProcessor {
private:
    using TBase = IResourceProcessor;
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY_DEF(TString, SubColumnName);
    virtual TConclusion<EExecutionResult> DoExecute(const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const override;

    virtual bool HasSubColumns() const override {
        return !!SubColumnName;
    }

    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("col", ColumnId);
        if (!!SubColumnName) {
            result.InsertValue("sub", SubColumnName);
        }
        return result;
    }

    virtual bool IsAggregation() const override {
        return false;
    }

    virtual ui64 DoGetWeight() const override {
        return SubColumnName ? 2 : 5;
    }

public:
    TOriginalColumnAccessorProcessor(const ui32 inputId, const ui32 outputId, const ui32 columnId, const TString& subColumnName)
        : TBase({ inputId }, { outputId }, EProcessorType::AssembleOriginalData)
        , ColumnId(columnId)
        , SubColumnName(subColumnName) {
    }
};

}   // namespace NKikimr::NArrow::NSSA
