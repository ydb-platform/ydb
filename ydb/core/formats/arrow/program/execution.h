#pragma once
#include "collection.h"
#include "graph_execute.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>

namespace NKikimr::NArrow::NSSA {

class TProcessorContext;
class IDataSource {
public:
    class TFetchIndexContext {
    public:
        enum EOperation {
            Equals,
            StartsWith,
            EndsWith,
            Contains
        };

    private:
        YDB_READONLY(ui32, ColumnId, 0);
        YDB_READONLY_DEF(TString, SubColumnName);
        YDB_READONLY(EOperation, Operation, EOperation::Equals);

    public:
        TFetchIndexContext(const ui32 columnId, const TString& subColumnName, const EOperation operation)
            : ColumnId(columnId)
            , SubColumnName(subColumnName)
            , Operation(operation) {
        }
    };

    class TCheckIndexContext: public TFetchIndexContext {
    private:
        using TBase = TFetchIndexContext;
        YDB_READONLY_DEF(std::shared_ptr<arrow::Scalar>, Value);

    public:
        TCheckIndexContext(
            const ui32 columnId, const TString& subColumnName, const std::shared_ptr<arrow::Scalar>& value, const EOperation operation)
            : TBase(columnId, subColumnName, operation)
            , Value(value) {
            AFL_VERIFY(Value);
        }
    };

private:
    virtual TConclusion<bool> DoStartFetchData(const TProcessorContext& context, const ui32 columnId, const TString& subColumnName) = 0;
    virtual void DoAssembleAccessor(const TProcessorContext& context, const ui32 columnId, const TString& subColumnName) = 0;
    virtual TConclusion<bool> DoStartFetchIndex(const TProcessorContext& context, const TFetchIndexContext& fetchContext) = 0;
    virtual TConclusion<bool> DoCheckIndex(
        const TProcessorContext& context, const ui32 outputId, const TCheckIndexContext& checkContext) = 0;

public:
    virtual ~IDataSource() = default;

    TConclusion<bool> CheckIndex(const TProcessorContext& context, const ui32 outputId, const TCheckIndexContext& checkContext) {
        return DoCheckIndex(context, outputId, checkContext);
    }

    void AssembleAccessor(const TProcessorContext& context, const ui32 columnId, const TString& subColumnName) {
        DoAssembleAccessor(context, columnId, subColumnName);
    }

    [[nodiscard]] TConclusion<bool> StartFetchData(const TProcessorContext& context, const ui32 columnId, const TString& subColumnName) {
        return DoStartFetchData(context, columnId, subColumnName);
    }

    [[nodiscard]] TConclusion<bool> StartFetchIndex(const TProcessorContext& context, const TFetchIndexContext& fetchContext) {
        return DoStartFetchIndex(context, fetchContext);
    }
};

class TProcessorContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<NAccessor::TAccessorsCollection>, Resources);
    YDB_READONLY_DEF(std::shared_ptr<IDataSource>, DataSource);
    YDB_READONLY_DEF(std::optional<ui32>, Limit);
    YDB_READONLY(bool, Reverse, false);

public:
    TProcessorContext(const std::shared_ptr<IDataSource>& dataSource, const std::shared_ptr<NAccessor::TAccessorsCollection>& resources,
        const std::optional<ui32> limit, const bool reverse)
        : Resources(resources)
        , DataSource(dataSource)
        , Limit(limit)
        , Reverse(reverse) {
    }
};

class TFailDataSource: public IDataSource {
private:
    virtual TConclusion<bool> DoStartFetchData(
        const TProcessorContext& /*context*/, const ui32 /*columnId*/, const TString& /*subColumnName*/) override {
        AFL_VERIFY(false);
        return true;
    }
    virtual void DoAssembleAccessor(const TProcessorContext& /*context*/, const ui32 /*columnId*/, const TString& /*subColumnName*/) override {
        AFL_VERIFY(false);
    }
};

class TSimpleDataSource: public IDataSource {
private:
    class TBlobAddress {
    private:
        YDB_READONLY(ui32, ColumnId, 0);
        YDB_READONLY_DEF(TString, SubColumnName);

    public:
        explicit TBlobAddress(const ui32 columnId)
            : ColumnId(columnId) {
        }
        explicit TBlobAddress(const ui32 columnId, const TString& subColumnName)
            : ColumnId(columnId)
            , SubColumnName(subColumnName) {
        }

        bool operator==(const TBlobAddress& addr) const {
            return std::tie(ColumnId, SubColumnName) == std::tie(addr.ColumnId, addr.SubColumnName);
        }

        explicit operator size_t() const {
            return ColumnId ^ (SubColumnName.size() << 10);
        }
    };

    mutable THashMap<TBlobAddress, TString> Blobs;
    mutable THashMap<ui32, NAccessor::TChunkConstructionData> Info;
    std::shared_ptr<NAccessor::TAccessorsCollection> Resources;

    virtual TConclusion<bool> DoStartFetchData(
        const TProcessorContext& /*context*/, const ui32 columnId, const TString& subColumnName) override {
        AFL_VERIFY(Blobs.contains(TBlobAddress(columnId, subColumnName)));
        return false;
    }
    virtual void DoAssembleAccessor(const TProcessorContext& context, const ui32 columnId, const TString& subColumnName) override;

public:
    const std::shared_ptr<NAccessor::TAccessorsCollection>& GetResources() const {
        return Resources;
    }

    TSimpleDataSource() {
        Resources = std::make_shared<NAccessor::TAccessorsCollection>();
    }

    void AddBlob(const ui32 columnId, const TString& subColumnName, const std::shared_ptr<arrow::Array>& data);
};

}   // namespace NKikimr::NArrow::NSSA
