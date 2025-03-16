#pragma once
#include "collection.h"
#include "graph_execute.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>

#include <util/digest/fnv.h>
#include <util/digest/numeric.h>

namespace NKikimr::NArrow::NSSA {

enum class EIndexCheckOperation {
    Equals,
    StartsWith,
    EndsWith,
    Contains
};

class TProcessorContext;

class IFetchLogic {
private:
    YDB_READONLY(ui32, EntityId, 0);

public:
    IFetchLogic(const ui32 id)
        : EntityId(id) {
    }
};

class IDataSource {
public:
    class TDataAddress {
    private:
        YDB_READONLY(ui32, ColumnId, 0);
        YDB_READONLY_DEF(TString, ColumnName);
        std::optional<bool> IsFullColumn;
        THashSet<TString> SubColumnNames;

    public:
        TDataAddress SelectSubColumns(const THashSet<TString>& selected) const {
            AFL_VERIFY(selected.size());
            TDataAddress result(ColumnId, ColumnName, std::nullopt);
            for (auto&& i : selected) {
                if (!i) {
                    AFL_VERIFY(selected.size() == 1);
                    continue;
                }
                AFL_VERIFY(SubColumnNames.contains(i));
                result.AddSubColumnName(i);
            }
            return result;
        }

        bool HasSubColumns() const {
            return SubColumnNames.size();
        }

        const THashSet<TString>& GetSubColumnNames(const bool withEmptyKey) const {
            if (SubColumnNames.size() || !withEmptyKey) {
                return SubColumnNames;
            } else {
                static THashSet<TString> subColumns = { "" };
                return subColumns;
            }
        }

        explicit TDataAddress(const ui32 columnId, const TString& columnName, const std::optional<TString>& subColumnName)
            : ColumnId(columnId)
            , ColumnName(columnName) {
            AFL_VERIFY(!!ColumnName);
            if (subColumnName) {
                AddSubColumnName(*subColumnName);
            }
        }

        void MergeFrom(const TDataAddress& addr) {
            AFL_VERIFY(ColumnId == addr.ColumnId);
            AFL_VERIFY(ColumnName == addr.ColumnName);
            if (IsFullColumn) {
                AFL_VERIFY(!!addr.IsFullColumn);
                if (IsFullColumn == addr.IsFullColumn && !*IsFullColumn) {
                    SubColumnNames.insert(addr.SubColumnNames.begin(), addr.SubColumnNames.end());
                } else {
                    IsFullColumn = true;
                    SubColumnNames.clear();
                }
            } else {
                IsFullColumn = addr.IsFullColumn;
                SubColumnNames = addr.SubColumnNames;
            }
        }

        void AddSubColumnName(const TString& scName) {
            if (!IsFullColumn) {
                IsFullColumn = !scName;
            } else {
                AFL_VERIFY(*IsFullColumn == !scName);
            }
            if (!!scName) {
                AFL_VERIFY(SubColumnNames.emplace(scName).second);
            }
        }

        NJson::TJsonValue DebugJson() const {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("id", ColumnId);
            result.InsertValue("name", ColumnName);
            if (SubColumnNames.size()) {
                result.InsertValue("sub", JoinSeq(",", SubColumnNames));
            }
            return result;
        }

        ui64 GetWeight() const {
            return SubColumnNames.size() ? 1 : 2;
        }
    };

    class TFetchHeaderContext {
    private:
        YDB_READONLY(ui32, ColumnId, 0);
        YDB_READONLY_DEF(THashSet<TString>, SubColumnNames);

    public:
        NJson::TJsonValue DebugJson() const {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("cid", ColumnId);
            if (SubColumnNames.size()) {
                result.InsertValue("sc", JoinSeq(",", SubColumnNames));
            }
            return result;
        }

        ui64 GetWeight() const {
            return 1;
        }

        TFetchHeaderContext(const ui32 columnId, const THashSet<TString>& subColumnNames)
            : ColumnId(columnId)
            , SubColumnNames(subColumnNames) {
        }

        void AddSubColumn(const TString& subColumnName) {
            AFL_VERIFY(SubColumnNames.emplace(subColumnName).second);
        }
        void MergeFrom(const TFetchHeaderContext& ctx) {
            AFL_VERIFY(ColumnId == ctx.GetColumnId());
            SubColumnNames.insert(ctx.SubColumnNames.begin(), ctx.SubColumnNames.end());
        }
    };

    class TFetchIndexContext {
    public:
        using EOperation = EIndexCheckOperation;

        class TOperationsBySubColumn {
        private:
            std::optional<bool> FullColumnOperations;
            THashMap<TString, THashSet<EOperation>> Data;

        public:
            const THashMap<TString, THashSet<EOperation>>& GetData() const {
                return Data;
            }

            bool HasSubColumns() const {
                AFL_VERIFY(FullColumnOperations);
                return !*FullColumnOperations;
            }

            TOperationsBySubColumn& Add(const TString& subColumn, const EOperation operation, const bool strict = true) {
                if (FullColumnOperations) {
                    AFL_VERIFY(*FullColumnOperations == !subColumn);
                } else {
                    FullColumnOperations = !subColumn;
                }
                if (strict) {
                    AFL_VERIFY(Data[subColumn].emplace(operation).second);
                } else {
                    Data[subColumn].emplace(operation);
                }
                return *this;
            }
        };

    private:
        YDB_READONLY(ui32, ColumnId, 0);
        YDB_READONLY_DEF(TOperationsBySubColumn, OperationsBySubColumn);

    public:
        NJson::TJsonValue DebugJson() const {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("cid", ColumnId);
            for (auto&& i : OperationsBySubColumn.GetData()) {
                auto& subColumnJson = result.InsertValue(i.first, NJson::JSON_ARRAY);
                for (auto&& op : i.second) {
                    subColumnJson.AppendValue(::ToString(op));
                }
            }
            return result;
        }

        ui64 GetWeight() const {
            ui64 w = 0;
            for (auto&& i : OperationsBySubColumn.GetData()) {
                w += i.second.size() * (i.first ? 1 : 2);
            }
            return w;
        }

        void MergeFrom(const TFetchIndexContext& ctx) {
            AFL_VERIFY(ColumnId == ctx.GetColumnId());
            AFL_VERIFY(ctx.OperationsBySubColumn.GetData().size());
            for (auto&& [subColumnName, operators] : ctx.OperationsBySubColumn.GetData()) {
                for (auto&& op : operators) {
                    OperationsBySubColumn.Add(subColumnName, op, false);
                }
            }
        }

        TFetchIndexContext(const ui32 columnId, const TOperationsBySubColumn& operationsBySubColumn)
            : ColumnId(columnId)
            , OperationsBySubColumn(operationsBySubColumn) {
            AFL_VERIFY(OperationsBySubColumn.GetData().size());
        }
    };

    class TCheckIndexContext {
    private:
        YDB_READONLY(ui32, ColumnId, 0);
        YDB_READONLY_DEF(TString, SubColumnName);
        YDB_READONLY(EIndexCheckOperation, Operation, EIndexCheckOperation::Equals);

    public:
        TCheckIndexContext(const ui32 columnId, const TString& subColumnName, const EIndexCheckOperation operation)
            : ColumnId(columnId)
            , SubColumnName(subColumnName)
            , Operation(operation) {
        }

        bool operator==(const TCheckIndexContext& item) const {
            return std::tie(ColumnId, SubColumnName, Operation) == std::tie(item.ColumnId, item.SubColumnName, item.Operation);
        }

        operator size_t() const {
            return CombineHashes<ui64>(
                (ui64)Operation, CombineHashes<ui64>(ColumnId, FnvHash<ui64>(SubColumnName.data(), SubColumnName.size())));
        }
    };

    class TCheckHeaderContext {
    private:
        YDB_READONLY(ui32, ColumnId, 0);
        YDB_READONLY_DEF(TString, SubColumnName);

    public:
        TCheckHeaderContext(const ui32 columnId, const TString& subColumnName)
            : ColumnId(columnId)
            , SubColumnName(subColumnName) {
        }
    };

private:
    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchData(
        const TProcessorContext& context, const TDataAddress& addr) = 0;
    virtual void DoAssembleAccessor(const TProcessorContext& context, const ui32 columnId, const TString& subColumnName) = 0;

    virtual TConclusion<std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>> DoStartFetchIndex(
        const TProcessorContext& context, const TFetchIndexContext& fetchContext) = 0;
    virtual TConclusion<NArrow::TColumnFilter> DoCheckIndex(
        const TProcessorContext& context, const TCheckIndexContext& fetchContext, const std::shared_ptr<arrow::Scalar>& value) = 0;

    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchHeader(
        const TProcessorContext& context, const TFetchHeaderContext& fetchContext) = 0;
    virtual TConclusion<NArrow::TColumnFilter> DoCheckHeader(const TProcessorContext& context, const TCheckHeaderContext& fetchContext) = 0;

    virtual TConclusion<bool> DoStartFetch(
        const NArrow::NSSA::TProcessorContext& context, const std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>& fetchers) = 0;

public:
    virtual ~IDataSource() = default;

    TConclusion<bool> StartFetch(
        const NArrow::NSSA::TProcessorContext& context, const std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>& fetchers) {
        return DoStartFetch(context, fetchers);
    }

    [[nodiscard]] TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> StartFetchHeader(
        const TProcessorContext& context, const TFetchHeaderContext& fetchContext) {
        return DoStartFetchHeader(context, fetchContext);
    }

    TConclusion<NArrow::TColumnFilter> CheckHeader(const TProcessorContext& context, const TCheckHeaderContext& fetchContext) {
        return DoCheckHeader(context, fetchContext);
    }

    [[nodiscard]] TConclusion<std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>> StartFetchIndex(
        const TProcessorContext& context, const TFetchIndexContext& fetchContext) {
        return DoStartFetchIndex(context, fetchContext);
    }

    TConclusion<NArrow::TColumnFilter> CheckIndex(
        const TProcessorContext& context, const TCheckIndexContext& fetchContext, const std::shared_ptr<arrow::Scalar>& value) {
        return DoCheckIndex(context, fetchContext, value);
    }

    [[nodiscard]] TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> StartFetchData(
        const TProcessorContext& context, const TDataAddress& addr) {
        return DoStartFetchData(context, addr);
    }

    void AssembleAccessor(const TProcessorContext& context, const ui32 columnId, const TString& subColumnName) {
        DoAssembleAccessor(context, columnId, subColumnName);
    }
};

class TProcessorContext {
private:
    YDB_READONLY_DEF(std::shared_ptr<NAccessor::TAccessorsCollection>, Resources);
    YDB_READONLY_DEF(std::weak_ptr<IDataSource>, DataSource);
    YDB_READONLY_DEF(std::optional<ui32>, Limit);
    YDB_READONLY(bool, Reverse, false);

public:
    template <class T>
    std::shared_ptr<T> GetDataSourceVerifiedAs() const {
        auto result = std::static_pointer_cast<T>(DataSource.lock());
        AFL_VERIFY(result);
        return result;
    }

    TProcessorContext(const std::weak_ptr<IDataSource>& dataSource, const std::shared_ptr<NAccessor::TAccessorsCollection>& resources,
        const std::optional<ui32> limit, const bool reverse)
        : Resources(resources)
        , DataSource(dataSource)
        , Limit(limit)
        , Reverse(reverse) {
    }
};

class TFailDataSource: public IDataSource {
private:
    virtual TConclusion<bool> DoStartFetch(const NArrow::NSSA::TProcessorContext& /*context*/,
        const std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>& /*fetchers*/) override {
        AFL_VERIFY(false);
        return false;
    }
    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchHeader(
        const TProcessorContext& /*context*/, const TFetchHeaderContext& /*fetchContext*/) override {
        AFL_VERIFY(false);
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }
    virtual TConclusion<NArrow::TColumnFilter> DoCheckHeader(
        const TProcessorContext& /*context*/, const TCheckHeaderContext& /*fetchContext*/) override {
        AFL_VERIFY(false);
        return TColumnFilter::BuildAllowFilter();
    }
    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchData(
        const TProcessorContext& /*context*/, const TDataAddress& /*addr*/) override {
        AFL_VERIFY(false);
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }
    virtual void DoAssembleAccessor(const TProcessorContext& /*context*/, const ui32 /*columnId*/, const TString& /*subColumnName*/) override {
        AFL_VERIFY(false);
    }
    virtual TConclusion<std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>> DoStartFetchIndex(
        const TProcessorContext& /*context*/, const TFetchIndexContext& /*fetchContext*/) override {
        AFL_VERIFY(false);
        return std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>();
    }
    virtual TConclusion<NArrow::TColumnFilter> DoCheckIndex(const TProcessorContext& /*context*/, const TCheckIndexContext& /*fetchContext*/,
        const std::shared_ptr<arrow::Scalar>& /*value*/) override {
        AFL_VERIFY(false);
        return NArrow::TColumnFilter::BuildAllowFilter();
    }
};

class TFakeDataSource: public IDataSource {
private:
    virtual TConclusion<bool> DoStartFetch(const NArrow::NSSA::TProcessorContext& /*context*/,
        const std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>& /*fetchers*/) override {
        return false;
    }
    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchHeader(
        const TProcessorContext& /*context*/, const TFetchHeaderContext& /*fetchContext*/) override {
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }
    virtual TConclusion<NArrow::TColumnFilter> DoCheckHeader(
        const TProcessorContext& /*context*/, const TCheckHeaderContext& /*fetchContext*/) override {
        return TColumnFilter::BuildAllowFilter();
    }
    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchData(
        const TProcessorContext& /*context*/, const TDataAddress& /*addr*/) override {
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }
    virtual void DoAssembleAccessor(const TProcessorContext& /*context*/, const ui32 /*columnId*/, const TString& /*subColumnName*/) override {
    }
    virtual TConclusion<std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>> DoStartFetchIndex(
        const TProcessorContext& /*context*/, const TFetchIndexContext& /*fetchContext*/) override {
        return std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>();
    }
    virtual TConclusion<NArrow::TColumnFilter> DoCheckIndex(const TProcessorContext& /*context*/, const TCheckIndexContext& /*fetchContext*/,
        const std::shared_ptr<arrow::Scalar>& /*value*/) override {
        return NArrow::TColumnFilter::BuildAllowFilter();
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

    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchData(
        const TProcessorContext& /*context*/, const TDataAddress& addr) override {
        for (auto&& i : addr.GetSubColumnNames(true)) {
            AFL_VERIFY(Blobs.contains(TBlobAddress(addr.GetColumnId(), i)));
        }
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }
    virtual void DoAssembleAccessor(const TProcessorContext& context, const ui32 columnId, const TString& subColumnName) override;

    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchHeader(
        const TProcessorContext& /*context*/, const TFetchHeaderContext& /*fetchContext*/) override {
        AFL_VERIFY(false);
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }
    virtual TConclusion<NArrow::TColumnFilter> DoCheckHeader(
        const TProcessorContext& /*context*/, const TCheckHeaderContext& /*fetchContext*/) override {
        AFL_VERIFY(false);
        return TColumnFilter::BuildAllowFilter();
    }
    virtual TConclusion<std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>> DoStartFetchIndex(
        const TProcessorContext& /*context*/, const TFetchIndexContext& /*fetchContext*/) override {
        AFL_VERIFY(false);
        return std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>();
    }
    virtual TConclusion<NArrow::TColumnFilter> DoCheckIndex(const TProcessorContext& /*context*/, const TCheckIndexContext& /*fetchContext*/,
        const std::shared_ptr<arrow::Scalar>& /*value*/) override {
        AFL_VERIFY(false);
        return NArrow::TColumnFilter::BuildAllowFilter();
    }
    virtual TConclusion<bool> DoStartFetch(const NArrow::NSSA::TProcessorContext& /*context*/,
        const std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>& /*fetchers*/) override {
        return false;
    }

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
