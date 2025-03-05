#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/conclusion/status.h>

#include <util/generic/string.h>

namespace NKikimr::NArrow::NAccessor {
class TAccessorsCollection;
}

namespace NKikimr::NArrow::NSSA {

using IChunkedArray = NAccessor::IChunkedArray;
using TAccessorsCollection = NAccessor::TAccessorsCollection;

class TColumnInfo {
private:
    bool GeneratedFlag = false;
    YDB_READONLY_DEF(std::string, ColumnName);
    YDB_READONLY(ui32, ColumnId, 0);
    explicit TColumnInfo(const ui32 columnId, const std::string& columnName, const bool generated)
        : GeneratedFlag(generated)
        , ColumnName(columnName)
        , ColumnId(columnId) {
    }

public:
    TString DebugString() const {
        return TStringBuilder() << (GeneratedFlag ? "G:" : "") << ColumnName;
    }

    static TColumnInfo Generated(const ui32 columnId, const std::string& columnName) {
        return TColumnInfo(columnId, columnName, true);
    }

    static TColumnInfo Original(const ui32 columnId, const std::string& columnName) {
        return TColumnInfo(columnId, columnName, false);
    }

    bool IsGenerated() const {
        return GeneratedFlag;
    }
};

class IColumnResolver {
public:
    virtual ~IColumnResolver() = default;
    virtual TString GetColumnName(ui32 id, bool required = true) const = 0;
    virtual std::optional<ui32> GetColumnIdOptional(const TString& name) const = 0;
    bool HasColumn(const ui32 id) const {
        return !!GetColumnName(id, false);
    }

    ui32 GetColumnIdVerified(const char* name) const {
        auto result = GetColumnIdOptional(name);
        AFL_VERIFY(!!result);
        return *result;
    }

    ui32 GetColumnIdVerified(const TString& name) const {
        auto result = GetColumnIdOptional(name);
        AFL_VERIFY(!!result);
        return *result;
    }

    ui32 GetColumnIdVerified(const std::string& name) const {
        auto result = GetColumnIdOptional(TString(name.data(), name.size()));
        AFL_VERIFY(!!result);
        return *result;
    }

    std::set<ui32> GetColumnIdsSetVerified(const std::set<TString>& columnNames) const {
        std::set<ui32> result;
        for (auto&& i : columnNames) {
            AFL_VERIFY(result.emplace(GetColumnIdVerified(i)).second);
        }
        return result;
    }
    virtual TColumnInfo GetDefaultColumn() const = 0;
};

class TSchemaColumnResolver: public IColumnResolver {
private:
    std::shared_ptr<arrow::Schema> Schema;

public:
    virtual TString GetColumnName(ui32 id, bool required = true) const override {
        AFL_VERIFY(id);
        if (id < (ui32)Schema->num_fields() + 1) {
            const std::string& name = Schema->field(id - 1)->name();
            return TString(name.data(), name.size());
        } else {
            AFL_VERIFY(!required);
            return "";
        }
    }
    virtual std::optional<ui32> GetColumnIdOptional(const TString& name) const override {
        const int index = Schema->GetFieldIndex(name);
        if (index == -1) {
            return std::nullopt;
        } else {
            return index + 1;
        }
    }
    virtual TColumnInfo GetDefaultColumn() const override {
        AFL_VERIFY(false);
        return TColumnInfo::Generated(0, "");
    }
    TSchemaColumnResolver(const std::shared_ptr<arrow::Schema>& schema)
        : Schema(schema) {
    }
};

class TColumnChainInfo {
private:
    YDB_READONLY(ui32, ColumnId, 0);

public:
    TString DebugString() const {
        return ::ToString(ColumnId);
    }

    template <class TContainer>
    static std::vector<ui32> ExtractColumnIds(const TContainer& container) {
        std::vector<ui32> result;
        for (auto&& i : container) {
            result.emplace_back(i.GetColumnId());
        }
        return result;
    }

    template <class TContainer>
    static std::vector<TColumnChainInfo> BuildVector(const TContainer& container) {
        std::vector<TColumnChainInfo> result;
        for (auto&& i : container) {
            result.emplace_back(i);
        }
        return result;
    }

    static std::vector<TColumnChainInfo> BuildVector(const std::initializer_list<ui32> container) {
        std::vector<TColumnChainInfo> result;
        for (auto&& i : container) {
            result.emplace_back(i);
        }
        return result;
    }

    TColumnChainInfo(const ui32 columnId)
        : ColumnId(columnId) {
    }

    bool operator==(const TColumnChainInfo& item) const {
        return ColumnId == item.ColumnId;
    }
};

enum class EProcessorType {
    Unknown = 0,
    Const,
    Calculation,
    Projection,
    Filter,
    Aggregation,
    Original
};

class TFetchingInfo {
private:
    YDB_READONLY(bool, RemoveCurrent, false);
    YDB_READONLY(bool, FullRestore, true);
    YDB_READONLY_DEF(std::vector<TString>, SubColumns);

public:
    static TFetchingInfo BuildFullRestore(const bool replace) {
        TFetchingInfo result;
        result.RemoveCurrent = replace;
        return result;
    }
    static TFetchingInfo BuildSubColumnsRestore(const std::vector<TString>& subColumns) {
        TFetchingInfo result;
        result.FullRestore = false;
        result.SubColumns = subColumns;
        return result;
    }
};

class TProcessorContext {
protected:
    std::vector<TColumnChainInfo> ColumnsToFetch;
    std::vector<TColumnChainInfo> OriginalColumnsToUse;
    std::vector<TColumnChainInfo> ColumnsToDrop;

public:
    const std::vector<TColumnChainInfo>& GetColumnsToFetch() const {
        return ColumnsToFetch;
    }
    const std::vector<TColumnChainInfo>& GetOriginalColumnsToUse() const {
        return OriginalColumnsToUse;
    }
    const std::vector<TColumnChainInfo>& GetColumnsToDrop() const {
        return ColumnsToDrop;
    }

    TProcessorContext(
        std::vector<TColumnChainInfo>&& toFetch, std::vector<TColumnChainInfo>&& originalToUse, std::vector<TColumnChainInfo>&& toDrop)
        : ColumnsToFetch(std::move(toFetch))
        , OriginalColumnsToUse(std::move(originalToUse))
        , ColumnsToDrop(std::move(toDrop)) {
    }
};

class IResourceProcessor {
private:
    YDB_READONLY_DEF(std::vector<TColumnChainInfo>, Input);
    YDB_READONLY_DEF(std::vector<TColumnChainInfo>, Output);
    YDB_READONLY(EProcessorType, ProcessorType, EProcessorType::Unknown);

    virtual TConclusionStatus DoExecute(const std::shared_ptr<TAccessorsCollection>& resources, const TProcessorContext& context) const = 0;

    virtual NJson::TJsonValue DoDebugJson() const {
        return NJson::JSON_MAP;
    }
    virtual ui64 DoGetWeight() const {
        return 0;
    }

public:
    ui64 GetWeight() const {
        return DoGetWeight();
    }

    virtual std::optional<TFetchingInfo> BuildFetchTask(
        const ui32 columnId, const NAccessor::IChunkedArray::EType arrType, const std::shared_ptr<TAccessorsCollection>& resources) const;

    virtual bool IsAggregation() const = 0;

    virtual ~IResourceProcessor() = default;

    NJson::TJsonValue DebugJson() const;

    ui32 GetOutputColumnIdOnce() const {
        AFL_VERIFY(Output.size() == 1)("size", Output.size());
        return Output.front().GetColumnId();
    }

    ui32 GetInputColumnIdOnce() const {
        AFL_VERIFY(Input.size() == 1)("size", Input.size());
        return Input.front().GetColumnId();
    }

    IResourceProcessor(std::vector<TColumnChainInfo>&& input, std::vector<TColumnChainInfo>&& output, const EProcessorType type)
        : Input(std::move(input))
        , Output(std::move(output))
        , ProcessorType(type) {
    }

    [[nodiscard]] TConclusionStatus Execute(const std::shared_ptr<TAccessorsCollection>& resources, const TProcessorContext& context) const;
};

class TResourceProcessorStep: public TProcessorContext {
private:
    using TBase = TProcessorContext;
    YDB_READONLY_DEF(std::shared_ptr<IResourceProcessor>, Processor);

public:
    NJson::TJsonValue DebugJson() const;

    TResourceProcessorStep(std::vector<TColumnChainInfo>&& toFetch, std::vector<TColumnChainInfo>&& originalToUse,
        std::shared_ptr<IResourceProcessor>&& processor, std::vector<TColumnChainInfo>&& toDrop)
        : TBase(std::move(toFetch), std::move(originalToUse), std::move(toDrop))
        , Processor(std::move(processor)) {
        AFL_VERIFY(Processor);
    }

    const IResourceProcessor* operator->() const {
        return Processor.get();
    }

    const IResourceProcessor& operator*() const {
        return *Processor;
    }
};

}   // namespace NKikimr::NArrow::NSSA
