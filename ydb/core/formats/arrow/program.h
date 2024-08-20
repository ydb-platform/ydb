#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_aggregate.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <util/system/types.h>

#include <ydb/library/arrow_kernels/operations.h>
#include <ydb/core/scheme_types/scheme_types_defs.h>
#include "arrow_helpers.h"
#include "arrow_filter.h"

namespace NKikimr::NArrow {

using EOperation = NKikimr::NKernels::EOperation;

enum class EAggregate {
    Unspecified = 0,
    Some = 1,
    Count = 2,
    Min = 3,
    Max = 4,
    Sum = 5,
    //Avg = 6,
};

}

namespace NKikimr::NSsa {

using EOperation = NArrow::EOperation;
using EAggregate = NArrow::EAggregate;
using TFunctionPtr = std::shared_ptr<arrow::compute::ScalarFunction>;

const char * GetFunctionName(EOperation op);
const char * GetFunctionName(EAggregate op);
const char * GetHouseFunctionName(EAggregate op);
inline const char * GetHouseGroupByName() { return "ch.group_by"; }
EOperation ValidateOperation(EOperation op, ui32 argsSize);

class TDatumBatch {
private:
    std::shared_ptr<arrow::Schema> SchemaBase;
    THashMap<std::string, ui32> NewColumnIds;
    std::vector<std::shared_ptr<arrow::Field>> NewColumnsPtr;
    int64_t Rows = 0;

public:
    std::vector<arrow::Datum> Datums;

    ui64 GetRecordsCount() const {
        return Rows;
    }

    void SetRecordsCount(const ui64 value) {
        Rows = value;
    }

    TDatumBatch(const std::shared_ptr<arrow::Schema>& schema, std::vector<arrow::Datum>&& datums, const i64 rows);

    const std::shared_ptr<arrow::Schema>& GetSchema() {
        if (NewColumnIds.size()) {
            std::vector<std::shared_ptr<arrow::Field>> fields = SchemaBase->fields();
            fields.insert(fields.end(), NewColumnsPtr.begin(), NewColumnsPtr.end());
            SchemaBase = std::make_shared<arrow::Schema>(fields);
            NewColumnIds.clear();
            NewColumnsPtr.clear();
        }
        return SchemaBase;
    }

    arrow::Status AddColumn(const std::string& name, arrow::Datum&& column);
    arrow::Result<arrow::Datum> GetColumnByName(const std::string& name) const;
    bool HasColumn(const std::string& name) const {
        if (NewColumnIds.contains(name)) {
            return true;
        }
        return SchemaBase->GetFieldIndex(name) > -1;
    }
    std::shared_ptr<arrow::Table> ToTable();
    std::shared_ptr<arrow::RecordBatch> ToRecordBatch();
    static std::shared_ptr<TDatumBatch> FromRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
    static std::shared_ptr<TDatumBatch> FromTable(const std::shared_ptr<arrow::Table>& batch);
};

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

template <class TAssignObject>
class IStepFunction {
    using TSelf = IStepFunction<TAssignObject>;
protected:
    arrow::compute::ExecContext* Ctx;
public:
    using TPtr = std::shared_ptr<TSelf>;

    IStepFunction(arrow::compute::ExecContext* ctx)
        : Ctx(ctx)
    {}

    virtual ~IStepFunction() {}

    virtual arrow::Result<arrow::Datum> Call(const TAssignObject& assign, const TDatumBatch& batch) const = 0;

protected:
    std::optional<std::vector<arrow::Datum>> BuildArgs(const TDatumBatch& batch, const std::vector<TColumnInfo>& args) const {
        std::vector<arrow::Datum> arguments;
        arguments.reserve(args.size());
        for (auto& colName : args) {
            auto column = NArrow::TStatusValidator::GetValid(batch.GetColumnByName(colName.GetColumnName()));
            arguments.push_back(column);
        }
        return std::move(arguments);
    }
};

class TAssign {
private:
    YDB_ACCESSOR_DEF(std::optional<ui32>, YqlOperationId);
public:
    using TOperationType = EOperation;

    TAssign(const TColumnInfo& column, EOperation op, std::vector<TColumnInfo>&& args)
        : Column(column)
        , Operation(ValidateOperation(op, args.size()))
        , Arguments(std::move(args))
        , FuncOpts(nullptr)
    {}

    TAssign(const TColumnInfo& column, EOperation op, std::vector<TColumnInfo>&& args, std::shared_ptr<arrow::compute::FunctionOptions> funcOpts)
        : Column(column)
        , Operation(ValidateOperation(op, args.size()))
        , Arguments(std::move(args))
        , FuncOpts(std::move(funcOpts))
    {}

    explicit TAssign(const TColumnInfo& column, bool value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::BooleanScalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const TColumnInfo& column, i8 value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::Int8Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const TColumnInfo& column, ui8 value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::UInt8Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const TColumnInfo& column, i16 value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::Int16Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const TColumnInfo& column, ui16 value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::UInt16Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const TColumnInfo& column, i32 value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::Int32Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const TColumnInfo& column, ui32 value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::UInt32Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const TColumnInfo& column, i64 value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::Int64Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const TColumnInfo& column, ui64 value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::UInt64Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const TColumnInfo& column, float value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::FloatScalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const TColumnInfo& column, double value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::DoubleScalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const TColumnInfo& column, const std::string& value, bool binary)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant( binary ? std::make_shared<arrow::BinaryScalar>(arrow::Buffer::FromString(value), arrow::binary())
                              : std::make_shared<arrow::StringScalar>(value))
        , FuncOpts(nullptr)
    {}

    TAssign(const TColumnInfo& column, const std::shared_ptr<arrow::Scalar>& value)
        : Column(column)
        , Operation(EOperation::Constant)
        , Constant(value)
        , FuncOpts(nullptr)
    {}

    TAssign(const TColumnInfo& column,
            TFunctionPtr kernelFunction,
            std::vector<TColumnInfo>&& args,
            std::shared_ptr<arrow::compute::FunctionOptions> funcOpts)
        : Column(column)
        , Arguments(std::move(args))
        , FuncOpts(std::move(funcOpts))
        , KernelFunction(std::move(kernelFunction))
    {}

    static TAssign MakeTimestamp(const TColumnInfo& column, ui64 value);

    bool IsConstant() const { return Operation == EOperation::Constant; }
    bool IsOk() const { return Operation != EOperation::Unspecified || !!KernelFunction; }
    EOperation GetOperation() const { return Operation; }
    const std::vector<TColumnInfo>& GetArguments() const { return Arguments; }
    std::shared_ptr<arrow::Scalar> GetConstant() const { return Constant; }
    const TColumnInfo& GetColumn() const { return Column; }
    const std::string& GetName() const { return Column.GetColumnName(); }
    const arrow::compute::FunctionOptions* GetOptions() const { return FuncOpts.get(); }

    IStepFunction<TAssign>::TPtr GetFunction(arrow::compute::ExecContext* ctx) const;
    TString DebugString() const;
private:
    const TColumnInfo Column;
    EOperation Operation{EOperation::Unspecified};
    std::vector<TColumnInfo> Arguments;
    std::shared_ptr<arrow::Scalar> Constant;
    std::shared_ptr<arrow::compute::FunctionOptions> FuncOpts;
    TFunctionPtr KernelFunction;
};

class TAggregateAssign {
public:
    using TOperationType = EAggregate;

    TAggregateAssign(const TColumnInfo& column, EAggregate op = EAggregate::Unspecified)
        : Column(column)
        , Operation(op)
    {
        if (op != EAggregate::Count) {
            op = EAggregate::Unspecified;
        }
    }

    TAggregateAssign(const TColumnInfo& column, EAggregate op, const TColumnInfo& arg)
        : Column(column)
        , Operation(op)
        , Arguments({arg})
    {
        if (Arguments.empty()) {
            op = EAggregate::Unspecified;
        }
    }

    TAggregateAssign(const TColumnInfo& column,
            TFunctionPtr kernelFunction,
            const std::vector<TColumnInfo>& args)
        : Column(column)
        , Arguments(args)
        , KernelFunction(kernelFunction)
    {}

    bool IsOk() const { return Operation != EAggregate::Unspecified || !!KernelFunction; }
    EAggregate GetOperation() const { return Operation; }
    const std::vector<TColumnInfo>& GetArguments() const { return Arguments; }
    std::vector<TColumnInfo>& MutableArguments() { return Arguments; }
    const std::string& GetName() const { return Column.GetColumnName(); }
    const arrow::compute::ScalarAggregateOptions* GetOptions() const { return &ScalarOpts; }

    IStepFunction<TAggregateAssign>::TPtr GetFunction(arrow::compute::ExecContext* ctx) const;
    TString DebugString() const;

private:
    TColumnInfo Column;
    EAggregate Operation{EAggregate::Unspecified};
    std::vector<TColumnInfo> Arguments;
    arrow::compute::ScalarAggregateOptions ScalarOpts; // TODO: make correct options
    TFunctionPtr KernelFunction;
};


/// Group of commands that finishes with projection. Steps add locality for columns definition.
///
/// In step we have non-decreasing count of columns (line to line) till projection. So columns are either source
/// for the step either defined in this step.
/// It's also possible to use several filters in step. They would be applyed after assignes, just before projection.
/// "Filter (a > 0 AND b <= 42)" is logically equal to "Filret a > 0; Filter b <= 42"
/// Step combines (f1 AND f2 AND ... AND fn) into one filter and applies it once. You have to split filters in different
/// steps if you want to run them separately. I.e. if you expect that f1 is fast and leads to a small row-set.
/// Then when we place all assignes before filters they have the same row count. It's possible to run them in parallel.
class TProgramStep {
private:
    YDB_READONLY_DEF(std::vector<TAssign>, Assignes);
    YDB_READONLY_DEF(std::vector<TColumnInfo>, Filters); // List of filter columns. Implicit "Filter by (f1 AND f2 AND .. AND fn)"
    std::set<ui32> FilterOriginalColumnIds;

    YDB_ACCESSOR_DEF(std::vector<TAggregateAssign>, GroupBy);
    YDB_READONLY_DEF(std::vector<TColumnInfo>, GroupByKeys); // TODO: it's possible to use them without GROUP BY for DISTINCT
    YDB_READONLY_DEF(std::vector<TColumnInfo>, Projection); // Step's result columns (remove others)
public:
    using TDatumBatch = TDatumBatch;

    TString DebugString() const {
        TStringBuilder sb;
        sb << "{";
        if (Assignes.size()) {
            sb << "assignes=[";
            for (auto&& i : Assignes) {
                sb << i.DebugString() << ";";
            }
            sb << "];";
        }
        if (Filters.size()) {
            sb << "filters=[";
            for (auto&& i : Filters) {
                sb << i.DebugString() << ";";
            }
            sb << "];";
        }
        if (GroupBy.size()) {
            sb << "group_by_assignes=[";
            for (auto&& i : GroupBy) {
                sb << i.DebugString() << ";";
            }
            sb << "];";
        }
        if (GroupByKeys.size()) {
            sb << "group_by_keys=[";
            for (auto&& i : GroupByKeys) {
                sb << i.DebugString() << ";";
            }
            sb << "];";
        }

        sb << "projections=[";
        for (auto&& i : Projection) {
            sb << i.DebugString() << ";";
        }
        sb << "];";

        sb << "}";
        return sb;
    }

    std::set<std::string> GetColumnsInUsage(const bool originalOnly = false) const;

    const std::set<ui32>& GetFilterOriginalColumnIds() const;

    void AddAssigne(const TAssign& a) {
        if (!a.GetColumn().IsGenerated()) {
            FilterOriginalColumnIds.emplace(a.GetColumn().GetColumnId());
        }
        for (auto&& i : a.GetArguments()) {
            if (!i.IsGenerated()) {
                FilterOriginalColumnIds.emplace(i.GetColumnId());
            }
        }
        Assignes.emplace_back(a);
    }
    void AddFilter(const TColumnInfo& f) {
        if (!f.IsGenerated()) {
            FilterOriginalColumnIds.emplace(f.GetColumnId());
        }
        Filters.emplace_back(f);
    }
    void AddGroupBy(const TAggregateAssign& g) {
        GroupBy.emplace_back(g);
    }
    void AddGroupByKeys(const TColumnInfo& c) {
        GroupByKeys.emplace_back(c);
    }
    void AddProjection(const TColumnInfo& c) {
        Projection.emplace_back(c);
    }

    bool Empty() const {
        return Assignes.empty() && Filters.empty() && Projection.empty() && GroupBy.empty() && GroupByKeys.empty();
    }

    arrow::Status Apply(std::shared_ptr<arrow::RecordBatch>& batch, arrow::compute::ExecContext* ctx) const;

    [[nodiscard]] arrow::Status ApplyAssignes(TDatumBatch& batch, arrow::compute::ExecContext* ctx) const;
    arrow::Status ApplyAggregates(TDatumBatch& batch, arrow::compute::ExecContext* ctx) const;
    arrow::Status ApplyFilters(TDatumBatch& batch) const;
    arrow::Status ApplyProjection(std::shared_ptr<arrow::RecordBatch>& batch) const;
    arrow::Status ApplyProjection(TDatumBatch& batch) const;

    arrow::Status MakeCombinedFilter(TDatumBatch& batch, NArrow::TColumnFilter& result) const;

    bool IsFilterOnly() const {
        return Filters.size() && (!GroupBy.size() && !GroupByKeys.size());
    }

    [[nodiscard]] arrow::Result<std::shared_ptr<NArrow::TColumnFilter>> BuildFilter(const std::shared_ptr<NArrow::TGeneralContainer>& t) const;
};

struct TProgram {
public:
    std::vector<std::shared_ptr<TProgramStep>> Steps;
    THashMap<ui32, TColumnInfo> SourceColumns;

    TProgram() = default;

    TProgram(std::vector<std::shared_ptr<TProgramStep>>&& steps)
        : Steps(std::move(steps))
    {}

    arrow::Status ApplyTo(std::shared_ptr<arrow::Table>& table, arrow::compute::ExecContext* ctx) const {
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = NArrow::SliceToRecordBatches(table);
        for (auto&& i : batches) {
            auto status = ApplyTo(i, ctx);
            if (!status.ok()) {
                return status;
            }
        }
        table = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches(batches));
        return arrow::Status::OK();
    }

    arrow::Status ApplyTo(std::shared_ptr<arrow::RecordBatch>& batch, arrow::compute::ExecContext* ctx) const {
        try {
            for (auto& step : Steps) {
                auto status = step->Apply(batch, ctx);
                if (!status.ok()) {
                    return status;
                }
            }
        } catch (const std::exception& ex) {
            return arrow::Status::Invalid(ex.what());
        }
        return arrow::Status::OK();
    }

    std::set<std::string> GetEarlyFilterColumns() const;
    std::set<std::string> GetProcessingColumns() const;
    TString DebugString() const {
        TStringBuilder sb;
        sb << "[";
        for (auto&& i : Steps) {
            sb << i->DebugString() << ";";
        }
        sb << "]";
        return sb;
    }
};

inline arrow::Status ApplyProgram(
    std::shared_ptr<arrow::Table>& batch,
    const TProgram& program,
    arrow::compute::ExecContext* ctx = nullptr) {
    return program.ApplyTo(batch, ctx);
}

inline arrow::Status ApplyProgram(
    std::shared_ptr<arrow::RecordBatch>& batch,
    const TProgram& program,
    arrow::compute::ExecContext* ctx = nullptr) {
    return program.ApplyTo(batch, ctx);
}

}
