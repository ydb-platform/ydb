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

struct TDatumBatch {
    std::shared_ptr<arrow::Schema> Schema;
    std::vector<arrow::Datum> Datums;
    int64_t Rows{};

    arrow::Status AddColumn(const std::string& name, arrow::Datum&& column);
    arrow::Result<arrow::Datum> GetColumnByName(const std::string& name) const;
    std::shared_ptr<arrow::RecordBatch> ToRecordBatch() const;
    static std::shared_ptr<TDatumBatch> FromRecordBatch(std::shared_ptr<arrow::RecordBatch>& batch);
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
    std::optional<std::vector<arrow::Datum>> BuildArgs(const TDatumBatch& batch, const std::vector<std::string>& args) const {
        std::vector<arrow::Datum> arguments;
        arguments.reserve(args.size());
        for (auto& colName : args) {
            auto column = batch.GetColumnByName(colName);
            if (!column.ok()) {
                return {};
            }
            arguments.push_back(*column);
        }
        return std::move(arguments);
    }
};

class TAssign {
public:
    using TOperationType = EOperation;

    TAssign(const std::string& name, EOperation op, std::vector<std::string>&& args)
        : Name(name)
        , Operation(ValidateOperation(op, args.size()))
        , Arguments(std::move(args))
        , FuncOpts(nullptr)
    {}

    TAssign(const std::string& name, EOperation op, std::vector<std::string>&& args, std::shared_ptr<arrow::compute::FunctionOptions> funcOpts)
        : Name(name)
        , Operation(ValidateOperation(op, args.size()))
        , Arguments(std::move(args))
        , FuncOpts(std::move(funcOpts))
    {}

    explicit TAssign(const std::string& name, bool value)
        : Name(name)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::BooleanScalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const std::string& name, i32 value)
        : Name(name)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::Int32Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const std::string& name, ui32 value)
        : Name(name)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::UInt32Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const std::string& name, i64 value)
        : Name(name)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::Int64Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const std::string& name, ui64 value)
        : Name(name)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::UInt64Scalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const std::string& name, float value)
        : Name(name)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::FloatScalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const std::string& name, double value)
        : Name(name)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::DoubleScalar>(value))
        , FuncOpts(nullptr)
    {}

    explicit TAssign(const std::string& name, const std::string& value, bool binary)
        : Name(name)
        , Operation(EOperation::Constant)
        , Constant( binary ? std::make_shared<arrow::BinaryScalar>(arrow::Buffer::FromString(value), arrow::binary())
                              : std::make_shared<arrow::StringScalar>(value))
        , FuncOpts(nullptr)
    {}

    TAssign(const std::string& name, const std::shared_ptr<arrow::Scalar>& value)
        : Name(name)
        , Operation(EOperation::Constant)
        , Constant(value)
        , FuncOpts(nullptr)
    {}

    TAssign(const std::string& name,
            TFunctionPtr kernelFunction,
            std::vector<std::string>&& args,
            std::shared_ptr<arrow::compute::FunctionOptions> funcOpts)
        : Name(name)
        , Arguments(std::move(args))
        , FuncOpts(std::move(funcOpts))
        , KernelFunction(std::move(kernelFunction))
    {}

    bool IsConstant() const { return Operation == EOperation::Constant; }
    bool IsOk() const { return Operation != EOperation::Unspecified || !!KernelFunction; }
    EOperation GetOperation() const { return Operation; }
    const std::vector<std::string>& GetArguments() const { return Arguments; }
    std::shared_ptr<arrow::Scalar> GetConstant() const { return Constant; }
    const std::string& GetName() const { return Name; }
    const arrow::compute::FunctionOptions* GetOptions() const { return FuncOpts.get(); }

    IStepFunction<TAssign>::TPtr GetFunction(arrow::compute::ExecContext* ctx) const;
private:
    std::string Name;
    EOperation Operation{EOperation::Unspecified};
    std::vector<std::string> Arguments;
    std::shared_ptr<arrow::Scalar> Constant;
    std::shared_ptr<arrow::compute::FunctionOptions> FuncOpts;
    TFunctionPtr KernelFunction;
};

class TAggregateAssign {
public:
    using TOperationType = EAggregate;

    TAggregateAssign(const std::string& name, EAggregate op = EAggregate::Unspecified)
        : Name(name)
        , Operation(op)
    {
        if (op != EAggregate::Count) {
            op = EAggregate::Unspecified;
        }
    }

    TAggregateAssign(const std::string& name, EAggregate op, std::string&& arg)
        : Name(name)
        , Operation(op)
        , Arguments({std::move(arg)})
    {
        if (Arguments.empty()) {
            op = EAggregate::Unspecified;
        }
    }

    TAggregateAssign(const std::string& name,
            TFunctionPtr kernelFunction,
            std::vector<std::string>&& args)
        : Name(name)
        , Arguments(std::move(args))
        , KernelFunction(kernelFunction)
    {}

    bool IsOk() const { return Operation != EAggregate::Unspecified || !!KernelFunction; }
    EAggregate GetOperation() const { return Operation; }
    const std::vector<std::string>& GetArguments() const { return Arguments; }
    std::vector<std::string>& MutableArguments() { return Arguments; }
    const std::string& GetName() const { return Name; }
    const arrow::compute::ScalarAggregateOptions* GetOptions() const { return &ScalarOpts; }

    IStepFunction<TAggregateAssign>::TPtr GetFunction(arrow::compute::ExecContext* ctx) const;

private:
    std::string Name;
    EAggregate Operation{EAggregate::Unspecified};
    std::vector<std::string> Arguments;
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
struct TProgramStep {
    std::vector<TAssign> Assignes;
    std::vector<std::string> Filters; // List of filter columns. Implicit "Filter by (f1 AND f2 AND .. AND fn)"
    std::vector<TAggregateAssign> GroupBy;
    std::vector<std::string> GroupByKeys; // TODO: it's possible to use them without GROUP BY for DISTINCT
    std::vector<std::string> Projection; // Step's result columns (remove others)

    using TDatumBatch = TDatumBatch;

    std::set<std::string> GetColumnsInUsage() const;

    bool Empty() const {
        return Assignes.empty() && Filters.empty() && Projection.empty() && GroupBy.empty() && GroupByKeys.empty();
    }

    arrow::Status Apply(std::shared_ptr<arrow::RecordBatch>& batch, arrow::compute::ExecContext* ctx) const;

    arrow::Status ApplyAssignes(TDatumBatch& batch, arrow::compute::ExecContext* ctx) const;
    arrow::Status ApplyAggregates(TDatumBatch& batch, arrow::compute::ExecContext* ctx) const;
    arrow::Status ApplyFilters(TDatumBatch& batch) const;
    arrow::Status ApplyProjection(std::shared_ptr<arrow::RecordBatch>& batch) const;
    arrow::Status ApplyProjection(TDatumBatch& batch) const;

    arrow::Status MakeCombinedFilter(TDatumBatch& batch, NArrow::TColumnFilter& result) const;
};

struct TProgram {
    std::vector<std::shared_ptr<TProgramStep>> Steps;
    THashMap<ui32, TString> SourceColumns;

    TProgram() = default;

    TProgram(std::vector<std::shared_ptr<TProgramStep>>&& steps)
        : Steps(std::move(steps))
    {}

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
    NArrow::TColumnFilter MakeEarlyFilter(const std::shared_ptr<arrow::RecordBatch>& batch,
                                      arrow::compute::ExecContext* ctx) const;
};

inline arrow::Status ApplyProgram(
    std::shared_ptr<arrow::RecordBatch>& batch,
    const TProgram& program,
    arrow::compute::ExecContext* ctx = nullptr)
{
    return program.ApplyTo(batch, ctx);
}

}
