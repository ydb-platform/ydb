#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_aggregate.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <util/system/types.h>

#include <ydb/core/scheme_types/scheme_types_defs.h>

namespace NKikimr::NArrow {

enum class EOperation {
    Unspecified = 0,
    Constant,
    //
    CastBoolean,
    CastInt8,
    CastInt16,
    CastInt32,
    CastInt64,
    CastUInt8,
    CastUInt16,
    CastUInt32,
    CastUInt64,
    CastFloat,
    CastDouble,
    CastBinary,
    CastFixedSizeBinary,
    CastString,
    CastTimestamp,
    //
    IsValid,
    IsNull,
    //
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    //
    Invert,
    And,
    Or,
    Xor,
    //
    Add,
    Subtract,
    Multiply,
    Divide,
    Abs,
    Negate,
    Gcd,
    Lcm,
    Modulo,
    ModuloOrZero,
    AddNotNull,
    SubtractNotNull,
    MultiplyNotNull,
    DivideNotNull,
    //
    BinaryLength,
    MatchSubstring,
    // math
    Acosh,
    Atanh,
    Cbrt,
    Cosh,
    E,
    Erf,
    Erfc,
    Exp,
    Exp2,
    Exp10,
    Hypot,
    Lgamma,
    Pi,
    Sinh,
    Sqrt,
    Tgamma,
    // round
    Floor,
    Ceil,
    Trunc,
    Round,
    RoundBankers,
    RoundToExp2
};

enum class EAggregate {
    Unspecified = 0,
    Any = 1,
    Count = 2,
    Min = 3,
    Max = 4,
    Sum = 5,
    Avg = 6,
};

const char * GetFunctionName(EOperation op);
const char * GetFunctionName(EAggregate op);
EOperation ValidateOperation(EOperation op, ui32 argsSize);

class TAssign {
public:
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
        , FuncOpts(funcOpts)
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

    explicit TAssign(const std::string& name, const std::string& value)
        : Name(name)
        , Operation(EOperation::Constant)
        , Constant(std::make_shared<arrow::StringScalar>(value))
        , FuncOpts(nullptr)
    {}

    TAssign(const std::string& name, const std::shared_ptr<arrow::Scalar>& value)
        : Name(name)
        , Operation(EOperation::Constant)
        , Constant(value)
        , FuncOpts(nullptr)
    {}

    bool IsConstant() const { return Operation == EOperation::Constant; }
    bool IsOk() const { return Operation != EOperation::Unspecified; }
    EOperation GetOperation() const { return Operation; }
    const std::vector<std::string>& GetArguments() const { return Arguments; }
    std::shared_ptr<arrow::Scalar> GetConstant() const { return Constant; }
    const std::string& GetName() const { return Name; }
    const arrow::compute::FunctionOptions* GetFunctionOptions() const { return FuncOpts.get(); }

private:
    std::string Name;
    EOperation Operation{EOperation::Unspecified};
    std::vector<std::string> Arguments;
    std::shared_ptr<arrow::Scalar> Constant;
    std::shared_ptr<arrow::compute::FunctionOptions> FuncOpts;
};

class TAggregateAssign {
public:
    TAggregateAssign(const std::string& name, EAggregate op, std::string&& arg)
        : Name(name)
        , Operation(op)
        , Arguments({std::move(arg)})
    {
        if (arg.empty() && op != EAggregate::Count) {
            // COUNT(*) doesn't have arguments
            op = EAggregate::Unspecified;
        }
    }

    bool IsOk() const { return Operation != EAggregate::Unspecified; }
    EAggregate GetOperation() const { return Operation; }
    const std::vector<std::string>& GetArguments() const { return Arguments; }
    const std::string& GetName() const { return Name; }
    const arrow::compute::ScalarAggregateOptions& GetAggregateOptions() const { return ScalarOpts; }

private:
    std::string Name;
    EAggregate Operation{EAggregate::Unspecified};
    std::vector<std::string> Arguments;
    arrow::compute::ScalarAggregateOptions ScalarOpts; // TODO: make correct options
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
    std::vector<std::string> Projection; // Step's result columns (remove others)

    struct TDatumBatch {
        std::shared_ptr<arrow::Schema> fields;
        int64_t rows;
        std::vector<arrow::Datum> datums;
    };

    bool Empty() const {
        return Assignes.empty() && Filters.empty() && Projection.empty();
    }

    void Apply(std::shared_ptr<arrow::RecordBatch>& batch, arrow::compute::ExecContext* ctx) const;

    void ApplyAssignes(std::shared_ptr<TDatumBatch>& batch, arrow::compute::ExecContext* ctx) const;
    void ApplyAggregates(std::shared_ptr<TDatumBatch>& batch, arrow::compute::ExecContext* ctx) const;
    void ApplyFilters(std::shared_ptr<TDatumBatch>& batch) const;
    void ApplyProjection(std::shared_ptr<arrow::RecordBatch>& batch) const;
    void ApplyProjection(std::shared_ptr<TDatumBatch>& batch) const;
};

inline void ApplyProgram(std::shared_ptr<arrow::RecordBatch>& batch,
                         const std::vector<std::shared_ptr<TProgramStep>>& program,
                         arrow::compute::ExecContext* ctx = nullptr) {
    for (auto& step : program) {
        step->Apply(batch, ctx);
    }
}

struct TSsaProgramSteps {
    std::vector<std::shared_ptr<TProgramStep>> Program;
    THashMap<ui32, TString> ProgramSourceColumns;
};

}
