#pragma once

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>


namespace NKikimr {
namespace NMiniKQL {

static constexpr NUdf::TDataTypeId LegacyPairUi64Ui64 = 0x101;

struct TSelectColumn {
    TStringBuf Label;
    ui32 ColumnId = 0;
    NScheme::TTypeInfo SchemeType;
    EColumnTypeConstraint TypeConstraint = EColumnTypeConstraint::Nullable;
    TSelectColumn()
    {}

    TSelectColumn(TStringBuf label, ui32 columnId, NScheme::TTypeInfo schemeType, EColumnTypeConstraint typeConstraint)
        : Label(label)
        , ColumnId(columnId)
        , SchemeType(schemeType)
        , TypeConstraint(typeConstraint)
    {}
};

struct TReadRangeOptions
{
    struct TFlags {
        enum : ui32 {
            IncludeInitValue = 0x00,
            ExcludeInitValue = 0x01,

            IncludeTermValue = 0x00,
            ExcludeTermValue = 0x02,

            IncludeBoth = IncludeInitValue | IncludeTermValue,
            ExcludeBoth = ExcludeInitValue | ExcludeTermValue,
            Default = IncludeBoth
        };
    };

    TReadRangeOptions(ui32 valueType, const TTypeEnvironment& env);

    TRuntimeNode ItemsLimit;
    TRuntimeNode BytesLimit;
    TRuntimeNode InitValue;
    TRuntimeNode TermValue;
    TRuntimeNode PayloadStruct;
    TRuntimeNode Flags;
};

// Supported types - Data or Optional of Data for each column
using TKeyColumnValues = TArrayRef<const TRuntimeNode>;

struct TTableRangeOptions
{
    TTableRangeOptions(const TTypeEnvironment& env);

    TRuntimeNode ItemsLimit; // default = ui64 0
    TRuntimeNode BytesLimit; // default = ui64 0
    TRuntimeNode Flags; // default = ui64 IncludeInitValue | IncludeTermValue
    TKeyColumnValues FromColumns;
    TKeyColumnValues ToColumns;
    TArrayRef<bool> SkipNullKeys;
    TArrayRef<bool> ForbidNullArgsFrom;
    TArrayRef<bool> ForbidNullArgsTo;
    TRuntimeNode Reverse; // default = <unset>
};

enum class EInplaceUpdateMode
{
    Unknown = 0,
    FirstMode = 1,
    Sum = FirstMode,
    Min = 2,
    Max = 3,
    IfNotExistOrEmpty = 4,
    LastMode
};

const char* InplaceUpdateModeToCString(EInplaceUpdateMode mode);
EInplaceUpdateMode InplaceUpdateModeFromCString(const char* str);

class TUpdateRowBuilder
{
public:
    TUpdateRowBuilder(const TTypeEnvironment& env);
    TUpdateRowBuilder(const TUpdateRowBuilder&) = default;
    TUpdateRowBuilder& operator=(const TUpdateRowBuilder&) = default;
    // Supports Data or Optional of Data
    void SetColumn(ui32 columnId, NScheme::TTypeInfo expectedType, TRuntimeNode value);
    // Supports Data
    void InplaceUpdateColumn(
            ui32 columnId, NScheme::TTypeInfo expectedType,
            TRuntimeNode value, EInplaceUpdateMode mode);
    void EraseColumn(ui32 columnId);
    TRuntimeNode Build();

private:
    TStructLiteralBuilder Builder;
    const TTypeEnvironment& Env;
    TInternName NullInternName;
};

class TParametersBuilder
{
public:
    TParametersBuilder(const TTypeEnvironment& env);
    TParametersBuilder(const TParametersBuilder&) = default;
    TParametersBuilder& operator=(const TParametersBuilder&) = default;
    TParametersBuilder& Add(const TStringBuf& name, TRuntimeNode value);
    TRuntimeNode Build();

private:
    TStructLiteralBuilder StructBuilder;
};

class TKikimrProgramBuilder: public TProgramBuilder
{
public:
    struct TBindFlags {
        enum {
            OptimizeLiterals = 0x1,
            Default = OptimizeLiterals,
            DisableOptimization = 0
        };
    };

    TKikimrProgramBuilder(
            const TTypeEnvironment& env,
            const IFunctionRegistry& functionRegistry);

    //-- table db functions
    // row should contain only static nodes, columns - structType
    // returns Optional of structType
    TRuntimeNode SelectRow(
            const TTableId& tableId,
            const TArrayRef<NScheme::TTypeInfo>& keyTypes,
            const TArrayRef<const TSelectColumn>& columns,
            const TKeyColumnValues& row,
            const TReadTarget& target = TReadTarget());

    TRuntimeNode SelectRow(
            const TTableId& tableId,
            const TArrayRef<NScheme::TTypeInfo>& keyTypes,
            const TArrayRef<const TSelectColumn>& columns,
            const TKeyColumnValues& row,
            TRuntimeNode readTarget);

    TTableRangeOptions GetDefaultTableRangeOptions() const {
        return TTableRangeOptions(Env);
    }

    // from/to should contain only static nodes, columns - structType
    // returns Struct with fields { List: List of structType, Truncated: Bool }
    TRuntimeNode SelectRange(
            const TTableId& tableId,
            const TArrayRef<NScheme::TTypeInfo>& keyTypes,
            const TArrayRef<const TSelectColumn>& columns,
            const TTableRangeOptions& options,
            const TReadTarget& target = TReadTarget());

    TRuntimeNode SelectRange(
            const TTableId& tableId,
            const TArrayRef<NScheme::TTypeInfo>& keyTypes,
            const TArrayRef<const TSelectColumn>& columns,
            const TTableRangeOptions& options,
            TRuntimeNode readTarget);

    TUpdateRowBuilder GetUpdateRowBuilder() const {
        return TUpdateRowBuilder(Env);
    }

    // returns Void
    TRuntimeNode UpdateRow(
            const TTableId& tableId,
            const TArrayRef<NScheme::TTypeInfo>& keyTypes,
            const TKeyColumnValues& row,
            TUpdateRowBuilder& update);

    // returns Void
    TRuntimeNode EraseRow(
            const TTableId& tableId,
            const TArrayRef<NScheme::TTypeInfo>& keyTypes,
            const TKeyColumnValues& row);

    //-- parameters functions
    TRuntimeNode Prepare(TRuntimeNode listOfVoid);

    TRuntimeNode Parameter(const TStringBuf& name, TType* type);

    TRuntimeNode MapParameter(
            TRuntimeNode list, // must be a parameter
            std::function<TRuntimeNode(TRuntimeNode item)> handler);

    TRuntimeNode FlatMapParameter(
            TRuntimeNode list,
            std::function<TRuntimeNode(TRuntimeNode item)> handler);

    TRuntimeNode AcquireLocks(TRuntimeNode lockTxId);

    TRuntimeNode ReadTarget(const TReadTarget& target);

    TRuntimeNode CombineByKeyMerge(TRuntimeNode list);

    TRuntimeNode Diagnostics();

    TRuntimeNode PartialSort(TRuntimeNode list, TRuntimeNode ascending,
        std::function<TRuntimeNode(TRuntimeNode item)> keyExtractor);

    TRuntimeNode PartialTake(TRuntimeNode list, TRuntimeNode count);

    TRuntimeNode Bind(
            TRuntimeNode program,
            TRuntimeNode parameters,
            ui32 bindFlags = TBindFlags::Default);

    TRuntimeNode Build(
            TRuntimeNode listOfVoid,
            ui32 bindFlags = TBindFlags::Default);

    TParametersBuilder GetParametersBuilder() const {
        return TParametersBuilder(Env);
    }

    //-- special functions
    TRuntimeNode Abort();
    TRuntimeNode StepTxId();
    TRuntimeNode SetResult(const TStringBuf& label, TRuntimeNode payload);

    using TProgramBuilder::NewDataLiteral;
private:
    TRuntimeNode NewDataLiteral(const std::pair<ui64, ui64>& data) const;
    TRuntimeNode BuildTableId(const TTableId& tableId) const;
    TVector<TRuntimeNode> FixKeysType(
        const TArrayRef<NScheme::TTypeInfo>& keyTypes,
        const TKeyColumnValues& row) const;
    TRuntimeNode RewriteNullType(
        TRuntimeNode value,
        NUdf::TDataTypeId expectedType) const;
    TInternName NullInternName;
    TTupleType* TableIdType;
};

} // namespace NMiniKQL
} // namespace NKikimr

