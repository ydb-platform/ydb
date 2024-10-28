#include "kikimr_program_builder.h"

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/library/yql/minikql/mkql_opt_literal.h>

#include <array>

namespace NKikimr {
namespace NMiniKQL {
namespace {

TType* ValidateColumns(
        const TArrayRef<const TSelectColumn>& columns,
        TRuntimeNode& tags,
        const TProgramBuilder* builder)
{
    TStructTypeBuilder rowTypeBuilder(builder->GetTypeEnvironment());
    TStructLiteralBuilder tagsBuilder(builder->GetTypeEnvironment());
    rowTypeBuilder.Reserve(columns.size());
    tagsBuilder.Reserve(columns.size());
    for (auto& col : columns) {
        // TODO: support pg types
        MKQL_ENSURE(col.SchemeType.GetTypeId() != 0, "Null type is not allowed");
        MKQL_ENSURE(col.SchemeType.GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");
        TType* dataType;
        if (col.SchemeType.GetTypeId() == NYql::NProto::TypeIds::Decimal)
            dataType = TDataDecimalType::Create(
                col.SchemeType.GetDecimalType().GetPrecision(),
                col.SchemeType.GetDecimalType().GetScale(),
                builder->GetTypeEnvironment());
        else
            dataType = TDataType::Create(col.SchemeType.GetTypeId(), builder->GetTypeEnvironment());

        if (col.TypeConstraint == EColumnTypeConstraint::Nullable) {
            dataType = TOptionalType::Create(dataType, builder->GetTypeEnvironment());
        }

        rowTypeBuilder.Add(col.Label, dataType);
        tagsBuilder.Add(col.Label, builder->NewDataLiteral<ui32>(col.ColumnId));
    }

    tags = TRuntimeNode(tagsBuilder.Build(), true);
    return rowTypeBuilder.Build();
}

} // namespace

TReadRangeOptions::TReadRangeOptions(ui32 valueType, const TTypeEnvironment& env)
        : ItemsLimit(BuildDataLiteral(NUdf::TUnboxedValuePod((ui64)0), NUdf::EDataSlot::Uint64, env), true)
    , BytesLimit(BuildDataLiteral(NUdf::TUnboxedValuePod((ui64)0), NUdf::EDataSlot::Uint64, env), true)
    , InitValue(BuildEmptyOptionalDataLiteral(valueType, env), true)
    , TermValue(BuildEmptyOptionalDataLiteral(valueType, env), true)
    , PayloadStruct(env.GetEmptyStructLazy(), true)
    , Flags(BuildDataLiteral(NUdf::TUnboxedValuePod((ui32)TFlags::Default), NUdf::EDataSlot::Uint32, env), true)
{
}

TTableRangeOptions::TTableRangeOptions(const TTypeEnvironment& env)
        : ItemsLimit(BuildDataLiteral(NUdf::TUnboxedValuePod((ui64)0), NUdf::EDataSlot::Uint64, env), true)
    , BytesLimit(BuildDataLiteral(NUdf::TUnboxedValuePod((ui64)0), NUdf::EDataSlot::Uint64, env), true)
    , Flags(BuildDataLiteral(NUdf::TUnboxedValuePod((ui32)TReadRangeOptions::TFlags::Default), NUdf::EDataSlot::Uint32, env), true)
    , Reverse(BuildDataLiteral(NUdf::TUnboxedValuePod(false), NUdf::EDataSlot::Bool, env), true)
{
}

TParametersBuilder::TParametersBuilder(const TTypeEnvironment& env)
    : StructBuilder(env)
{
}

TParametersBuilder& TParametersBuilder::Add(const TStringBuf& name, TRuntimeNode value) {
    StructBuilder.Add(name, value);
    return *this;
}

TRuntimeNode TParametersBuilder::Build() {
    return TRuntimeNode(StructBuilder.Build(), true);
}

static TRuntimeNode DoRewriteNullType(
    TRuntimeNode value,
    NUdf::TDataTypeId expectedType,
    const TInternName& nullInternName,
    const TTypeEnvironment& env)
{
    if (!value.IsImmediate()) {
        auto& callable = static_cast<TCallable&>(*value.GetNode());
        auto callableName = callable.GetType()->GetNameStr();
        if (callableName == nullInternName) {
            return TRuntimeNode(BuildEmptyOptionalDataLiteral(expectedType, env), true);
        }
    }
    return value;
}

TUpdateRowBuilder::TUpdateRowBuilder(const TTypeEnvironment& env)
    : Builder(env)
    , Env(env)
{
    NullInternName = Env.InternName(TStringBuf("Null"));
}

void TUpdateRowBuilder::SetColumn(
        ui32 columnId, NScheme::TTypeInfo expectedType,
        TRuntimeNode value)
{
    // TODO: support pg types
    MKQL_ENSURE(expectedType.GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");

    bool isOptional;
    value = DoRewriteNullType(value, expectedType.GetTypeId(), NullInternName, Env);
    TDataType* dataType = UnpackOptionalData(value, isOptional);

    MKQL_ENSURE(expectedType.GetTypeId() == dataType->GetSchemeType(),
        "Mismatch of column type expectedType = " << expectedType.GetTypeId()
        << " actual type = " << dataType->GetSchemeType());
    Builder.Add(ToString(columnId), value);
}

void TUpdateRowBuilder::InplaceUpdateColumn(
        ui32 columnId, NScheme::TTypeInfo expectedType,
        TRuntimeNode value,
        EInplaceUpdateMode mode)
{
    // TODO: support pg types
    MKQL_ENSURE(expectedType.GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");
    MKQL_ENSURE(AS_TYPE(TDataType, value)->GetSchemeType() == expectedType.GetTypeId(),
            "Mismatch of column type");
    TDataType* byteType = TDataType::Create(NUdf::TDataType<ui8>::Id, Env);
    std::array<TType*, 2> elements;
    elements[0] = byteType;
    elements[1] = value.GetStaticType();
    TTupleType* tupleType = TTupleType::Create(elements.size(), elements.data(), Env);
    std::array<TRuntimeNode, 2> items;
    items[0] = TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod((ui8)mode), NUdf::TDataType<ui8>::Id, Env), true);
    items[1] = value;
    TTupleLiteral* tuple = TTupleLiteral::Create(
            items.size(), items.data(), tupleType, Env);
    Builder.Add(ToString(columnId), TRuntimeNode(tuple, true));
}

void TUpdateRowBuilder::EraseColumn(ui32 columnId)
{
    Builder.Add(ToString(columnId), TRuntimeNode(Env.GetVoidLazy(), true));
}

TRuntimeNode TUpdateRowBuilder::Build()
{
    return TRuntimeNode(Builder.Build(), true);
}

const char* InplaceUpdateModeToCString(EInplaceUpdateMode mode) {
    switch (mode) {
        case EInplaceUpdateMode::Sum:
            return "Sum";
        case EInplaceUpdateMode::Min:
            return "Min";
        case EInplaceUpdateMode::Max:
            return "Max";
        case EInplaceUpdateMode::IfNotExistOrEmpty:
            return "IfNotExistOrEmpty";
        case EInplaceUpdateMode::Unknown:
        case EInplaceUpdateMode::LastMode:
        default:
            return "Unknown";
    }
}

EInplaceUpdateMode InplaceUpdateModeFromCString(const char* str) {
    if (strcmp(str, "Sum") == 0) {
        return EInplaceUpdateMode::Sum;
    } else if (strcmp(str, "Min") == 0) {
        return EInplaceUpdateMode::Min;
    } else if (strcmp(str, "Max") == 0) {
        return EInplaceUpdateMode::Max;
    } else if (strcmp(str, "IfNotExistOrEmpty") == 0) {
        return EInplaceUpdateMode::IfNotExistOrEmpty;
    } else {
        return EInplaceUpdateMode::Unknown;
    }
}

TKikimrProgramBuilder::TKikimrProgramBuilder(
        const TTypeEnvironment& env,
        const IFunctionRegistry& functionRegistry)
    : TProgramBuilder(env, functionRegistry, true)
{
    UseNullType = false;
    NullInternName = Env.InternName(TStringBuf("Null"));

    std::array<TType*, 3> tupleTypes;
    tupleTypes[0] = NewDataType(NUdf::TDataType<ui64>::Id);
    tupleTypes[1] = NewDataType(NUdf::TDataType<ui64>::Id);
    tupleTypes[2] = NewDataType(NUdf::TDataType<ui64>::Id);
    TableIdType = TTupleType::Create(tupleTypes.size(), tupleTypes.data(), Env);
}

TRuntimeNode TKikimrProgramBuilder::RewriteNullType(
        TRuntimeNode value,
        NUdf::TDataTypeId expectedType) const
{
    return DoRewriteNullType(value, expectedType, NullInternName, Env);
}

TVector<TRuntimeNode> TKikimrProgramBuilder::FixKeysType(
        const TArrayRef<NScheme::TTypeInfo>& keyTypes,
        const TKeyColumnValues& row) const
{
    MKQL_ENSURE(!row.empty(), "Expected at least 1 key column");
    MKQL_ENSURE(keyTypes.size() == row.size(), "Mismatch of key types count");
    TVector<TRuntimeNode> tmp(row.size());
    for (ui32 i = 0; i < row.size(); ++i) {
        // TODO: support pg types
        MKQL_ENSURE(keyTypes[i].GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");
        tmp[i] = RewriteNullType(row[i], keyTypes[i].GetTypeId());
        bool isOptional;
        TDataType* dataType = UnpackOptionalData(tmp[i], isOptional);
        MKQL_ENSURE(keyTypes[i].GetTypeId() == dataType->GetSchemeType(),
            "Mismatch of key column type, expected: "
            << keyTypes[i].GetTypeId() << ", but got: " << dataType->GetSchemeType());
    }
    return tmp;
}

TRuntimeNode TKikimrProgramBuilder::ReadTarget(const TReadTarget& target) {
    MKQL_ENSURE(!target.HasSnapshotTime(), "Snapshots are not supported");
    return NewDataLiteral((ui32)target.GetMode());
}

TRuntimeNode TKikimrProgramBuilder::SelectRow(
        const TTableId& tableId,
        const TArrayRef<NScheme::TTypeInfo>& keyTypes,
        const TArrayRef<const TSelectColumn>& columns,
        const TKeyColumnValues& row, const TReadTarget& target)
{
    return SelectRow(tableId, keyTypes, columns, row, ReadTarget(target));
}

TRuntimeNode TKikimrProgramBuilder::SelectRow(
        const TTableId& tableId,
        const TArrayRef<NScheme::TTypeInfo>& keyTypes,
        const TArrayRef<const TSelectColumn>& columns,
        const TKeyColumnValues& row, TRuntimeNode readTarget)
{
    MKQL_ENSURE(AS_TYPE(TDataType, readTarget)->GetSchemeType() == NUdf::TDataType<ui32>::Id,
        "Read target must be ui32");

    auto rows = FixKeysType(keyTypes, row);

    TRuntimeNode tags;
    auto rowType = ValidateColumns(columns, tags, this);
    TType* optType = NewOptionalType(rowType);

    TCallableBuilder builder(Env, "SelectRow", optType);

    builder.Add(BuildTableId(tableId));
    builder.Add(TRuntimeNode(rowType, true));
    builder.Add(tags);
    builder.Add(NewTuple(TKeyColumnValues(rows)));
    builder.Add(readTarget);

    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::SelectRange(
        const TTableId& tableId,
        const TArrayRef<NScheme::TTypeInfo>& keyTypes,
        const TArrayRef<const TSelectColumn>& columns,
        const TTableRangeOptions& options,
        const TReadTarget& target)
{
    return SelectRange(tableId, keyTypes, columns, options, ReadTarget(target));
}

static TRuntimeNode BuildOptionList(const TArrayRef<bool>& arr, TProgramBuilder& pBuilder) {
    TListLiteralBuilder builder(pBuilder.GetTypeEnvironment(), pBuilder.NewDataType(NUdf::TDataType<bool>::Id));
    for (auto& item : arr) {
        builder.Add(pBuilder.NewDataLiteral(item));
    }
    return TRuntimeNode(builder.Build(), true);
}

static bool AtLeastOneFlagSet(const TArrayRef<bool>& arr) {
    for (bool flag : arr) {
        if (flag)
            return true;
    }
    return false;
}

TRuntimeNode TKikimrProgramBuilder::SelectRange(
        const TTableId& tableId,
        const TArrayRef<NScheme::TTypeInfo>& keyTypes,
        const TArrayRef<const TSelectColumn>& columns,
        const TTableRangeOptions& options,
        TRuntimeNode readTarget)
{
    MKQL_ENSURE(AS_TYPE(TDataType, readTarget)->GetSchemeType() == NUdf::TDataType<ui32>::Id, "Read target must be ui32");
    MKQL_ENSURE(AS_TYPE(TDataType, options.Flags)->GetSchemeType() == NUdf::TDataType<ui32>::Id, "Flags must be ui32");
    MKQL_ENSURE(AS_TYPE(TDataType, options.ItemsLimit)->GetSchemeType() == NUdf::TDataType<ui64>::Id, "ItemsLimit must be ui64");
    MKQL_ENSURE(AS_TYPE(TDataType, options.BytesLimit)->GetSchemeType() == NUdf::TDataType<ui64>::Id, "BytesLimit must be ui64");

    if (options.Reverse) {
        MKQL_ENSURE(AS_TYPE(TDataType, options.Reverse)->GetSchemeType() == NUdf::TDataType<bool>::Id, "Reverse must be bool");
    }

    MKQL_ENSURE(options.FromColumns.size() > 0, "Expected at least one key component in the 'from' section of the range");
    const ui32 maxKeyColumnsCount = ::Max<ui32>(options.FromColumns.size(), options.ToColumns.size());
    MKQL_ENSURE(keyTypes.size() == maxKeyColumnsCount, "Mismatch of key types count");
    TVector<TRuntimeNode> from(options.FromColumns.size());
    TVector<TRuntimeNode> to(options.ToColumns.size());
    for (ui32 i = 0; i < maxKeyColumnsCount; ++i) {
        TDataType* dataFrom = nullptr;
        TDataType* dataTo = nullptr;
        if (i < options.FromColumns.size()) {
            MKQL_ENSURE(keyTypes[i].GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");
            from[i] = RewriteNullType(options.FromColumns[i], keyTypes[i].GetTypeId());

            bool isOptionalFrom;
            dataFrom = UnpackOptionalData(from[i], isOptionalFrom);
        }

        if (i < options.ToColumns.size()) {
            MKQL_ENSURE(keyTypes[i].GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");
            to[i] = RewriteNullType(options.ToColumns[i], keyTypes[i].GetTypeId());

            bool isOptionalTo;
            dataTo = UnpackOptionalData(to[i], isOptionalTo);
        }

        MKQL_ENSURE(dataFrom || dataTo, "Invalid key tuple values");

        if (dataFrom && dataTo) {
            MKQL_ENSURE(dataFrom->IsSameType(*dataTo), "Data types for key column " << i
                << " don't match From: " << NUdf::GetDataTypeInfo(*dataFrom->GetDataSlot()).Name
                << " To: " << NUdf::GetDataTypeInfo(*dataTo->GetDataSlot()).Name);
        }

        ui32 dataType = dataFrom ? dataFrom->GetSchemeType() : dataTo->GetSchemeType();
        MKQL_ENSURE(keyTypes[i].GetTypeId() == dataType, "Mismatch of key column " << i
            << " type, expected: " << NUdf::GetDataTypeInfo(NUdf::GetDataSlot(keyTypes[i].GetTypeId())).Name
            << ", but got: " << NUdf::GetDataTypeInfo(NUdf::GetDataSlot(dataType)).Name);
    }

    TRuntimeNode tags;
    auto rowType = ValidateColumns(columns, tags, this);
    TType* listType = NewListType(rowType);
    TDataType* boolType = TDataType::Create(NUdf::TDataType<bool>::Id, Env);

    TRuntimeNode skipNullKeys = BuildOptionList(options.SkipNullKeys, *this);
    TRuntimeNode forbidNullArgsFrom = BuildOptionList(options.ForbidNullArgsFrom, *this);
    TRuntimeNode forbidNullArgsTo = BuildOptionList(options.ForbidNullArgsTo, *this);

    TStructTypeBuilder returnTypeBuilder(Env);
    returnTypeBuilder.Reserve(2);
    returnTypeBuilder.Add(TStringBuf("List"), listType);
    returnTypeBuilder.Add(TStringBuf("Truncated"), boolType);
    auto returnType = returnTypeBuilder.Build();
    TCallableBuilder builder(Env, "SelectRange", returnType);

    builder.Add(BuildTableId(tableId));
    builder.Add(TRuntimeNode(rowType, true));
    builder.Add(tags);
    builder.Add(NewTuple(TKeyColumnValues(from)));
    builder.Add(NewTuple(TKeyColumnValues(to)));
    builder.Add(options.Flags);
    builder.Add(options.ItemsLimit);
    builder.Add(options.BytesLimit);
    builder.Add(readTarget);
    builder.Add(skipNullKeys);
    builder.Add(options.Reverse);

    // Compat with older kikimr
    // The following 'if' should be removed when all deployed versions have support for 13 parameters.
    if (AtLeastOneFlagSet(options.ForbidNullArgsFrom) ||
        AtLeastOneFlagSet(options.ForbidNullArgsTo)) {
        builder.Add(forbidNullArgsFrom);
        builder.Add(forbidNullArgsTo);
    }

    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::UpdateRow(
        const TTableId& tableId,
        const TArrayRef<NScheme::TTypeInfo>& keyTypes,
        const TKeyColumnValues& row,
        TUpdateRowBuilder& update)
{
    auto rows = FixKeysType(keyTypes, row);

    TCallableBuilder builder(Env, "UpdateRow", Env.GetTypeOfVoidLazy());

    builder.Add(BuildTableId(tableId));
    builder.Add(NewTuple(TKeyColumnValues(rows)));
    builder.Add(update.Build());
    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::EraseRow(
        const TTableId& tableId,
        const TArrayRef<NScheme::TTypeInfo>& keyTypes,
        const TKeyColumnValues& row)
{
    auto rows = FixKeysType(keyTypes, row);

    TCallableBuilder builder(Env, "EraseRow", Env.GetTypeOfVoidLazy());

    builder.Add(BuildTableId(tableId));
    builder.Add(NewTuple(TKeyColumnValues(rows)));
    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::Prepare(TRuntimeNode listOfVoid)
{
    auto listType = listOfVoid.GetStaticType();
    AS_TYPE(TListType, listType);

    const auto& listDetailedType = static_cast<const TListType&>(*listType);
    MKQL_ENSURE(listDetailedType.GetItemType()->IsVoid(), "Expected list of void");

    return listOfVoid;
}

TRuntimeNode TKikimrProgramBuilder::Parameter(
        const TStringBuf& name, TType* type)
{
    MKQL_ENSURE(!name.empty(), "Empty parameter name is not allowed");

    TCallableBuilder callableBuilder(Env, "Parameter", type, true);
    callableBuilder.SetTypePayload(name);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::MapParameter(
        TRuntimeNode list,
        std::function<TRuntimeNode(TRuntimeNode item)> handler)
{
    MKQL_ENSURE(!list.IsImmediate() && list.GetNode()->GetType()->IsCallable(), "Expected Parameter callable");
    auto callable = static_cast<TCallable*>(list.GetNode());
    MKQL_ENSURE(callable->GetType()->GetName() == "Parameter", "Expected Parameter callable");

    auto listType = list.GetStaticType();
    AS_TYPE(TListType, listType);

    const auto& listDetailedType = static_cast<const TListType&>(*listType);
    auto itemType = listDetailedType.GetItemType();
    ThrowIfListOfVoid(itemType);

    TRuntimeNode itemArg = Arg(itemType);
    auto newItem = handler(itemArg);

    auto resultListType = TListType::Create(newItem.GetStaticType(), Env);
    TCallableBuilder callableBuilder(Env, "MapParameter", resultListType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(newItem);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::FlatMapParameter(
        TRuntimeNode list,
        std::function<TRuntimeNode(TRuntimeNode item)> handler)
{
    MKQL_ENSURE(!list.IsImmediate() && list.GetNode()->GetType()->IsCallable(), "Expected Parameter callable");
    auto callable = static_cast<TCallable*>(list.GetNode());
    MKQL_ENSURE(callable->GetType()->GetName() == "Parameter", "Expected Parameter callable");

    auto listType = list.GetStaticType();
    AS_TYPE(TListType, listType);

    const auto& listDetailedType = static_cast<const TListType&>(*listType);
    auto itemType = listDetailedType.GetItemType();
    ThrowIfListOfVoid(itemType);

    TRuntimeNode itemArg = Arg(itemType);
    auto newItem = handler(itemArg);
    auto outputListType = AS_TYPE(TListType, newItem);

    TCallableBuilder callableBuilder(Env, "FlatMapParameter", outputListType);
    callableBuilder.Add(list);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(newItem);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::AcquireLocks(TRuntimeNode lockTxId) {
    MKQL_ENSURE(AS_TYPE(TDataType, lockTxId)->GetSchemeType() == NUdf::TDataType<ui64>::Id, "LockTxId must be ui64");

    TCallableBuilder callableBuilder(Env, "AcquireLocks", Env.GetTypeOfVoidLazy());
    callableBuilder.Add(lockTxId);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::CombineByKeyMerge(TRuntimeNode list) {
    auto listType = list.GetStaticType();
    MKQL_ENSURE(listType->IsList(), "Expected list");

    TCallableBuilder callableBuilder(Env, "CombineByKeyMerge", listType);
    callableBuilder.Add(list);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::Diagnostics() {
    TCallableBuilder callableBuilder(Env, "Diagnostics", Env.GetTypeOfVoidLazy());
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::PartialSort(TRuntimeNode list, TRuntimeNode ascending,
    std::function<TRuntimeNode(TRuntimeNode item)> keyExtractor)
{
    return BuildSort("PartialSort", list, ascending, keyExtractor);
}

TRuntimeNode TKikimrProgramBuilder::PartialTake(TRuntimeNode list, TRuntimeNode count) {
    return BuildTake("PartialTake", list, count);
}

TRuntimeNode TKikimrProgramBuilder::Bind(TRuntimeNode program, TRuntimeNode parameters, ui32 bindFlags) {
    auto listType = program.GetStaticType();
    AS_TYPE(TListType, listType);

    const auto& listDetailedType = static_cast<const TListType&>(*listType);
    MKQL_ENSURE(listDetailedType.GetItemType()->IsVoid(), "Expected list of void");

    MKQL_ENSURE(parameters.IsImmediate() && parameters.GetStaticType()->IsStruct(), "Expected struct");

    auto& parametersStruct = static_cast<TStructLiteral&>(*parameters.GetNode());

    {
        TExploringNodeVisitor explorer;
        explorer.Walk(program.GetNode(), Env);
        auto parameterFunc = Env.InternName(TStringBuf("Parameter"));
        auto mapParameterFunc = Env.InternName(TStringBuf("MapParameter"));
        auto flatMapParameterFunc = Env.InternName(TStringBuf("FlatMapParameter"));
        auto arg = Env.InternName(TStringBuf("Arg"));
        for (auto node : explorer.GetNodes()) {
            if (node->GetType()->GetKind() != TType::EKind::Callable)
                continue;

            auto& callable = static_cast<TCallable&>(*node);
            if (callable.HasResult())
                continue;

            auto callableName = callable.GetType()->GetNameStr();
            if (callableName == parameterFunc) {
                MKQL_ENSURE(callable.GetType()->GetPayload(), "Expected payload");
                auto structObj = AS_VALUE(NMiniKQL::TStructLiteral, NMiniKQL::TRuntimeNode(callable.GetType()->GetPayload(), true));
                auto payloadIndex = 1; // fields: Args, Payload
                const TStringBuf parameterName(AS_VALUE(NMiniKQL::TDataLiteral, structObj->GetValue(payloadIndex))->AsValue().AsStringRef());

                auto parameterIndex = parametersStruct.GetType()->FindMemberIndex(parameterName);
                MKQL_ENSURE(parameterIndex, "Missing value for parameter: " << parameterName);

                auto value = parametersStruct.GetValue(*parameterIndex);
                MKQL_ENSURE(value.IsImmediate(), "Parameter value must be immediate");

                MKQL_ENSURE(value.GetStaticType()->IsSameType(*callable.GetType()->GetReturnType()),
                    "Incorrect type for parameter " << parameterName
                    << ", expected: " << PrintNode(callable.GetType()->GetReturnType(), true)
                    << ", actual: " << PrintNode(value.GetStaticType(), true));

                callable.SetResult(value, Env);
            } else if (callableName == mapParameterFunc || callableName == flatMapParameterFunc) {
                MKQL_ENSURE(callable.GetInput(0).HasValue(), "Expected parameter value");
                auto value = callable.GetInput(0).GetValue();
                auto list = AS_VALUE(TListLiteral, TRuntimeNode(value, true));

                // lambda item type
                auto retItemType = callable.GetInput(2).GetStaticType();
                if (callableName == flatMapParameterFunc) {
                    retItemType = AS_TYPE(TListType, retItemType)->GetItemType();
                }

                TVector<TRuntimeNode> mapResults;
                mapResults.reserve(list->GetItemsCount());

                TExploringNodeVisitor explorer;
                auto lambdaArgNode = callable.GetInput(1).GetNode();
                auto lambdaRootNode = callable.GetInput(2);
                explorer.Walk(lambdaRootNode.GetNode(), Env);
                for (ui32 i = 0; i < list->GetItemsCount(); ++i) {
                    auto itemValue = list->GetItems()[i];
                    bool wereChanges;
                    auto newValue = SinglePassVisitCallables(lambdaRootNode, explorer,
                        [&](TInternName name) {
                            Y_UNUSED(name);
                            return [&](TCallable& callable, const TTypeEnvironment& env) {
                                Y_UNUSED(env);
                                if (&callable == lambdaArgNode) {
                                    return itemValue;
                                }

                                if (callable.GetType()->GetNameStr() == arg) {
                                    TCallableBuilder itemCallableBuilder(Env, arg.Str(), callable.GetType()->GetReturnType(), true);
                                    TRuntimeNode itemArg(itemCallableBuilder.Build(), false);
                                    return itemArg;
                                }

                                return TRuntimeNode(&callable, false);
                            };
                        }, Env, false, wereChanges);

                    mapResults.push_back(newValue);
                }

                TRuntimeNode res;
                TListLiteralBuilder listBuilder(Env, retItemType);
                if (callableName == mapParameterFunc) {
                    for (auto& node : mapResults) {
                        listBuilder.Add(node);
                    }
                    res = TRuntimeNode(listBuilder.Build(), true);
                } else {
                    res = mapResults.empty()
                        ? TRuntimeNode(listBuilder.Build(), true)
                        : Extend(mapResults);
                }

                callable.SetResult(res, Env);
            }
        }

        for (auto node : explorer.GetNodes()) {
            node->Freeze(Env);
        }
    }

    program.Freeze();
    if (bindFlags & TBindFlags::OptimizeLiterals) {
        program = LiteralPropagationOptimization(program, Env, true);
    }

    return program;
}

TRuntimeNode TKikimrProgramBuilder::Build(TRuntimeNode listOfVoid, ui32 bindFlags)
{
    auto program = Prepare(listOfVoid);
    TParametersBuilder parametersBuilder(Env);
    return Bind(program, parametersBuilder.Build(), bindFlags);
}

TRuntimeNode TKikimrProgramBuilder::Abort() {
    TCallableBuilder callableBuilder(Env, "Abort", Env.GetVoidLazy()->GetGenericType());
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::StepTxId() {
    std::array<TType*, 2> tupleTypes;
    tupleTypes[0] = NewDataType(NUdf::TDataType<ui64>::Id);
    tupleTypes[1] = NewDataType(NUdf::TDataType<ui64>::Id);
    auto returnType = NewTupleType(tupleTypes);

    TCallableBuilder callableBuilder(Env, "StepTxId", returnType);
    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::SetResult(const TStringBuf& label, TRuntimeNode payload) {
    MKQL_ENSURE(!label.empty(), "label must not be empty");
    MKQL_ENSURE(CanExportType(payload.GetStaticType(), Env),
        TStringBuilder() << "Failed to export type:" << *payload.GetStaticType());
    TCallableBuilder builder(Env, "SetResult", Env.GetTypeOfVoidLazy());
    builder.Add(TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::String>(label));
    builder.Add(payload);
    return TRuntimeNode(builder.Build(), false);
}

TRuntimeNode TKikimrProgramBuilder::NewDataLiteral(const std::pair<ui64, ui64>& data) const {
    return TRuntimeNode(BuildDataLiteral(NUdf::TStringRef(reinterpret_cast<const char*>(&data), sizeof(data)), LegacyPairUi64Ui64, Env), true);
}

TRuntimeNode TKikimrProgramBuilder::BuildTableId(const TTableId& tableId) const {
    if (tableId.SchemaVersion) {
        std::array<TRuntimeNode, 3> items;
        items[0] = TProgramBuilder::NewDataLiteral<ui64>(tableId.PathId.OwnerId);
        items[1] = TProgramBuilder::NewDataLiteral<ui64>(tableId.PathId.LocalPathId);
        items[2] = TProgramBuilder::NewDataLiteral<ui64>(tableId.SchemaVersion);
        return TRuntimeNode(TTupleLiteral::Create(items.size(), items.data(), TableIdType, Env), true);
    } else {
        // temporary compatibility (KIKIMR-8446)
        return NewDataLiteral({tableId.PathId.OwnerId, tableId.PathId.LocalPathId});
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
