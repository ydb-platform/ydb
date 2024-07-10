#include "mkql_keys.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/scheme_types/scheme_types_defs.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/codec.h>

#include <util/generic/maybe.h>
#include <util/generic/algorithm.h>
#include <functional>

namespace NKikimr {
namespace NMiniKQL {

namespace {

bool ExtractKeyData(TRuntimeNode valueNode, bool isOptional, NUdf::TUnboxedValue& data) {
    if (valueNode.HasValue()) {
        TNode* value = valueNode.GetValue();
        TNode* dataValue = value;
        if (isOptional) {
            MKQL_ENSURE(value->GetType()->IsOptional(), "Expected optional");
            auto opt = static_cast<TOptionalLiteral*>(value);
            if (!opt->HasItem()) {
                data = NUdf::TUnboxedValue();
                return true;
            }
            else {
                dataValue = opt->GetItem().GetNode();
            }
        }

        if (dataValue->GetType()->IsData()) {
            data = NUdf::TUnboxedValuePod(static_cast<TDataLiteral*>(dataValue)->AsValue());
            return true;
        }
    }

    return false;
}

NScheme::TTypeInfo UnpackTypeInfo(NKikimr::NMiniKQL::TType *type, bool &isOptional) {
    isOptional = false;
    if (type->GetKind() == TType::EKind::Pg) {
        auto pgType = static_cast<TPgType*>(type);
        auto pgTypeId = pgType->GetTypeId();
        return NScheme::TTypeInfo(NScheme::NTypeIds::Pg, NPg::TypeDescFromPgTypeId(pgTypeId));
    } else {
        auto dataType = UnpackOptionalData(type, isOptional);
        return NScheme::TTypeInfo(dataType->GetSchemeType());
    }
}

THolder<TKeyDesc> ExtractKeyTuple(const TTableId& tableId, TTupleLiteral* tuple,
    const TVector<TKeyDesc::TColumnOp>& columns,
    TKeyDesc::ERowOperation rowOperation, bool requireStaticKey, const TTypeEnvironment& env) {
    TVector<NScheme::TTypeInfo> keyColumnTypes(tuple->GetValuesCount());
    TVector<TCell> fromValues(tuple->GetValuesCount());
    TVector<TCell> toValues(tuple->GetValuesCount());
    bool inclusiveFrom = true;
    bool inclusiveTo = true;
    bool point = true;
    ui32 staticComponents = 0;
    for (ui32 i = 0; i < tuple->GetValuesCount(); ++i) {
        auto type = tuple->GetType()->GetElementType(i);
        bool isOptional;
        auto typeInfo = UnpackTypeInfo(type, isOptional);
        keyColumnTypes[i] = typeInfo;
        if (i != staticComponents) {
            continue;
        }

        auto valueNode = tuple->GetValue(i);
        NUdf::TUnboxedValue data;
        bool hasImmediateData = ExtractKeyData(valueNode, isOptional, data);
        if (!hasImmediateData) {
            MKQL_ENSURE(!requireStaticKey, "Expected static key components");
            point = false;
            continue;
        }

        ++staticComponents;
        fromValues[i] = toValues[i] = MakeCell(keyColumnTypes[i], data, env);
    }

    TTableRange range(TConstArrayRef<TCell>(fromValues.data(), tuple->GetValuesCount()),
        inclusiveFrom, TConstArrayRef<TCell>(toValues.data(), staticComponents), inclusiveTo, point);
    return MakeHolder<TKeyDesc>(tableId, range, rowOperation, keyColumnTypes, columns);
}

void ExtractReadColumns(TStructType* columnsType, TStructLiteral* tags, TVector<TKeyDesc::TColumnOp>& columns) {
    MKQL_ENSURE(columnsType->GetMembersCount() == tags->GetValuesCount(), "Mismatch count of tags");
    for (ui32 i = 0; i < columnsType->GetMembersCount(); ++i) {
        auto memberName = columnsType->GetMemberName(i);
        MKQL_ENSURE(tags->GetType()->GetMemberName(i) == memberName, "Mismatch name of column");
        ui32 columnId = AS_VALUE(TDataLiteral, tags->GetValue(i))->AsValue().Get<ui32>();
        TKeyDesc::TColumnOp& op = columns[i];
        op.Column = columnId;
        op.Operation = TKeyDesc::EColumnOperation::Read;
        bool isOptional;
        auto typeInfo = UnpackTypeInfo(columnsType->GetMemberType(i), isOptional);
        MKQL_ENSURE(typeInfo.GetTypeId() != 0, "Null type is not allowed");
        op.ExpectedType = typeInfo;
    }
}

THolder<TKeyDesc> ExtractSelectRow(TCallable& callable, const TTypeEnvironment& env) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");
    auto tableId = ExtractTableId(callable.GetInput(0));
    auto columnsNode = callable.GetInput(1);
    MKQL_ENSURE(columnsNode.IsImmediate() && columnsNode.GetNode()->GetType()->IsType(), "Expected struct type");
    auto columnsType = AS_TYPE(TStructType, static_cast<TType*>(columnsNode.GetNode()));
    auto tags = AS_VALUE(TStructLiteral, callable.GetInput(2));
    TVector<TKeyDesc::TColumnOp> columns(columnsType->GetMembersCount());
    ExtractReadColumns(columnsType, tags, columns);
    auto tuple = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    THolder<TKeyDesc> desc = ExtractKeyTuple(tableId, tuple, columns, TKeyDesc::ERowOperation::Read, true, env);
    desc->ReadTarget = ExtractFlatReadTarget(callable.GetInput(4));
    return desc;
}

THolder<TKeyDesc> ExtractSelectRange(TCallable& callable, const TTypeEnvironment& env) {
    MKQL_ENSURE(callable.GetInputsCount() >= 9 && callable.GetInputsCount() <= 13, "Expected 9 to 13 args");
    auto tableId = ExtractTableId(callable.GetInput(0));
    auto columnsNode = callable.GetInput(1);
    MKQL_ENSURE(columnsNode.IsImmediate() && columnsNode.GetNode()->GetType()->IsType(), "Expected struct type");
    auto columnsType = AS_TYPE(TStructType, static_cast<TType*>(columnsNode.GetNode()));
    TVector<TKeyDesc::TColumnOp> columns(columnsType->GetMembersCount());
    auto tags = AS_VALUE(TStructLiteral, callable.GetInput(2));
    ExtractReadColumns(columnsType, tags, columns);
    auto fromTuple = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    auto toTuple = AS_VALUE(TTupleLiteral, callable.GetInput(4));

    auto flagsInput = callable.GetInput(5);
    const ui32 flags = AS_VALUE(TDataLiteral, flagsInput)->AsValue().Get<ui32>();

    ui64 itemsLimit = AS_VALUE(TDataLiteral, callable.GetInput(6))->AsValue().Get<ui64>();
    ui64 bytesLimit = AS_VALUE(TDataLiteral, callable.GetInput(7))->AsValue().Get<ui64>();

    TVector<NScheme::TTypeInfo> keyColumnTypes(Max(fromTuple->GetValuesCount(), toTuple->GetValuesCount()));
    TVector<TCell> fromValues(keyColumnTypes.size()); // padded with NULLs
    TVector<TCell> toValues(toTuple->GetValuesCount());
    bool inclusiveFrom = !(flags & TReadRangeOptions::TFlags::ExcludeInitValue);
    bool inclusiveTo = !(flags & TReadRangeOptions::TFlags::ExcludeTermValue);
    bool point = false;
    for (ui32 i = 0; i < fromTuple->GetValuesCount(); ++i) {
        auto type = fromTuple->GetType()->GetElementType(i);
        bool isOptional;
        auto typeInfo = UnpackTypeInfo(type, isOptional);
        keyColumnTypes[i] = typeInfo;
        auto valueNode = fromTuple->GetValue(i);
        NUdf::TUnboxedValue data;
        bool hasImmediateData = ExtractKeyData(valueNode, isOptional, data);
        MKQL_ENSURE(hasImmediateData, "Expected static key components");
        fromValues[i] = MakeCell(keyColumnTypes[i], data, env);
    }

    for (ui32 i = 0; i < toTuple->GetValuesCount(); ++i) {
        auto type = toTuple->GetType()->GetElementType(i);
        bool isOptional;
        auto typeInfo = UnpackTypeInfo(type, isOptional);
        keyColumnTypes[i] = typeInfo;
        auto valueNode = toTuple->GetValue(i);
        NUdf::TUnboxedValue data;
        bool hasImmediateData = ExtractKeyData(valueNode, isOptional, data);
        MKQL_ENSURE(hasImmediateData, "Expected static key components");
        toValues[i] = MakeCell(keyColumnTypes[i], data, env);
    }

    bool reverse = false;
    if (callable.GetInputsCount() > 10) {
        reverse = AS_VALUE(TDataLiteral, callable.GetInput(10))->AsValue().Get<bool>();
    }

    TTableRange range(TConstArrayRef<TCell>(fromValues.data(), fromValues.size()),
        inclusiveFrom, TConstArrayRef<TCell>(toValues.data(), toValues.size()), inclusiveTo, point);
    THolder<TKeyDesc> desc(
        new TKeyDesc(tableId, range, TKeyDesc::ERowOperation::Read, keyColumnTypes, columns, itemsLimit, bytesLimit, reverse));
    desc->ReadTarget = ExtractFlatReadTarget(callable.GetInput(8));
    return desc;
}

THolder<TKeyDesc> ExtractUpdateRow(TCallable& callable, const TTypeEnvironment& env) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");
    auto tableId = ExtractTableId(callable.GetInput(0));
    auto updateNode = callable.GetInput(2);
    auto update = AS_VALUE(TStructLiteral, updateNode);
    TVector<TKeyDesc::TColumnOp> columns(update->GetValuesCount());
    for (ui32 i = 0; i < update->GetValuesCount(); ++i) {
        auto memberName = update->GetType()->GetMemberName(i);
        ui32 columnId = 0;
        MKQL_ENSURE(TryFromString<ui32>(memberName, columnId), "Member names must be a column id as a number");
        auto cmd = update->GetValue(i);
        TKeyDesc::TColumnOp& op = columns[i];
        op.Column = columnId;
        op.InplaceUpdateMode = 0;
        op.ImmediateUpdateSize = 0;
        if (cmd.GetStaticType()->IsVoid()) {
            // erase
            op.Operation = TKeyDesc::EColumnOperation::Set;
            op.ExpectedType = NScheme::TTypeInfo(0);
        } else if (cmd.GetStaticType()->IsTuple()) {
            // inplace update
            TTupleLiteral* tuple = AS_VALUE(TTupleLiteral, cmd);
            MKQL_ENSURE(tuple->GetValuesCount() == 2, "Expected pair");
            auto inplaceModeNode = tuple->GetValue(0);
            ui32 mode = AS_VALUE(TDataLiteral, inplaceModeNode)->AsValue().Get<ui8>();
            MKQL_ENSURE(mode >= (ui32)EInplaceUpdateMode::FirstMode && mode <= (ui32)EInplaceUpdateMode::LastMode,
                "Wrong inplace update mode");
            auto valueNode = tuple->GetValue(1);
            op.Operation = TKeyDesc::EColumnOperation::InplaceUpdate;
            bool isOptional;
            auto typeInfo = UnpackTypeInfo(valueNode.GetStaticType(), isOptional);
            op.ExpectedType = typeInfo;
            MKQL_ENSURE(!isOptional, "Expected data type for inplace update, not an optional");
            op.InplaceUpdateMode = mode;

            NUdf::TUnboxedValue data;
            if (ExtractKeyData(valueNode, isOptional, data)) {
                op.ImmediateUpdateSize = MakeCell(op.ExpectedType, data, env, false).Size();
            }
        }
        else {
             // update
            op.Operation = TKeyDesc::EColumnOperation::Set;
            bool isOptional;
            auto typeInfo = UnpackTypeInfo(cmd.GetStaticType(), isOptional);
            op.ExpectedType = typeInfo;
            MKQL_ENSURE(op.ExpectedType.GetTypeId() != 0, "Null type is not allowed");

            NUdf::TUnboxedValue data;
            if (ExtractKeyData(cmd, isOptional, data)) {
                op.ImmediateUpdateSize = MakeCell(op.ExpectedType, data, env, false).Size();
            }
        }
    }

    auto tuple = AS_VALUE(TTupleLiteral, callable.GetInput(1));
    return ExtractKeyTuple(tableId, tuple, columns, TKeyDesc::ERowOperation::Update, false, env);
}

THolder<TKeyDesc> ExtractEraseRow(TCallable& callable, const TTypeEnvironment& env) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    auto tableId = ExtractTableId(callable.GetInput(0));
    auto tuple = AS_VALUE(TTupleLiteral, callable.GetInput(1));
    THolder<TKeyDesc> desc;
    return ExtractKeyTuple(tableId, tuple, TVector<TKeyDesc::TColumnOp>(), TKeyDesc::ERowOperation::Erase, false, env);
}

}

#define MAKE_PRIMITIVE_TYPE_CELL(type, layout) \
    case NUdf::TDataType<type>::Id: return MakeCell<layout>(value);

TCell MakeCell(NScheme::TTypeInfo type, const NUdf::TUnboxedValuePod& value,
    const TTypeEnvironment& env, bool copy,
    i32 typmod, TMaybe<TString>* error)
{
    if (!value)
        return TCell();

    switch(type.GetTypeId()) {
        KNOWN_FIXED_VALUE_TYPES(MAKE_PRIMITIVE_TYPE_CELL)
    case NUdf::TDataType<NUdf::TDecimal>::Id:
        {
            auto intVal = value.GetInt128();
            const auto& val = env.NewString(sizeof(intVal));
            std::memcpy(val.Data(), reinterpret_cast<const char*>(&intVal), sizeof(intVal));
            return TCell(val.Data(), val.Size());
        }
        break;
    }

    TString binary;
    NYql::NUdf::TStringRef ref;
    bool isPg = (type.GetTypeId() == NScheme::NTypeIds::Pg);
    if (isPg) {
        auto typeDesc = type.GetTypeDesc();
        if (typmod != -1 && NPg::TypeDescNeedsCoercion(typeDesc)) {
            TMaybe<TString> err;
            binary = NYql::NCommon::PgValueCoerce(value, NPg::PgTypeIdFromTypeDesc(typeDesc), typmod, &err);
            if (err) {
                if (error) {
                    *error = err;
                }
                return TCell();
            }
        } else {
            binary = NYql::NCommon::PgValueToNativeBinary(value, NPg::PgTypeIdFromTypeDesc(typeDesc));
        }
        ref = NYql::NUdf::TStringRef(binary);
    } else {
        ref = value.AsStringRef();
    }
    if (!isPg && !copy && !value.IsEmbedded() && (value.IsString() || TCell::CanInline(ref.Size()))) {
        return TCell(ref.Data(), ref.Size());
    }

    const auto& val = env.NewString(ref.Size());
    std::memcpy(val.Data(), ref.Data(), ref.Size());
    return TCell(val.Data(), val.Size());
}

#undef MAKE_PRIMITIVE_TYPE_CELL

TReadTarget ExtractFlatReadTarget(TRuntimeNode modeInput) {
    const ui32 mode = static_cast<TDataLiteral&>(*modeInput.GetValue()).AsValue().Get<ui32>();
    switch ((TReadTarget::EMode)mode) {
    case TReadTarget::EMode::Online:
        return TReadTarget::Online();
    case TReadTarget::EMode::Head:
        return TReadTarget::Head();
    case TReadTarget::EMode::Follower:
        return TReadTarget::Follower();
    default:
        THROW TWithBackTrace<yexception>() << "Bad read target mode";
    }
}

THolder<TKeyDesc> ExtractTableKey(TCallable& callable, const TTableStrings& strings, const TTypeEnvironment& env) {
    auto name = callable.GetType()->GetNameStr();
    if (name == strings.SelectRow) {
        return ExtractSelectRow(callable, env);
    }
    else if (name == strings.SelectRange) {
        return ExtractSelectRange(callable, env);
    }
    else if (name == strings.UpdateRow) {
        return ExtractUpdateRow(callable, env);
    }
    else if (name == strings.EraseRow) {
        return ExtractEraseRow(callable, env);
    }

    return nullptr;
}

TVector<THolder<TKeyDesc>> ExtractTableKeys(TExploringNodeVisitor& explorer, const TTypeEnvironment& env) {
    TVector<THolder<TKeyDesc>> descList;
    TTableStrings strings(env);
    for (auto node : explorer.GetNodes()) {
        if (node->GetType()->GetKind() != TType::EKind::Callable)
            continue;

        TCallable& callable = static_cast<TCallable&>(*node);
        THolder<TKeyDesc> desc = ExtractTableKey(callable, strings, env);
        if (desc) {
            descList.emplace_back(std::move(desc));
        }
    }
    return descList;
}

TTableId ExtractTableId(const TRuntimeNode& node) {
    if (node.GetStaticType()->IsTuple()) {
        const TTupleLiteral* tupleNode = AS_VALUE(TTupleLiteral, node);
        MKQL_ENSURE(tupleNode->GetValuesCount() >= 3u, "Unexpected number of tuple items");
        const ui64 ownerId = AS_VALUE(TDataLiteral, tupleNode->GetValue(0))->AsValue().Get<ui64>();
        const ui64 localPathId = AS_VALUE(TDataLiteral, tupleNode->GetValue(1))->AsValue().Get<ui64>();
        const ui64 schemaVersion = AS_VALUE(TDataLiteral, tupleNode->GetValue(2))->AsValue().Get<ui64>();
        return TTableId(ownerId, localPathId, schemaVersion);
    } else {
        const TStringBuf buffer(AS_VALUE(TDataLiteral, node)->AsValue().AsStringRef());
        using TPair = std::pair<ui64, ui64>;
        MKQL_ENSURE(sizeof(TPair) == buffer.size(), "Wrong table id buffer size.");
        const auto pair = reinterpret_cast<const TPair*>(buffer.data());
        return TTableId(pair->first, pair->second);
    }
}

void FillKeyTupleValue(const NUdf::TUnboxedValue& row, const TVector<ui32>& rowIndices,
    const TVector<NScheme::TTypeInfo>& rowTypes, TVector<TCell>& cells, const TTypeEnvironment& env)
{
    for (ui32 i = 0; i < rowIndices.size(); ++i) {
        auto rowIndex = rowIndices[i];

        auto value = row.GetElement(rowIndex);
        auto type = rowTypes[rowIndex];

        // NOTE: We have to copy values here as some values inlined in TUnboxedValue
        // cannot be be inlined in TCell. So set copy = true.
        cells[i] = MakeCell(type, value, env, /* copy */true);
    }
}

}
}
