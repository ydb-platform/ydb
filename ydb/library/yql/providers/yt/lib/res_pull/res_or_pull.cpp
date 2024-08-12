#include "res_or_pull.h"

#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <util/stream/holder.h>
#include <util/stream/str.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

//////////////////////////////////////////////////////////////////////////////////////////////////////////

TYsonExecuteResOrPull::TYsonExecuteResOrPull(TMaybe<ui64> rowLimit, TMaybe<ui64> byteLimit, const TMaybe<TVector<TString>>& columns)
    : IExecuteResOrPull(rowLimit, byteLimit, columns)
    , Writer(new NYson::TYsonWriter(Out.Get(), NYson::EYsonFormat::Binary, ::NYson::EYsonType::Node, true))
{
}

TString TYsonExecuteResOrPull::Finish() {
    if (IsList) {
        Writer->OnEndList();
    }
    Writer.Destroy();
    Out.Destroy();
    return Result;
}

bool TYsonExecuteResOrPull::WriteNext(const NYT::TNode& item) {
    if (IsList) {
        if (!HasCapacity()) {
            Truncated = true;
            return false;
        }
        Writer->OnListItem();
    }

    Writer->OnRaw(NYT::NodeToYsonString(item, NYT::NYson::EYsonFormat::Binary), ::NYson::EYsonType::Node);
    ++Row;

    return IsList;
}

void TYsonExecuteResOrPull::WriteValue(const NUdf::TUnboxedValue& value, TType* type) {
    if (type->IsList()) {
        auto inputType = AS_TYPE(TListType, type)->GetItemType();
        TMaybe<TVector<ui32>> structPositions = NCommon::CreateStructPositions(inputType, Columns.Defined() ? Columns.Get() : nullptr);
        SetListResult();
        const auto it = value.GetListIterator();
        for (NUdf::TUnboxedValue item; it.Next(item); ++Row) {
            if (!HasCapacity()) {
                Truncated = true;
                break;
            }

            Writer->OnListItem();
            NCommon::WriteYsonValue(*Writer, item, inputType, structPositions.Get());
        }
    } else {
        NCommon::WriteYsonValue(*Writer, value, type, nullptr);
    }
}

bool TYsonExecuteResOrPull::WriteNext(TMkqlIOCache& specsCache, const NYT::TNode& rec, ui32 tableIndex) {
    if (!HasCapacity()) {
        Truncated = true;
        return false;
    }

    NYql::DecodeToYson(specsCache, tableIndex, rec, *Out);
    ++Row;
    return true;
}

bool TYsonExecuteResOrPull::WriteNext(TMkqlIOCache& specsCache, const NYT::TYaMRRow& rec, ui32 tableIndex) {
    if (!HasCapacity()) {
        Truncated = true;
        return false;
    }

    NYql::DecodeToYson(specsCache, tableIndex, rec, *Out);
    ++Row;
    return true;
}

bool TYsonExecuteResOrPull::WriteNext(TMkqlIOCache& specsCache, const NUdf::TUnboxedValue& rec, ui32 tableIndex) {
    if (!HasCapacity()) {
        Truncated = true;
        return false;
    }

    NYql::DecodeToYson(specsCache, tableIndex, rec, *Out);
    ++Row;
    return true;
}

void TYsonExecuteResOrPull::SetListResult() {
    if (!IsList) {
        IsList = true;
        Writer->OnBeginList();
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

TSkiffExecuteResOrPull::TSkiffExecuteResOrPull(TMaybe<ui64> rowLimit, TMaybe<ui64> byteLimit, NCommon::TCodecContext& codecCtx, const NKikimr::NMiniKQL::THolderFactory& holderFactory, const NYT::TNode& attrs, const TString& optLLVM, const TVector<TString>& columns)
    : IExecuteResOrPull(rowLimit, byteLimit, columns)
    , HolderFactory(holderFactory)
    , SkiffWriter(*Out.Get(), 0, 4_MB)
{
    Specs.SetUseSkiff(optLLVM);
    Specs.Init(codecCtx, attrs);
    YQL_ENSURE(Specs.Outputs.size() == 1);

    SkiffWriter.SetSpecs(Specs);

    AlphabeticPermutation = CreateAlphabeticPositions(Specs.Outputs[0].RowType, columns);
}

TString TSkiffExecuteResOrPull::Finish() {
    SkiffWriter.Finish();
    Out.Destroy();
    Specs.Clear();
    return Result;
}

bool TSkiffExecuteResOrPull::WriteNext(const NYT::TNode& item) {
    if (IsList) {
        if (!HasCapacity()) {
            Truncated = true;
            return false;
        }
    }

    // For now this method is used only for the nodes that are lists of columns
    // Feel free to extend it for other types (maps, for example) but make sure
    // that the order of columns is correct.
    YQL_ENSURE(item.GetType() == NYT::TNode::EType::List, "Expected list node");
    const auto& listNode = item.UncheckedAsList();

    const auto& permutation = *AlphabeticPermutation;
    YQL_ENSURE(permutation.size() == listNode.size(), "Expected the same number of columns and values");

    // TODO: Node is being copied here. This can be avoided by doing in-place swaps
    // but it requires changing the signature of function to pass mutable node here.
    // Note, that it can be implemented without actual change of the node by
    // applying inverse permutation after the shuffle.
    auto alphabeticItem = NYT::TNode::CreateList();
    auto& alphabeticList = alphabeticItem.UncheckedAsList();
    alphabeticList.reserve(listNode.size());
    for (size_t index = 0; index < listNode.size(); ++index) {
        alphabeticList.push_back(listNode[permutation[index]]);
    }

    TStringStream err;
    auto value = NCommon::ParseYsonNodeInResultFormat(HolderFactory, alphabeticItem, Specs.Outputs[0].RowType, &err);
    if (!value) {
        throw yexception() << "Could not parse yson node with error: " << err.Str();
    }

    // Call above produces rows in alphabetic order.
    SkiffWriter.AddRow(*value);

    return IsList;
}

void TSkiffExecuteResOrPull::WriteValue(const NUdf::TUnboxedValue& value, TType* type) {
    if (type->IsList()) {
        SetListResult();
        const auto it = value.GetListIterator();
        for (NUdf::TUnboxedValue item; it.Next(item); ++Row) {
            if (!HasCapacity()) {
                Truncated = true;
                break;
            }

            SkiffWriter.AddRow(item);
        }
    } else {
        SkiffWriter.AddRow(value);
    }
}

bool TSkiffExecuteResOrPull::WriteNext(TMkqlIOCache& specsCache, const NYT::TNode& rec, ui32 /*tableIndex*/) {
    if (!HasCapacity()) {
        Truncated = true;
        return false;
    }

    // For now this method is used only for the nodes that are maps from column name
    // to value. Feel free to extend it for other types (lists, for example) but
    // make sure that the order of columns is correct.
    YQL_ENSURE(rec.GetType() == NYT::TNode::EType::Map, "Expected map node");

    TStringStream err;
    auto value = NCommon::ParseYsonNode(specsCache.GetHolderFactory(), rec, Specs.Outputs[0].RowType, Specs.Outputs[0].NativeYtTypeFlags, &err);
    if (!value) {
        throw yexception() << "Could not parse yson node with error: " << err.Str();
    }
    SkiffWriter.AddRow(*value);

    ++Row;
    return true;
}

bool TSkiffExecuteResOrPull::WriteNext(TMkqlIOCache& specsCache, const NYT::TYaMRRow& rec, ui32 tableIndex) {
    if (!HasCapacity()) {
        Truncated = true;
        return false;
    }

    NUdf::TUnboxedValue node;
    node = DecodeYamr(specsCache, tableIndex, rec);
    SkiffWriter.AddRow(node);

    ++Row;
    return true;
}

bool TSkiffExecuteResOrPull::WriteNext(TMkqlIOCache& specsCache, const NUdf::TUnboxedValue& rec, ui32 tableIndex) {
    Y_UNUSED(specsCache);
    Y_UNUSED(tableIndex);

    if (!HasCapacity()) {
        Truncated = true;
        return false;
    }

    YQL_ENSURE(rec.GetListLength() == AlphabeticPermutation->size());
    TUnboxedValueVector alphabeticValues(rec.GetListLength());
    for (size_t index = 0; index < rec.GetListLength(); ++index) {
        alphabeticValues[index] = rec.GetElement((*AlphabeticPermutation)[index]);
    }

    NUdf::TUnboxedValue alphabeticRecord = HolderFactory.RangeAsArray(alphabeticValues.begin(), alphabeticValues.end());
    SkiffWriter.AddRow(alphabeticRecord);

    ++Row;
    return true;
}

void TSkiffExecuteResOrPull::SetListResult() {
    IsList = true;
}

TMaybe<TVector<ui32>> TSkiffExecuteResOrPull::CreateAlphabeticPositions(NKikimr::NMiniKQL::TType* inputType, const TVector<TString>& columns)
{
    if (inputType->GetKind() != TType::EKind::Struct) {
        return Nothing();
    }
    auto inputStruct = AS_TYPE(TStructType, inputType);

    YQL_ENSURE(columns.empty() || columns.size() == inputStruct->GetMembersCount());

    if (columns.empty()) {
        TVector<ui32> positions(inputStruct->GetMembersCount());
        for (size_t index = 0; index < positions.size(); ++index) {
            positions[index] = index;
        }
        return positions;
    }

    TMap<TStringBuf, ui32> orders;
    for (size_t index = 0; index < columns.size(); ++index) {
        orders.emplace(columns[index], -1);
    }
    {
        ui32 index = 0;
        for (auto& [column, order] : orders) {
            order = index++;
        }
    }

    TVector<ui32> positions(columns.size());
    for (size_t index = 0; index < columns.size(); ++index) {
        positions[orders[columns[index]]] = index;
    }

    return positions;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

} // NYql
