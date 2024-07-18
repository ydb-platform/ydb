#include "res_or_pull.h"

#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/utils/log/log.h>

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
    , Columns(columns)
{
    Specs.SetUseSkiff(optLLVM);
    Specs.Init(codecCtx, attrs);

    AlphabeticPermutations.reserve(Specs.Outputs.size());
    for (size_t index = 0; index < Specs.Outputs.size(); ++index) {
        const auto& output = Specs.Outputs[index];
        auto columnPermutation = NCommon::CreateStructPositions(output.RowType, Columns.empty() ? nullptr : &Columns);
        AlphabeticPermutations.push_back(columnPermutation);
    }
}

TString TSkiffExecuteResOrPull::Finish() {
    SkiffWriter.Finish();
    Out.Destroy();
    Specs.Clear();
    return Result;
}

bool TSkiffExecuteResOrPull::WriteNext(const NYT::TNode& item) {
    // Row will be made alphabetic below, so for Skiff writer it is always alphabetic.
    EnsureShuffled(false);

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

    const auto& permutation = *AlphabeticPermutations[0];
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
    SkiffWriter.AddRow(*value);

    return IsList;
}

void TSkiffExecuteResOrPull::WriteValue(const NUdf::TUnboxedValue& value, TType* type) {
    EnsureShuffled(false);

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
        YQL_LOG(ERROR) << "WriteValue(value, type): " << value << " " << value.GetListLength();
        for (size_t i = 0; i < value.GetListLength(); ++i) {
            YQL_LOG(ERROR) << "WriteNext(specsCache, recBox, tableIndex): " << value.GetElement(i);
        }
        SkiffWriter.AddRow(value);
    }
}

bool TSkiffExecuteResOrPull::WriteNext(TMkqlIOCache& specsCache, const NYT::TNode& rec, ui32 tableIndex) {
    EnsureShuffled(false);

    if (!HasCapacity()) {
        Truncated = true;
        return false;
    }

    // For now this method is used only for the nodes that are maps from column name
    // to value. Feel free to extend it for other types (lists, for example) but
    // make sure that the order of columns is correct.
    YQL_ENSURE(rec.GetType() == NYT::TNode::EType::Map, "Expected map node");

    TStringStream err;
    auto value = NCommon::ParseYsonNode(specsCache.GetHolderFactory(), rec, Specs.Outputs[tableIndex].RowType, Specs.Outputs[tableIndex].NativeYtTypeFlags, &err);
    if (!value) {
        throw yexception() << "Could not parse yson node with error: " << err.Str();
    }
    SkiffWriter.AddRow(*value);

    ++Row;
    return true;
}

bool TSkiffExecuteResOrPull::WriteNext(TMkqlIOCache& specsCache, const NYT::TYaMRRow& rec, ui32 tableIndex) {
    // For YAMR format the order of columns is not actually important.
    EnsureShuffled(false);

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

    EnsureShuffled(true);

    if (!HasCapacity()) {
        Truncated = true;
        return false;
    }

    SkiffWriter.AddRow(rec);

    ++Row;
    return true;
}

void TSkiffExecuteResOrPull::SetListResult() {
    IsList = true;
}

void TSkiffExecuteResOrPull::EnsureShuffled(bool shuffled) {
    if (Shuffled) {
        YQL_ENSURE(*Shuffled == shuffled, "Shuffled and alphabetic rows are mixed in the Skiff writer");
        return;
    }

    Shuffled = shuffled;
    if (shuffled) {
        // Now Skiff writer expects rows with values order corresponding to Columns (i.e. the output order).
        SkiffWriter.SetSpecs(Specs, Columns);
    } else {
        // Now Skiff writer expects rows with values order corresponding to columns in alphabetical order.
        SkiffWriter.SetSpecs(Specs);
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

} // NYql
