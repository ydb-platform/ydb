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

    SkiffWriter.SetSpecs(Specs, columns);
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

    TStringStream err;
    auto value = NCommon::ParseYsonNodeInResultFormat(HolderFactory, item, Specs.Outputs[0].RowType, &err);
    if (!value) {
        throw yexception() << "Could not parse yson node with error: " << err.Str();
    }
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

bool TSkiffExecuteResOrPull::WriteNext(TMkqlIOCache& specsCache, const NYT::TNode& rec, ui32 tableIndex) {
    if (!HasCapacity()) {
        Truncated = true;
        return false;
    }

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

    SkiffWriter.AddRow(rec);

    ++Row;
    return true;
}

void TSkiffExecuteResOrPull::SetListResult() {
    if (!IsList) {
        IsList = true;
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

} // NYql
