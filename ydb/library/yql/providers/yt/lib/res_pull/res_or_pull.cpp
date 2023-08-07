#include "res_or_pull.h"

#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <util/stream/holder.h>
#include <util/stream/str.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

TExecuteResOrPull::TExecuteResOrPull(TMaybe<ui64> rowLimit, TMaybe<ui64> byteLimit, const TMaybe<TVector<TString>>& columns)
    : Rows(rowLimit)
    , Bytes(byteLimit)
    , Columns(columns)
    , Out(new THoldingStream<TCountingOutput>(THolder(new TStringOutput(Result))))
    , Writer(new NYson::TYsonWriter(Out.Get(), NYson::EYsonFormat::Binary, ::NYson::EYsonType::Node, true))
    , IsList(false)
    , Truncated(false)
    , Row(0)
{
}

ui64 TExecuteResOrPull::GetWrittenSize() const {
    YQL_ENSURE(Out, "GetWritten() must be callled before Finish()");
    return Out->Counter();
}

TString TExecuteResOrPull::Finish() {
    if (IsList) {
        Writer->OnEndList();
    }
    Writer.Destroy();
    Out.Destroy();
    return Result;
}

bool TExecuteResOrPull::WriteNext(TStringBuf val) {
    if (IsList) {
        if (!HasCapacity()) {
            Truncated = true;
            return false;
        }
        Writer->OnListItem();
    }
    Writer->OnRaw(val, ::NYson::EYsonType::Node);
    ++Row;
    return IsList;
}

void TExecuteResOrPull::WriteValue(const NUdf::TUnboxedValue& value, TType* type) {
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

}
