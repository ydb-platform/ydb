#include "yql.h"

#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>

TMaybe<TString> KqpResultToYson(const NKikimrMiniKQL::TResult& kqpResult,
    const NYson::EYsonFormat& ysonFormat, NYql::TExprContext& ctx)
{
    YQL_ENSURE(kqpResult.GetType().GetKind() == NKikimrMiniKQL::ETypeKind::Struct);
    const auto& structType = kqpResult.GetType().GetStruct();
    YQL_ENSURE(structType.GetMember(0).GetName() == "Data");
    YQL_ENSURE(structType.GetMember(1).GetName() == "Truncated");

    NKikimrMiniKQL::TResult dataResult;
    dataResult.MutableType()->CopyFrom(structType.GetMember(0).GetType());
    dataResult.MutableValue()->CopyFrom(kqpResult.GetValue().GetStruct(0));

    TStringStream out;
    NYson::TYsonWriter writer(&out, ysonFormat, ::NYson::EYsonType::Node, true);
    writer.OnBeginMap();
    writer.OnKeyedItem("Type");

    TVector<TString> columns;
    if (dataResult.GetType().GetKind() == NKikimrMiniKQL::ETypeKind::List) {
        const auto& itemType = dataResult.GetType().GetList().GetItem();
        if (itemType.GetKind() == NKikimrMiniKQL::ETypeKind::Struct) {
            for (auto& member : itemType.GetStruct().GetMember()) {
                columns.push_back(member.GetName());
            }
        }
    }

    auto resultDataType = NYql::ParseTypeFromKikimrProto(dataResult.GetType(), ctx);
    if (!resultDataType) {
        return Nothing();
    }
    NYql::NCommon::WriteResOrPullType(writer, resultDataType, columns);

    writer.OnKeyedItem("Data");

    NYql::IDataProvider::TFillSettings fillSettings;
    fillSettings.AllResultsBytesLimit = Nothing();
    fillSettings.RowsLimitPerWrite = Nothing();
    fillSettings.Format = NYql::IDataProvider::EResultFormat::Yson;
    fillSettings.FormatDetails = ToString((ui32)ysonFormat);

    bool truncated;
    NYql::KikimrResultToYson(out, writer, dataResult, columns, fillSettings, truncated);
    YQL_ENSURE(!truncated);

    if (kqpResult.GetValue().GetStruct(1).GetBool()) {
        writer.OnKeyedItem("Truncated");
        writer.OnBooleanScalar(true);
    }

    writer.OnEndMap();
    return out.Str();
}
