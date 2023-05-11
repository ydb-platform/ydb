#pragma once

#include "yql_kikimr_provider.h"

namespace NYql {

void KikimrResultToYson(const TStringStream& stream, NYson::TYsonWriter& writer, const NKikimrMiniKQL::TResult& result,
    const TVector<TString>& columnHints, const IDataProvider::TFillSettings& fillSettings, bool& truncated);

NKikimrMiniKQL::TResult* KikimrResultToProto(const NKikimrMiniKQL::TResult& result, const TVector<TString>& columnHints,
    const IDataProvider::TFillSettings& fillSettings, google::protobuf::Arena* arena);

bool IsRawKikimrResult(const NKikimrMiniKQL::TResult& result);

const TTypeAnnotationNode* ParseTypeFromKikimrProto(const NKikimrMiniKQL::TType& type, TExprContext& ctx);
bool ExportTypeToKikimrProto(const TTypeAnnotationNode& type, NKikimrMiniKQL::TType& protoType, TExprContext& ctx);
TExprNode::TPtr ParseKikimrProtoValue(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value,
    TPositionHandle pos, TExprContext& ctx);

const TTypeAnnotationNode* ParseTypeFromYdbType(const Ydb::Type& input, TExprContext& ctx);

} // namespace NYql
