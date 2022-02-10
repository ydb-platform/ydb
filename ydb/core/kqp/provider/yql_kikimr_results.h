#pragma once

#include "yql_kikimr_provider.h"

namespace NYql {

bool GetRunResultIndex(const NKikimrMiniKQL::TStructType& resultType, const TString& resultName, ui32& index);

NKikimrMiniKQL::TResult GetKikimrUnpackedRunResult(const NKikimrMiniKQL::TResult& runResult, ui32 index);
void GetKikimrUnpackedRunResult(const NKikimrMiniKQL::TResult& runResult, ui32 index,
    const NKikimrMiniKQL::TType*& type, const NKikimrMiniKQL::TValue*& value);

TVector<NKikimrMiniKQL::TResult*> UnpackKikimrRunResult(const NKikimrMiniKQL::TResult& runResult, google::protobuf::Arena* arena); 

void KikimrResultToYson(const TStringStream& stream, NYson::TYsonWriter& writer, const NKikimrMiniKQL::TResult& result,
    const TVector<TString>& columnHints, const IDataProvider::TFillSettings& fillSettings, bool& truncated);

NKikimrMiniKQL::TResult* KikimrResultToProto(const NKikimrMiniKQL::TResult& result, const TVector<TString>& columnHints, 
    const IDataProvider::TFillSettings& fillSettings, google::protobuf::Arena* arena); 

bool IsRawKikimrResult(const NKikimrMiniKQL::TResult& result);

const TTypeAnnotationNode* ParseTypeFromKikimrProto(const NKikimrMiniKQL::TType& type, TExprContext& ctx);
bool ExportTypeToKikimrProto(const TTypeAnnotationNode& type, NKikimrMiniKQL::TType& protoType, TExprContext& ctx);
TExprNode::TPtr ParseKikimrProtoValue(const NKikimrMiniKQL::TType& type, const NKikimrMiniKQL::TValue& value,
    TPositionHandle pos, TExprContext& ctx);
bool CheckKqpResultType(const NKikimrMiniKQL::TResult& kqpResult, const TTypeAnnotationNode& expectedType,
    TExprContext& ctx);

TMaybe<TString> KqpResultToYson(const NKikimrMiniKQL::TResult& kqpResult, const NYson::EYsonFormat& ysonFormat,
    TExprContext& ctx);

TMaybe<TString> GetTableListResult(const IKikimrGateway::TListPathResult& res,
    const IDataProvider::TFillSettings& fillSettings, TExprContext& ctx);

TMaybe<TString> GetTableMetadataResult(const TKikimrTableDescription& table,
    const IDataProvider::TFillSettings& fillSettings, TExprContext& ctx);

void TransformerStatsToProto(const TString& name, const IGraphTransformer::TStatistics& stats,
    NKikimrKqp::TTransformProfile& profile);

void TransformerStatsFromProto(const NKikimrKqp::TTransformProfile& proto, IGraphTransformer::TStatistics& stats,
    TString& name);

void KikimrProfileToYson(const NKikimrKqp::TKqlProfile& kqlProfile, NYson::TYsonWriter& writer);
void KikimrProfileToYson(const NKikimrKqp::TQueryProfile& profile, NYson::TYsonWriter& writer);

} // namespace NYql
