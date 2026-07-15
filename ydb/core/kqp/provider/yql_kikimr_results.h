#pragma once

#include <yql/essentials/core/yql_data_provider.h>

#include <util/stream/str.h>

namespace NYson {

class TYsonWriter;

} // namespace NYson

namespace NKikimrMiniKQL {

class TResult;

} // namespace NKikimrMiniKQL

namespace Ydb {

class Type;

} // namespace Ydb

namespace NYql {

class TColumnOrder;
class TTypeAnnotationNode;
struct TExprContext;

void KikimrResultToYson(const TStringStream& stream, NYson::TYsonWriter& writer, const NKikimrMiniKQL::TResult& result,
    const TColumnOrder& columnHints, const IDataProvider::TFillSettings& fillSettings, bool& truncated);

const TTypeAnnotationNode* ParseTypeFromYdbType(const Ydb::Type& input, TExprContext& ctx);

} // namespace NYql
