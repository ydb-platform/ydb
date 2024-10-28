#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <library/cpp/yson/consumer.h>
#include <library/cpp/yson/node/node.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/strbuf.h>

#include <functional>

namespace NYql {

class TTypeAnnotationNode;
class TStructExprType;
struct TExprContext;

namespace NCommon {

// empty return value means "remove member"
using TStructMemberMapper = std::function<TMaybe<TStringBuf> (TStringBuf member)>;

void WriteTypeToYson(NYson::TYsonConsumerBase& writer, const TTypeAnnotationNode* type, bool extendedForm = false);

// saves in columns order
void SaveStructTypeToYson(NYson::TYsonConsumerBase& writer, const TStructExprType* type,
    const TMaybe<TColumnOrder>& columns = {}, const TStructMemberMapper& mapper = {}, bool extendedForm = false);

NYT::TNode TypeToYsonNode(const TTypeAnnotationNode* type, bool extendedForm = false);
TString WriteTypeToYson(const TTypeAnnotationNode* type, NYT::NYson::EYsonFormat format = NYT::NYson::EYsonFormat::Binary,
    bool extendedForm = false);

const TTypeAnnotationNode* ParseTypeFromYson(const TStringBuf yson, TExprContext& ctx, const TPosition& pos = TPosition());
const TTypeAnnotationNode* ParseOrderAwareTypeFromYson(const TStringBuf yson, TColumnOrder& topLevelColumns, TExprContext& ctx, const TPosition& pos = TPosition());
const TTypeAnnotationNode* ParseTypeFromYson(const NYT::TNode& node, TExprContext& ctx, const TPosition& pos = TPosition());
const TTypeAnnotationNode* ParseOrderAwareTypeFromYson(const NYT::TNode& node, TColumnOrder& topLevelColumns, TExprContext& ctx, const TPosition& pos = TPosition());

void WriteResOrPullType(NYson::TYsonConsumerBase& writer, const TTypeAnnotationNode* type,
    const TColumnOrder& columns);

} // namespace NCommon
} // namespace NYql
