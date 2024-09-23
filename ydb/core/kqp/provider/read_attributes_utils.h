#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

class IAstAttributesVisitor {
public:
    virtual ~IAstAttributesVisitor() = default;

    virtual void VisitRead(TExprNode& read, TString cluster, TString tablePath) = 0;
    virtual void ExitRead() = 0;

    virtual void VisitAttribute(TString key, TString value) = 0;

    virtual void VisitNonAttribute(TExprNode::TPtr node) = 0;
};

void TraverseReadAttributes(IAstAttributesVisitor& visitor, TExprNode& node, TExprContext& ctx);

THashMap<std::pair<TString, TString>, THashMap<TString, TString>> GatherReadAttributes(TExprNode& node, TExprContext& ctx);

void ReplaceReadAttributes(TExprNode& node,
                           THashMap<TString, TString> attributesBeforeFilter,
                           TStringBuf cluster, TStringBuf tablePath,
                           TKikimrTableMetadataPtr metadata,
                           TExprContext& ctx);

TExprNode::TPtr BuildSchemaFromMetadata(TPositionHandle pos, TExprContext& ctx, const TMap<TString, NYql::TKikimrColumnMetadata>& columns);
} // namespace NYql
