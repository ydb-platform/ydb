#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/sql_types/sort_order.h>
#include <yql/essentials/core/yql_window_frame_settings.h>

namespace NYql {

class TWindowSortedColumnPusher {
public:
    TWindowSortedColumnPusher(const TExprNode::TPtr& sortTraits, TExprContext& ctx, const TExprNode::TPtr& frames);

    std::pair<TStringBuf, TStringBuf> GetRangeSortedColumnNames(const TWindowFrameSettings::TRangeFrame& rangeFrame) const;

    TVector<TStringBuf> GetAllSortedColumnNames() const;

    bool ShouldAddSortedColumn() const;

    ESortOrder SortOrder() const;

    TExprNode::TPtr GetStreamWithSortedColumns(TExprNode::TPtr stream, TExprNode::TPtr sortKeySelector) const;
    TExprNode::TPtr ClearStreamFromSortedColumns(TExprNode::TPtr stream) const;

private:
    TExprNode::TPtr AddSortedColumnsToStream(TExprNode::TPtr stream, TExprNode::TPtr sortKeySelector) const;

    using TNodeToColumnNameAndNodeTransform = THashMap<TExprNode::TPtr, std::pair<TNodeTransform, TString>>;

    ESortOrder SortOrder_;
    TExprContext& Ctx_;
    TNodeToColumnNameAndNodeTransform MappedSortedColumnsToNames_;
    TMaybe<const TTypeAnnotationNode*> SortColumnType_;
};

} // namespace NYql
