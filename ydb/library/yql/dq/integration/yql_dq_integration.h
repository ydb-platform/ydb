#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/yson/writer.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>

#include <google/protobuf/any.pb.h>

namespace NYql {

struct TDqSettings;

namespace NCommon {
    class TMkqlCallableCompilerBase;
}

class TFallbackError: public yexception {
public:
    TFallbackError(TIssuePtr issue = {})
        : Issue_(std::move(issue))
    {}

    TIssuePtr GetIssue() const {
        return Issue_;
    }
private:
    TIssuePtr Issue_;
};

class IDqIntegration {
public:
    virtual ~IDqIntegration() {}

    virtual ui64 Partition(const TDqSettings& config, size_t maxPartitions, const TExprNode& node,
        TVector<TString>& partitions, TString* clusterName, TExprContext& ctx, bool canFallback) = 0;
    virtual bool CheckPragmas(const TExprNode& node, TExprContext& ctx, bool skipIssues = false) { Y_UNUSED(skipIssues); Y_UNUSED(node); Y_UNUSED(ctx); return true; }
    virtual bool CanRead(const TExprNode& read, TExprContext& ctx, bool skipIssues = true) = 0;
    virtual TMaybe<ui64> EstimateReadSize(ui64 dataSizePerJob, ui32 maxTasksPerStage, const TExprNode& node, TExprContext& ctx) = 0;
    virtual TExprNode::TPtr WrapRead(const TDqSettings& config, const TExprNode::TPtr& read, TExprContext& ctx) = 0;

    // Nothing if callable is not for writing,
    // false if callable is for writing and there are some errors (they are added to ctx),
    // true if callable is for writing and no issues occured.
    virtual TMaybe<bool> CanWrite(const TExprNode& write, TExprContext& ctx) = 0;

    virtual TExprNode::TPtr WrapWrite(const TExprNode::TPtr& write, TExprContext& ctx) = 0;
    virtual void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) = 0;
    virtual bool CanFallback() = 0;
    virtual void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& settings, TString& sourceType) = 0;
    virtual void FillSinkSettings(const TExprNode& node, ::google::protobuf::Any& settings, TString& sinkType) = 0;
    virtual void FillTransformSettings(const TExprNode& node, ::google::protobuf::Any& settings) = 0;
    virtual void Annotate(const TExprNode& node, THashMap<TString, TString>& params) = 0;
    virtual bool PrepareFullResultTableParams(const TExprNode& root, TExprContext& ctx, THashMap<TString, TString>& params, THashMap<TString, TString>& secureParams) = 0;
    virtual void WriteFullResultTableRef(NYson::TYsonWriter& writer, const TVector<TString>& columns, const THashMap<TString, TString>& graphParams) = 0;
};

} // namespace NYql
