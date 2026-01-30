#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/mkql_node.h>

#include <yt/yql/providers/ytflow/integration/mkql_interface/yql_ytflow_lookup_provider.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>


namespace google::protobuf {

class Any;

} // namespace google::protobuf

namespace NYql::NCommon {

struct TMkqlBuildContext;

} // namespace NYql::NCommon

namespace NYql {

class IYtflowIntegration {
public:
    virtual ~IYtflowIntegration() = default;

    // Nothing if callable is not for reading,
    // false if callable is for reading and there are some errors (they are added to ctx),
    // true if callable is for reading and no issues occurred.
    virtual TMaybe<bool> CanRead(const TExprNode& read, TExprContext& ctx) = 0;
    virtual TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx) = 0;

    // Nothing if callable is not for writing,
    // false if callable is for writing and there are some errors (they are added to ctx),
    // true if callable is for writing and no issues occurred.
    virtual TMaybe<bool> CanWrite(const TExprNode& write, TExprContext& ctx) = 0;
    virtual TExprNode::TPtr WrapWrite(
        const TExprNode::TPtr& write,
        TExprContext& ctx) = 0;

    // Nothing if callable doesn't support lookup reads
    // false if callable supports lookup reads and there are some errors (they are added to ctx),
    // true if callable supports lookup reads and no issues occurred.
    virtual TMaybe<bool> CanLookupRead(
        const TExprNode& read,
        const TVector<TStringBuf>& keys,
        ERowSelectionMode rowSelectionMode,
        TExprContext& ctx) = 0;

    virtual TExprNode::TPtr GetReadWorld(const TExprNode& read, TExprContext& ctx) = 0;
    virtual TExprNode::TPtr GetWriteWorld(const TExprNode& write, TExprContext& ctx) = 0;
    virtual TExprNode::TPtr UpdateWriteWorld(
        const TExprNode::TPtr& write,
        const TExprNode::TPtr& world,
        TExprContext& ctx) = 0;

    virtual TExprNode::TPtr GetWriteContent(
        const TExprNode& write,
        TExprContext& ctx) = 0;

    virtual void FillSourceSettings(
        const TExprNode& source,
        ::google::protobuf::Any& settings,
        TExprContext& ctx) = 0;

    virtual void FillSinkSettings(
        const TExprNode& sink,
        ::google::protobuf::Any& settings,
        TExprContext& ctx) = 0;

    virtual NKikimr::NMiniKQL::TRuntimeNode BuildLookupSourceArgs(
        const TExprNode& read,
        NCommon::TMkqlBuildContext& ctx) = 0;
};

// Non-operational base which enables descendants to provide only partial functionality
class TEmptyYtflowIntegration
    : public IYtflowIntegration
{
public:
    TMaybe<bool> CanRead(const TExprNode& read, TExprContext& ctx) override;
    TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx) override;

    TMaybe<bool> CanWrite(const TExprNode& write, TExprContext& ctx) override;
    TExprNode::TPtr WrapWrite(const TExprNode::TPtr& write, TExprContext& ctx) override;

    TMaybe<bool> CanLookupRead(
        const TExprNode& read,
        const TVector<TStringBuf>& keys,
        ERowSelectionMode rowSelectionMode,
        TExprContext& ctx) override;

    TExprNode::TPtr GetReadWorld(const TExprNode& read, TExprContext& ctx) override;
    TExprNode::TPtr GetWriteWorld(const TExprNode& write, TExprContext& ctx) override;
    TExprNode::TPtr UpdateWriteWorld(
        const TExprNode::TPtr& write,
        const TExprNode::TPtr& world,
        TExprContext& ctx) override;

    TExprNode::TPtr GetWriteContent(const TExprNode& write, TExprContext& ctx) override;

    void FillSourceSettings(
        const TExprNode& source,
        ::google::protobuf::Any& settings,
        TExprContext& ctx) override;

    void FillSinkSettings(
        const TExprNode& sink,
        ::google::protobuf::Any& settings,
        TExprContext& ctx) override;

    NKikimr::NMiniKQL::TRuntimeNode BuildLookupSourceArgs(
        const TExprNode& read,
        NCommon::TMkqlBuildContext& ctx) override;
};

} // namespace NYql
