#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/maybe.h>


namespace google::protobuf {
    class Any;
} // namespace google::protobuf


namespace NYql {

class IYtflowIntegration {
public:
    virtual ~IYtflowIntegration() = default;

    // Nothing if callable is not for reading,
    // false if callable is for reading and there are some errors (they are added to ctx),
    // true if callable is for reading and no issues occured.
    virtual TMaybe<bool> CanRead(const TExprNode& read, TExprContext& ctx) = 0;
    virtual TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx) = 0;

    // Nothing if callable is not for writing,
    // false if callable is for writing and there are some errors (they are added to ctx),
    // true if callable is for writing and no issues occured.
    virtual TMaybe<bool> CanWrite(const TExprNode& write, TExprContext& ctx) = 0;
    virtual TExprNode::TPtr WrapWrite(const TExprNode::TPtr& write, TExprContext& ctx) = 0;

    virtual TExprNode::TPtr GetReadWorld(const TExprNode& read, TExprContext& ctx) = 0;
    virtual TExprNode::TPtr GetWriteWorld(const TExprNode& write, TExprContext& ctx) = 0;

    virtual TExprNode::TPtr GetWriteContent(const TExprNode& write, TExprContext& ctx) = 0;

    virtual void FillSourceSettings(
        const TExprNode& source, ::google::protobuf::Any& settings, TExprContext& ctx) = 0;

    virtual void FillSinkSettings(
        const TExprNode& sink, ::google::protobuf::Any& settings, TExprContext& ctx) = 0;
};

} // namespace NYql
