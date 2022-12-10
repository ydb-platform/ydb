#pragma once
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/string.h>

namespace NYql {

class TTopicKeyParser {
public:
    TTopicKeyParser() {}
    TTopicKeyParser(const TExprNode& expr, TExprNode::TPtr readSettings, TExprContext& ctx);

    const TString& GetTopicPath() const {
        return TopicPath;
    }

    TExprNode::TPtr GetUserSchema() const {
        return UserSchema;
    }

    TExprNode::TPtr GetColumnOrder() const {
        return ColumnOrder;
    }

    const TString& GetFormat() const {
        return Format;
    }

    const TString& GetCompression() const {
        return Compression;
    }

    TExprNode::TPtr GetDateTimeFormatName() const {
        return DateTimeFormatName;
    }

    TExprNode::TPtr GetDateTimeFormat() {
        return DateTimeFormat;
    }

    TExprNode::TPtr GetTimestampFormatName() {
        return TimestampFormatName;
    }

    TExprNode::TPtr GetTimestampFormat() {
        return TimestampFormat;
    }

    bool Parse(const TExprNode& expr, TExprNode::TPtr readSettings, TExprContext& ctx);

private:
    bool TryParseKey(const TExprNode& expr, TExprContext& ctx);
    bool TryParseObject(const TExprNode& expr, TExprNode::TPtr readSettings);

private:
    TString TopicPath;
    TString Format;
    TString Compression;
    TExprNode::TPtr DateTimeFormatName;
    TExprNode::TPtr DateTimeFormat;
    TExprNode::TPtr TimestampFormatName;
    TExprNode::TPtr TimestampFormat;
    TExprNode::TPtr UserSchema;
    TExprNode::TPtr ColumnOrder;
};

} // namespace NYql
