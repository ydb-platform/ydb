#pragma once
#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/string.h>

namespace NYql {

class TTopicKeyParser {
public:
    TTopicKeyParser() = default;
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

    TExprNode::TPtr GetDateTimeFormat() const {
        return DateTimeFormat;
    }

    TExprNode::TPtr GetTimestampFormatName() const {
        return TimestampFormatName;
    }

    TExprNode::TPtr GetTimestampFormat() const {
        return TimestampFormat;
    }

    TExprNode::TPtr GetDateFormat() const {
        return DateFormat;
    }

    TExprNode::TPtr GetWatermarkAdjustLateEvents() const {
        return WatermarkAdjustLateEvents;
    }

    TExprNode::TPtr GetWatermarkDropLateEvents() const {
        return WatermarkDropLateEvents;
    }

    TExprNode::TPtr GetWatermarkGranularity() const {
        return WatermarkGranularity;
    }

    TExprNode::TPtr GetWatermarkIdleTimeout() const {
        return WatermarkIdleTimeout;
    }

    TExprNode::TPtr GetWatermark() const {
        return Watermark;
    }

    TExprNode::TPtr GetSkipJsonErrors() const {
        return SkipJsonErrors;
    }

    TExprNode::TPtr GetStreamingTopicRead() const {
        return StreamingTopicRead;
    }

    bool Parse(const TExprNode& expr, TExprNode::TPtr readSettings, TExprContext& ctx);

    static std::optional<bool> ParseStreamingTopicRead(const TExprNode& expr, TExprContext& ctx);

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
    TExprNode::TPtr DateFormat;
    TExprNode::TPtr UserSchema;
    TExprNode::TPtr ColumnOrder;
    TExprNode::TPtr WatermarkAdjustLateEvents;
    TExprNode::TPtr WatermarkDropLateEvents;
    TExprNode::TPtr WatermarkGranularity;
    TExprNode::TPtr WatermarkIdleTimeout;
    TExprNode::TPtr Watermark;
    TExprNode::TPtr SkipJsonErrors;
    TExprNode::TPtr StreamingTopicRead;
};

} // namespace NYql
