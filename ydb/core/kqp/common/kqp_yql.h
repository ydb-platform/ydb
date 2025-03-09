#pragma once

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <yql/essentials/ast/yql_pos_handle.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

const TStringBuf KqpEffectTag = "KqpEffect";

enum class EPhysicalQueryType {
    Unspecified,
    Data,
    Scan,
    GenericQuery,
    GenericScript,
};

struct TKqpPhyQuerySettings {
    static constexpr std::string_view TypeSettingName = "type"sv;
    std::optional<EPhysicalQueryType> Type;

    static TKqpPhyQuerySettings Parse(const NNodes::TKqpPhysicalQuery& node);
    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
};

enum class EPhysicalTxType {
    Unspecified,
    Compute,
    Data,
    Scan,
    Generic
};

struct TKqpPhyTxSettings {
    static constexpr TStringBuf TypeSettingName = "type";
    std::optional<EPhysicalTxType> Type;

    static constexpr std::string_view WithEffectsSettingName = "with_effects"sv;
    bool WithEffects = false;

    static TKqpPhyTxSettings Parse(const NNodes::TKqpPhysicalTx& node);
    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
};

constexpr TStringBuf KqpReadRangesSourceName = "KqpReadRangesSource";
constexpr TStringBuf KqpTableSinkName = "KqpTableSinkName";

enum class EStreamLookupStrategyType {
    Unspecified,
    LookupRows,
    LookupJoinRows,
    LookupSemiJoinRows,
};

struct TKqpStreamLookupSettings {
    static constexpr TStringBuf StrategySettingName = "Strategy";
    static constexpr TStringBuf AllowNullKeysSettingName = "AllowNullKeysPrefixSize";

    // stream lookup strategy types
    static constexpr std::string_view LookupStrategyName = "LookupRows"sv;
    static constexpr std::string_view LookupJoinStrategyName = "LookupJoinRows"sv;
    static constexpr std::string_view LookupSemiJoinStrategyName = "LookupSemiJoinRows"sv;

    TMaybe<ui32> AllowNullKeysPrefixSize;
    EStreamLookupStrategyType Strategy = EStreamLookupStrategyType::Unspecified;

    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
    static TKqpStreamLookupSettings Parse(const NNodes::TKqlStreamLookupTable& node);
    static TKqpStreamLookupSettings Parse(const NNodes::TKqlStreamLookupIndex& node);
    static TKqpStreamLookupSettings Parse(const NNodes::TKqpCnStreamLookup& node);
    static TKqpStreamLookupSettings Parse(const NNodes::TCoNameValueTupleList& node);
};

struct TKqpReadTableSettings {
    static constexpr TStringBuf SkipNullKeysSettingName = "SkipNullKeys";
    static constexpr TStringBuf ItemsLimitSettingName = "ItemsLimit";
    static constexpr TStringBuf ReverseSettingName = "Reverse";
    static constexpr TStringBuf SortedSettingName = "Sorted";
    static constexpr TStringBuf SequentialSettingName = "Sequential";
    static constexpr TStringBuf ForcePrimaryName = "ForcePrimary";
    static constexpr TStringBuf GroupByFieldNames = "GroupByFieldNames";
    static constexpr TStringBuf TabletIdName = "TabletId";

    TVector<TString> SkipNullKeys;
    TExprNode::TPtr ItemsLimit;
    bool Reverse = false;
    bool Sorted = false;
    TMaybe<ui64> SequentialInFlight;
    TMaybe<ui64> TabletId;
    bool ForcePrimary = false;

    void AddSkipNullKey(const TString& key);
    void SetItemsLimit(const TExprNode::TPtr& expr) { ItemsLimit = expr; }
    void SetReverse() { Reverse = true; }
    void SetSorted() { Sorted = true; }

    bool operator == (const TKqpReadTableSettings&) const = default;

    static TKqpReadTableSettings Parse(const NNodes::TKqlReadTableBase& node);
    static TKqpReadTableSettings Parse(const NNodes::TKqlReadTableRangesBase& node);
    static TKqpReadTableSettings Parse(const NNodes::TCoNameValueTupleList& node);
    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
};

struct TKqpUpsertRowsSettings {
    static constexpr TStringBuf InplaceSettingName = "Inplace";
    static constexpr TStringBuf IsUpdateSettingName = "IsUpdate";
    static constexpr TStringBuf IsConditionalUpdateSettingName = "IsConditionalUpdate";
    static constexpr TStringBuf AllowInconsistentWritesSettingName = "AllowInconsistentWrites";
    static constexpr TStringBuf ModeSettingName = "Mode";

    bool Inplace = false;
    bool IsUpdate = false;
    bool IsConditionalUpdate = false;
    bool AllowInconsistentWrites = false;
    TString Mode = "";

    void SetInplace() { Inplace = true; }
    void SetIsUpdate() { IsUpdate = true; }
    void SetIsConditionalUpdate() { IsConditionalUpdate = true; }
    void SetAllowInconsistentWrites() { AllowInconsistentWrites = true; }
    void SetMode(TStringBuf mode) { Mode = mode; }

    static TKqpUpsertRowsSettings Parse(const NNodes::TCoNameValueTupleList& settingsList);
    static TKqpUpsertRowsSettings Parse(const NNodes::TKqpUpsertRows& node);
    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
};

struct TKqpDeleteRowsSettings {
    static constexpr TStringBuf IsConditionalDeleteSettingName = "IsConditionalDelete";

    bool IsConditionalDelete = false;

    void SetIsConditionalDelete() { IsConditionalDelete = true; }

    static TKqpDeleteRowsSettings Parse(const NNodes::TCoNameValueTupleList& settingsList);
    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
};

struct TKqpReadTableExplainPrompt {
    static constexpr TStringBuf UsedKeyColumnsName = "UsedKeyColumns";
    static constexpr TStringBuf ExpectedMaxRangesName = "ExpectedMaxRanges";
    static constexpr TStringBuf PointPrefixLenName = "PointPrefixLen";

    TVector<TString> UsedKeyColumns;
    TMaybe<ui64> ExpectedMaxRanges;
    ui64 PointPrefixLen = 0;

    void SetUsedKeyColumns(TVector<TString> columns) {
        UsedKeyColumns = columns;
    }

    void SetExpectedMaxRanges(size_t count) {
        ExpectedMaxRanges = count;
    }

    void SetPointPrefixLen(size_t len) {
        PointPrefixLen = len;
    }

    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
    static TKqpReadTableExplainPrompt Parse(const NNodes::TKqlReadTableRangesBase& node);
    static TKqpReadTableExplainPrompt Parse(const NNodes::TCoNameValueTupleList& node);
};

TString KqpExprToPrettyString(const TExprNode& expr, TExprContext& ctx);
TString KqpExprToPrettyString(const NNodes::TExprBase& expr, TExprContext& ctx);

TString PrintKqpStageOnly(const NNodes::TDqStageBase& stage, TExprContext& ctx);

class IGraphTransformer;
struct TTypeAnnotationContext;
TAutoPtr<IGraphTransformer> GetDqIntegrationPeepholeTransformer(bool beforeDqTransforms, TIntrusivePtr<TTypeAnnotationContext> typesCtx);

} // namespace NYql
