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
constexpr TStringBuf KqpFullTextSourceName = "KqpFullTextSource";
constexpr TStringBuf KqpTableSinkName = "KqpTableSink";

enum class EStreamLookupStrategyType {
    Unspecified,
    LookupRows,
    LookupUniqueRows,
    LookupJoinRows,
    LookupSemiJoinRows,
};

struct TKqpStreamLookupSettings {
    static constexpr TStringBuf StrategySettingName = "Strategy";
    static constexpr TStringBuf AllowNullKeysSettingName = "AllowNullKeysPrefixSize";
    static constexpr TStringBuf VectorTopColumnSettingName = "VectorTopColumn";
    static constexpr TStringBuf VectorTopIndexSettingName = "VectorTopIndex";
    static constexpr TStringBuf VectorTopLimitSettingName = "VectorTopLimit";
    static constexpr TStringBuf VectorTopTargetSettingName = "VectorTopTarget";
    static constexpr TStringBuf VectorTopDistinctSettingName = "VectorTopDistinct";

    // stream lookup strategy types
    static constexpr std::string_view LookupStrategyName = "LookupRows"sv;
    static constexpr std::string_view LookupUniqueStrategyName = "LookupUniqueRows"sv;
    static constexpr std::string_view LookupJoinStrategyName = "LookupJoinRows"sv;
    static constexpr std::string_view LookupSemiJoinStrategyName = "LookupSemiJoinRows"sv;

    TMaybe<ui32> AllowNullKeysPrefixSize;
    EStreamLookupStrategyType Strategy = EStreamLookupStrategyType::Unspecified;

    // VectorTopColumn must be a fixed string, but Target and Limit may be calculated in runtime
    // Vector index settings are not needed here because we know them from the indexImpl table name or index name
    TString VectorTopColumn;
    TString VectorTopIndex;
    TExprNode::TPtr VectorTopTarget;
    TExprNode::TPtr VectorTopLimit;

    bool VectorTopDistinct = false;

    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
    static TKqpStreamLookupSettings Parse(const NNodes::TKqlStreamLookupTable& node);
    static TKqpStreamLookupSettings Parse(const NNodes::TKqlStreamLookupIndex& node);
    static TKqpStreamLookupSettings Parse(const NNodes::TKqpCnStreamLookup& node);
    static TKqpStreamLookupSettings Parse(const NNodes::TCoNameValueTupleList& node);
    static bool HasVectorTopColumn(const NNodes::TKqlStreamLookupTable& node);
    static bool HasVectorTopColumn(const NNodes::TCoNameValueTupleList& node);
};

struct TKqpDeleteRowsIndexSettings {
    bool SkipLookup = false;
    static constexpr TStringBuf SkipLookupSettingName = "SkipLookup";

    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
    static TKqpDeleteRowsIndexSettings Parse(const NNodes::TKqlDeleteRowsIndex& node);
};

enum class ERequestSorting {
    NONE = 0,
    ASC,
    DESC
};

template <ERequestSorting DefaultValue = ERequestSorting::NONE>
class TSortingOperator {
private:
    ERequestSorting Sorting = DefaultValue;
public:
    void SetSorting(const ERequestSorting sorting) {
        Sorting = sorting;
    }
    ERequestSorting GetSorting() const {
        return Sorting;
    }

    bool IsSorted() const {
        return Sorting != ERequestSorting::NONE;
    }

    bool IsReverse() const {
        return Sorting == ERequestSorting::DESC;
    }
};

struct TKqpReadTableFullTextIndexSettings: public TSortingOperator<ERequestSorting::NONE> {
public:
    static constexpr TStringBuf ItemsLimitSettingName = "ItemsLimit";
    static constexpr TStringBuf SkipLimitSettingName = "SkipLimit";
    static constexpr TStringBuf BFactorSettingName = "B";
    static constexpr TStringBuf K1FactorSettingName = "K1";
    static constexpr TStringBuf DefaultOperatorSettingName = "DefaultOperator";
    static constexpr TStringBuf MinimumShouldMatchSettingName = "MinimumShouldMatch";
    static constexpr TStringBuf ModeSettingName = "Mode";
    TExprNode::TPtr ItemsLimit;
    TExprNode::TPtr SkipLimit;
    TExprNode::TPtr BFactor;
    TExprNode::TPtr K1Factor;
    TExprNode::TPtr DefaultOperator;
    TExprNode::TPtr MinimumShouldMatch;
    TExprNode::TPtr Mode;

    void SetItemsLimit(const TExprNode::TPtr& expr) { ItemsLimit = expr; }
    void SetSkipLimit(const TExprNode::TPtr& expr) { SkipLimit = expr; }
    void SetBFactor(const TExprNode::TPtr& expr) { BFactor = expr; }
    void SetK1Factor(const TExprNode::TPtr& expr) { K1Factor = expr; }
    void SetDefaultOperator(const TExprNode::TPtr& expr) { DefaultOperator = expr; }
    void SetMinimumShouldMatch(const TExprNode::TPtr& expr) { MinimumShouldMatch = expr; }
    void SetMode(const TExprNode::TPtr& expr) { Mode = expr; }

    static TKqpReadTableFullTextIndexSettings Parse(const NNodes::TCoNameValueTupleList& node);
    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
};

struct TKqpReadTableSettings: public TSortingOperator<ERequestSorting::NONE> {
public:
    static constexpr TStringBuf SkipNullKeysSettingName = "SkipNullKeys";
    static constexpr TStringBuf ItemsLimitSettingName = "ItemsLimit";
    static constexpr TStringBuf ReverseSettingName = "Reverse";
    static constexpr TStringBuf SortedSettingName = "Sorted";
    static constexpr TStringBuf SequentialSettingName = "Sequential";
    static constexpr TStringBuf ForcePrimaryName = "ForcePrimary";
    static constexpr TStringBuf GroupByFieldNames = "GroupByFieldNames";
    static constexpr TStringBuf TabletIdName = "TabletId";
    static constexpr TStringBuf PointPrefixLenSettingName = "PointPrefixLen";
    static constexpr TStringBuf IndexSelectionDebugInfoSettingName = "IndexSelectionDebugInfo";
    static constexpr TStringBuf VectorTopKColumnSettingName = "VectorTopKColumn";
    static constexpr TStringBuf VectorTopKMetricSettingName = "VectorTopKMetric";
    static constexpr TStringBuf VectorTopKTargetSettingName = "VectorTopKTarget";
    static constexpr TStringBuf VectorTopKLimitSettingName = "VectorTopKLimit";

    TVector<TString> SkipNullKeys;
    TExprNode::TPtr ItemsLimit;
    TMaybe<ui64> SequentialInFlight;
    TMaybe<ui64> TabletId;
    bool ForcePrimary = false;
    ui64 PointPrefixLen = 0;
    THashMap<TString, TString> IndexSelectionInfo;

    // Vector top-K pushdown settings for brute force vector search
    TString VectorTopKColumn;
    TString VectorTopKMetric;
    TExprNode::TPtr VectorTopKTarget;
    TExprNode::TPtr VectorTopKLimit;

    void AddSkipNullKey(const TString& key);
    void SetItemsLimit(const TExprNode::TPtr& expr) { ItemsLimit = expr; }

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
    static constexpr TStringBuf IndexSelectionDebugInfoSettingName = "IndexSelectionDebugInfo";

    TVector<TString> UsedKeyColumns;
    TMaybe<ui64> ExpectedMaxRanges;
    ui64 PointPrefixLen = 0;
    THashMap<TString, TString> IndexSelectionInfo;

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
