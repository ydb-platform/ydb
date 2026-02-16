#include "sql.h"
#include "source.h"

#include "context.h"
#include "match_recognize.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/charset/ci_string.h>

#include <util/generic/scope.h>

using namespace NYql;

namespace NSQLTranslationV1 {

class TSubqueryNode: public INode {
public:
    TSubqueryNode(TSourcePtr&& source, const TString& alias, bool inSubquery, int ensureTupleSize, TScopedStatePtr scoped)
        : INode(source->GetPos())
        , Source_(std::move(source))
        , Alias_(alias)
        , InSubquery_(inSubquery)
        , EnsureTupleSize_(ensureTupleSize)
        , Scoped_(scoped)
    {
        YQL_ENSURE(!Alias_.empty());
    }

    ISource* GetSource() override {
        return Source_.Get();
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        YQL_ENSURE(!src, "Source not expected for subquery node");
        Source_->UseAsInner();
        if (!Source_->Init(ctx, nullptr)) {
            return false;
        }

        TTableList tableList;
        Source_->GetInputTables(tableList);

        auto tables = BuildInputTables(Pos_, tableList, InSubquery_, Scoped_);
        if (!tables->Init(ctx, Source_.Get())) {
            return false;
        }

        auto source = Source_->Build(ctx);
        if (!source) {
            return false;
        }
        if (EnsureTupleSize_ != -1) {
            source = Y("EnsureTupleSize", source, Q(ToString(EnsureTupleSize_)));
        }

        Node_ = Y("let", Alias_, Y("block", Q(L(tables, Y("return", Q(Y("world", source)))))));
        IsUsed_ = true;
        return true;
    }

    void DoUpdateState() const override {
        State_.Set(ENodeState::Const, true);
    }

    bool UsedSubquery() const override {
        return IsUsed_;
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node_);
        return Node_->Translate(ctx);
    }

    const TString* SubqueryAlias() const override {
        return &Alias_;
    }

    TPtr DoClone() const final {
        return new TSubqueryNode(Source_->CloneSource(), Alias_, InSubquery_, EnsureTupleSize_, Scoped_);
    }

protected:
    TSourcePtr Source_;
    TNodePtr Node_;
    const TString Alias_;
    const bool InSubquery_;
    const int EnsureTupleSize_;
    bool IsUsed_ = false;
    TScopedStatePtr Scoped_;
};

TNodePtr BuildSubquery(TSourcePtr source, const TString& alias, bool inSubquery, int ensureTupleSize, TScopedStatePtr scoped) {
    return new TSubqueryNode(std::move(source), alias, inSubquery, ensureTupleSize, scoped);
}

class TYqlSubqueryNode final: public INode {
public:
    TYqlSubqueryNode(TNodePtr source, TString alias)
        : INode(source->GetPos())
        , Source_(std::move(source))
        , Alias_(std::move(alias))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        TBlocks dependencies;
        {
            ctx.PushCurrentBlocks(&dependencies);
            Y_DEFER {
                ctx.PopCurrentBlocks();
            };

            if (!Source_->Init(ctx, src)) {
                return false;
            }
        }

        TNodePtr block = Y();
        for (TNodePtr& dependency : dependencies) {
            block->Add(std::move(dependency));
        }
        block->Add(Y("return", Q(Y("world", Source_))));

        Node_ = Y("let", Alias_, Y("block", Q(std::move(block))));
        IsUsed_ = true;
        return true;
    }

    TAstNode* Translate(TContext& ctx) const final {
        return Node_->Translate(ctx);
    }

    TNodePtr DoClone() const final {
        return new TYqlSubqueryNode(Source_->Clone(), Alias_);
    }

    // Is used at the TYqlProgramNode
    const TString* SubqueryAlias() const final {
        return &Alias_;
    }

    // Is used at the TYqlProgramNode
    bool UsedSubquery() const final {
        return IsUsed_;
    }

private:
    TNodePtr Source_;
    TString Alias_;
    bool IsUsed_ = false;

    TNodePtr Node_;
};

TNodePtr BuildYqlSubquery(TNodePtr source, TString alias) {
    return new TYqlSubqueryNode(std::move(source), std::move(alias));
}

class TSourceNode: public INode {
public:
    TSourceNode(TPosition pos, TSourcePtr&& source, bool checkExist, bool withTables, bool isInlineScalar)
        : INode(pos)
        , Source_(std::move(source))
        , CheckExist_(checkExist)
        , WithTables_(withTables)
        , IsInlineScalar_(isInlineScalar)
    {
    }

    ISource* GetSource() override {
        return Source_.Get();
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (IsInlineScalar_ &&
            !ctx.EnsureBackwardCompatibleFeatureAvailable(
                Source_->GetPos(),
                "Inline subquery",
                MakeLangVersion(2025, 04)))
        {
            return false;
        }

        if (AsInner_) {
            Source_->UseAsInner();
        }
        if (!Source_->Init(ctx, src)) {
            return false;
        }
        Node_ = Source_->Build(ctx);
        if (!Node_) {
            return false;
        }
        if (src) {
            if (IsSubquery()) {
                /// should be not used?
                auto columnsPtr = Source_->GetColumns();
                if (columnsPtr && (columnsPtr->All || columnsPtr->QualifiedAll || columnsPtr->List.size() == 1)) {
                    Node_ = Y("SingleMember", Y("SqlAccess", Q("dict"), Y("Take", Node_, Y("Uint64", Q("1"))), Y("Uint64", Q("0"))));
                } else {
                    ctx.Error(Pos_) << "Source used in expression should contain one concrete column";
                    if (RefPos_) {
                        ctx.Error(*RefPos_) << "Source is used here";
                    }

                    return false;
                }
            }
            src->AddDependentSource(Source_);
        }
        if (Node_ && WithTables_) {
            TTableList tableList;
            Source_->GetInputTables(tableList);

            TNodePtr inputTables(BuildInputTables(ctx.Pos(), tableList, IsSubquery(), ctx.Scoped));
            if (!inputTables->Init(ctx, Source_.Get())) {
                return false;
            }

            auto blockContent = inputTables;
            blockContent = L(blockContent, Y("return", Node_));
            Node_ = Y("block", Q(blockContent));
        }

        return true;
    }

    bool IsSubquery() const {
        return !AsInner_ && Source_->IsSelect() && !CheckExist_;
    }

    void DoUpdateState() const override {
        State_.Set(ENodeState::Const, IsSubquery());
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node_);
        return Node_->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TSourceNode(Pos_, Source_->CloneSource(), CheckExist_, WithTables_, IsInlineScalar_);
    }

protected:
    TSourcePtr Source_;
    TNodePtr Node_;
    bool CheckExist_;
    bool WithTables_;
    bool IsInlineScalar_;
};

TNodePtr BuildSourceNode(TPosition pos, TSourcePtr source, bool checkExist, bool withTables, bool isInlineScalar) {
    return new TSourceNode(pos, std::move(source), checkExist, withTables, isInlineScalar);
}

class TFakeSource: public ISource {
public:
    TFakeSource(TPosition pos, bool missingFrom, bool inSubquery)
        : ISource(pos)
        , MissingFrom_(missingFrom)
        , InSubquery_(inSubquery)
    {
    }

    bool IsFake() const override {
        return true;
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        // TODO: fix column reference scope - with proper scopes error below should happen earlier
        if (column.CanBeType()) {
            return true;
        }
        ctx.Error(Pos_) << (MissingFrom_ ? "Column references are not allowed without FROM" : "Source does not allow column references");
        ctx.Error(column.GetPos()) << "Column reference "
                                   << (column.GetColumnName() ? "'" + *column.GetColumnName() + "'" : "(expr)");
        return {};
    }

    bool AddFilter(TContext& ctx, TNodePtr filter) override {
        Y_UNUSED(filter);
        auto pos = filter ? filter->GetPos() : Pos_;
        ctx.Error(pos) << (MissingFrom_ ? "Filtering is not allowed without FROM" : "Source does not allow filtering");
        return false;
    }

    TNodePtr Build(TContext& ctx) override {
        Y_UNUSED(ctx);
        auto ret = Y("AsList", Y("AsStruct"));
        if (InSubquery_) {
            return Y("WithWorld", ret, "world");
        } else {
            return ret;
        }
    }

    bool AddGroupKey(TContext& ctx, const TString& column) override {
        Y_UNUSED(column);
        ctx.Error(Pos_) << "Grouping is not allowed " << (MissingFrom_ ? "without FROM" : "in this context");
        return false;
    }

    bool AddAggregation(TContext& ctx, TAggregationPtr aggr) override {
        YQL_ENSURE(aggr);
        ctx.Error(aggr->GetPos()) << "Aggregation is not allowed " << (MissingFrom_ ? "without FROM" : "in this context");
        return false;
    }

    bool AddAggregationOverWindow(TContext& ctx, const TString& windowName, TAggregationPtr func) override {
        Y_UNUSED(windowName);
        YQL_ENSURE(func);
        ctx.Error(func->GetPos()) << "Aggregation is not allowed " << (MissingFrom_ ? "without FROM" : "in this context");
        return false;
    }

    bool AddFuncOverWindow(TContext& ctx, const TString& windowName, TNodePtr func) override {
        Y_UNUSED(windowName);
        YQL_ENSURE(func);
        ctx.Error(func->GetPos()) << "Window functions are not allowed " << (MissingFrom_ ? "without FROM" : "in this context");
        return false;
    }

    TWindowSpecificationPtr FindWindowSpecification(TContext& ctx, const TString& windowName) const override {
        Y_UNUSED(windowName);
        ctx.Error(Pos_) << "Window and aggregation functions are not allowed " << (MissingFrom_ ? "without FROM" : "in this context");
        return {};
    }

    bool IsGroupByColumn(const TString& column) const override {
        Y_UNUSED(column);
        return false;
    }

    TNodePtr BuildFilter(TContext& ctx, const TString& label) override {
        Y_UNUSED(ctx);
        Y_UNUSED(label);
        return nullptr;
    }

    std::pair<TNodePtr, bool> BuildAggregation(const TString& label, TContext& ctx) override {
        Y_UNUSED(label);
        Y_UNUSED(ctx);
        return {nullptr, true};
    }

    TPtr DoClone() const final {
        return new TFakeSource(Pos_, MissingFrom_, InSubquery_);
    }

private:
    const bool MissingFrom_;
    const bool InSubquery_;
};

TSourcePtr BuildFakeSource(TPosition pos, bool missingFrom, bool inSubquery) {
    return new TFakeSource(pos, missingFrom, inSubquery);
}

class TNodeSource: public ISource {
public:
    TNodeSource(TPosition pos, const TNodePtr& node, bool wrapToList, bool wrapByTableSource)
        : ISource(pos)
        , Node_(node)
        , WrapToList_(wrapToList)
        , WrapByTableSource_(wrapByTableSource)
    {
        YQL_ENSURE(Node_);
        FakeSource_ = BuildFakeSource(pos);
    }

    bool ShouldUseSourceAsColumn(const TString& source) const final {
        return source && source != GetLabel();
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) final {
        Y_UNUSED(ctx);
        Y_UNUSED(column);
        return true;
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!Node_->Init(ctx, FakeSource_.Get())) {
            return false;
        }
        return ISource::DoInit(ctx, src);
    }

    TNodePtr Build(TContext& /*ctx*/) final {
        auto nodeAst = AstNode(Node_);
        if (WrapToList_) {
            nodeAst = Y("ToList", nodeAst);
        }

        if (WrapByTableSource_) {
            nodeAst = Y("TableSource", nodeAst);
        }

        return nodeAst;
    }

    TPtr DoClone() const final {
        return new TNodeSource(Pos_, SafeClone(Node_), WrapToList_, WrapByTableSource_);
    }

private:
    TNodePtr Node_;
    const bool WrapToList_;
    const bool WrapByTableSource_;
    TSourcePtr FakeSource_;
};

TSourcePtr BuildNodeSource(TPosition pos, const TNodePtr& node, bool wrapToList, bool wrapByTableSource) {
    return new TNodeSource(pos, node, wrapToList, wrapByTableSource);
}

class IProxySource: public ISource {
protected:
    IProxySource(TPosition pos, ISource* src)
        : ISource(pos)
        , Source_(src)
    {
    }

    void AllColumns() override {
        Y_DEBUG_ABORT_UNLESS(Source_);
        return Source_->AllColumns();
    }

    const TColumns* GetColumns() const override {
        Y_DEBUG_ABORT_UNLESS(Source_);
        return Source_->GetColumns();
    }

    void GetInputTables(TTableList& tableList) const override {
        if (Source_) {
            Source_->GetInputTables(tableList);
        }

        ISource::GetInputTables(tableList);
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        Y_DEBUG_ABORT_UNLESS(Source_);
        const TString label(Source_->GetLabel());
        Source_->SetLabel(Label_);
        const auto ret = Source_->AddColumn(ctx, column);
        Source_->SetLabel(label);
        return ret;
    }

    bool ShouldUseSourceAsColumn(const TString& source) const override {
        return Source_->ShouldUseSourceAsColumn(source);
    }

    bool IsStream() const override {
        Y_DEBUG_ABORT_UNLESS(Source_);
        return Source_->IsStream();
    }

    EOrderKind GetOrderKind() const override {
        Y_DEBUG_ABORT_UNLESS(Source_);
        return Source_->GetOrderKind();
    }

    TWriteSettings GetWriteSettings() const override {
        Y_DEBUG_ABORT_UNLESS(Source_);
        return Source_->GetWriteSettings();
    }

protected:
    void SetSource(ISource* source) {
        Source_ = source;
    }

    ISource* Source_;
};

class IRealSource: public ISource {
protected:
    explicit IRealSource(TPosition pos)
        : ISource(pos)
    {
    }

    void AllColumns() override {
        Columns_.SetAll();
    }

    const TColumns* GetColumns() const override {
        return &Columns_;
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        const auto& label = *column.GetSourceName();
        const auto& source = GetLabel();
        if (!label.empty() && label != source && !(source.StartsWith(label) && source[label.size()] == ':')) {
            if (column.IsReliable()) {
                ctx.Error(column.GetPos()) << "Unknown correlation name: " << label;
            }
            return {};
        }
        if (column.IsAsterisk()) {
            return true;
        }
        const auto* name = column.GetColumnName();
        if (name && !column.CanBeType() && !Columns_.IsColumnPossible(ctx, *name) && !IsAlias(EExprSeat::GroupBy, *name) && !IsAlias(EExprSeat::DistinctAggr, *name)) {
            if (column.IsReliable()) {
                TStringBuilder sb;
                sb << "Column " << *name << " is not in source column set";
                if (const auto mistype = FindColumnMistype(*name)) {
                    sb << ". Did you mean " << mistype.GetRef() << "?";
                }
                ctx.Error(column.GetPos()) << sb;
            }
            return {};
        }
        return true;
    }

    TMaybe<TString> FindColumnMistype(const TString& name) const override {
        auto result = FindMistypeIn(Columns_.Real, name);
        if (!result) {
            auto result = FindMistypeIn(Columns_.Artificial, name);
        }
        return result ? result : ISource::FindColumnMistype(name);
    }

protected:
    TColumns Columns_;
};

class IComposableSource: private TNonCopyable {
public:
    virtual ~IComposableSource() = default;
    virtual void BuildProjectWindowDistinct(TNodePtr& blocks, TContext& ctx, bool ordered) = 0;
};

using TComposableSourcePtr = TIntrusivePtr<IComposableSource>;

class TMuxSource: public ISource {
public:
    TMuxSource(TPosition pos, TVector<TSourcePtr>&& sources)
        : ISource(pos)
        , Sources_(std::move(sources))
    {
        YQL_ENSURE(Sources_.size() > 1);
    }

    void AllColumns() final {
        for (auto& source : Sources_) {
            source->AllColumns();
        }
    }

    const TColumns* GetColumns() const final {
        // Columns are equal in all sources. Return from the first one
        return Sources_.front()->GetColumns();
    }

    void GetInputTables(TTableList& tableList) const final {
        for (auto& source : Sources_) {
            source->GetInputTables(tableList);
        }
        ISource::GetInputTables(tableList);
    }

    bool IsStream() const final {
        return AnyOf(Sources_, [](const TSourcePtr& s) { return s->IsStream(); });
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        for (auto& source : Sources_) {
            if (AsInner_) {
                source->UseAsInner();
            }

            if (src) {
                src->AddDependentSource(source);
            }
            if (!source->Init(ctx, src)) {
                return false;
            }
            if (!source->InitFilters(ctx)) {
                return false;
            }
        }
        return true;
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) final {
        for (auto& source : Sources_) {
            if (!source->AddColumn(ctx, column)) {
                return {};
            }
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) final {
        TNodePtr block;
        auto muxArgs = Y();
        for (size_t i = 0; i < Sources_.size(); ++i) {
            auto& source = Sources_[i];
            auto input = source->Build(ctx);
            auto ref = ctx.MakeName("src");
            muxArgs->Add(ref);
            if (block) {
                block = L(block, Y("let", ref, input));
            } else {
                block = Y(Y("let", ref, input));
            }
            auto filter = source->BuildFilter(ctx, ref);
            if (filter) {
                block = L(block, Y("let", ref, filter));
            }
            if (ctx.EnableSystemColumns) {
                block = L(block, Y("let", ref, Y("RemoveSystemMembers", ref)));
            }
        }
        return GroundWithExpr(block, Y("Mux", Q(muxArgs)));
    }

    bool AddFilter(TContext& ctx, TNodePtr filter) final {
        Y_UNUSED(filter);
        ctx.Error() << "Filter is not allowed for multiple sources";
        return false;
    }

    TPtr DoClone() const final {
        return new TMuxSource(Pos_, CloneContainer(Sources_));
    }

protected:
    TVector<TSourcePtr> Sources_;
};

TSourcePtr BuildMuxSource(TPosition pos, TVector<TSourcePtr>&& sources) {
    return new TMuxSource(pos, std::move(sources));
}

class TSubqueryRefNode: public IRealSource {
public:
    TSubqueryRefNode(const TNodePtr& subquery, const TString& alias, int tupleIndex)
        : IRealSource(subquery->GetPos())
        , Subquery_(subquery)
        , Alias_(alias)
        , TupleIndex_(tupleIndex)
    {
        YQL_ENSURE(subquery->GetSource());
    }

    ISource* GetSource() override {
        return this;
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        // independent subquery should not connect source
        Subquery_->UseAsInner();
        if (!Subquery_->Init(ctx, nullptr)) {
            return false;
        }
        Columns_ = *Subquery_->GetSource()->GetColumns();
        Node_ = BuildAtom(Pos_, Alias_, TNodeFlags::Default);
        if (TupleIndex_ != -1) {
            Node_ = Y("Nth", Node_, Q(ToString(TupleIndex_)));
        }
        if (!Node_->Init(ctx, src)) {
            return false;
        }
        if (src && Subquery_->GetSource()->IsSelect()) {
            auto columnsPtr = &Columns_;
            if (columnsPtr && (columnsPtr->All || columnsPtr->QualifiedAll || columnsPtr->List.size() == 1)) {
                Node_ = Y("SingleMember", Y("SqlAccess", Q("dict"), Y("Take", Node_, Y("Uint64", Q("1"))), Y("Uint64", Q("0"))));
            } else {
                ctx.Error(Pos_) << "Source used in expression should contain one concrete column";
                if (RefPos_) {
                    ctx.Error(*RefPos_) << "Source is used here";
                }

                return false;
            }
        }
        TNodePtr sample;
        if (!BuildSamplingLambda(sample)) {
            return false;
        } else if (sample) {
            Node_ = Y("block", Q(Y(Y("let", Node_, Y("OrderedFlatMap", Node_, sample)), Y("return", Node_))));
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        Y_UNUSED(ctx);
        return Node_;
    }

    bool SetSamplingOptions(
        TContext& ctx,
        TPosition pos,
        ESampleClause sampleClause,
        ESampleMode mode,
        TNodePtr samplingRate,
        TNodePtr samplingSeed) override {
        if (mode == ESampleMode::System) {
            ctx.Error(pos) << "only Bernoulli sampling mode is supported for subqueries";
            return false;
        }
        if (samplingSeed) {
            ctx.Error(pos) << "'Repeatable' keyword is not supported for subqueries";
            return false;
        }
        return SetSamplingRate(ctx, sampleClause, samplingRate);
    }

    bool IsStream() const override {
        return Subquery_->GetSource()->IsStream();
    }

    void DoUpdateState() const override {
        State_.Set(ENodeState::Const, true);
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node_);
        return Node_->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TSubqueryRefNode(Subquery_, Alias_, TupleIndex_);
    }

protected:
    TNodePtr Subquery_;
    const TString Alias_;
    const int TupleIndex_;
    TNodePtr Node_;
};

TNodePtr BuildSubqueryRef(TNodePtr subquery, const TString& alias, int tupleIndex) {
    return new TSubqueryRefNode(std::move(subquery), alias, tupleIndex);
}

bool IsSubqueryRef(const TSourcePtr& source) {
    return dynamic_cast<const TSubqueryRefNode*>(source.Get()) != nullptr;
}

class TYqlSubqueryRefNode final: public INode {
public:
    TYqlSubqueryRefNode(TNodePtr subquery, TString ref)
        : INode(subquery->GetPos())
        , Subquery_(std::move(subquery))
        , Ref_(std::move(ref))
    {
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!Subquery_->Init(ctx, nullptr)) {
            return false;
        }

        Node_ = BuildAtom(Pos_, Ref_, TNodeFlags::Default);
        if (!Node_->Init(ctx, src)) {
            return false;
        }

        return true;
    }

    TAstNode* Translate(TContext& ctx) const final {
        return Node_->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TYqlSubqueryRefNode(Subquery_, Ref_);
    }

private:
    TNodePtr Subquery_;
    TString Ref_;

    TNodePtr Node_;
};

TNodePtr BuildYqlSubqueryRef(TNodePtr subquery, TString ref) {
    return new TYqlSubqueryRefNode(std::move(subquery), ref);
}

bool IsYqlSubqueryRef(const TNodePtr& source) {
    return dynamic_cast<const TYqlSubqueryRefNode*>(source.Get()) != nullptr;
}

class TInvalidSubqueryRefNode: public ISource {
public:
    explicit TInvalidSubqueryRefNode(TPosition pos)
        : ISource(pos)
        , Pos_(pos)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        ctx.Error(Pos_) << "Named subquery can not be used as a top level statement in libraries";
        return false;
    }

    TNodePtr Build(TContext& ctx) override {
        Y_UNUSED(ctx);
        return {};
    }

    TPtr DoClone() const final {
        return new TInvalidSubqueryRefNode(Pos_);
    }

protected:
    const TPosition Pos_;
};

TNodePtr BuildInvalidSubqueryRef(TPosition subqueryPos) {
    return new TInvalidSubqueryRefNode(subqueryPos);
}

class TTableSource: public IRealSource {
public:
    TTableSource(TPosition pos, const TTableRef& table, const TString& label)
        : IRealSource(pos)
        , Table_(table)
        , FakeSource_(BuildFakeSource(pos))
    {
        SetLabel(label.empty() ? Table_.ShortName() : label);
    }

    void GetInputTables(TTableList& tableList) const override {
        tableList.push_back(Table_);
        ISource::GetInputTables(tableList);
    }

    bool ShouldUseSourceAsColumn(const TString& source) const override {
        const auto& label = GetLabel();
        return source && source != label && !(label.StartsWith(source) && label[source.size()] == ':');
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        Columns_.Add(column.GetColumnName(), column.GetCountHint(), column.IsArtificial(), column.IsReliable());
        if (!IRealSource::AddColumn(ctx, column)) {
            return {};
        }
        return false;
    }

    bool SetSamplingOptions(
        TContext& ctx,
        TPosition pos,
        ESampleClause sampleClause,
        ESampleMode mode,
        TNodePtr samplingRate,
        TNodePtr samplingSeed) override {
        Y_UNUSED(pos);
        TString modeName;
        if (!samplingSeed) {
            samplingSeed = Y("Int32", Q("0"));
        }
        if (ESampleClause::Sample == sampleClause) {
            YQL_ENSURE(ESampleMode::Bernoulli == mode, "Internal logic error");
        }
        switch (mode) {
            case ESampleMode::Bernoulli:
                modeName = "bernoulli";
                break;
            case ESampleMode::System:
                modeName = "system";
                break;
        }

        if (!samplingRate->Init(ctx, FakeSource_.Get())) {
            return false;
        }

        samplingRate = PrepareSamplingRate(pos, sampleClause, samplingRate);

        auto sampleSettings = Q(Y(Q(modeName), Y("EvaluateAtom", Y("ToString", samplingRate)), Y("EvaluateAtom", Y("ToString", samplingSeed))));
        auto sampleOption = Q(Y(Q("sample"), sampleSettings));
        if (Table_.Options) {
            if (!Table_.Options->Init(ctx, this)) {
                return false;
            }
            Table_.Options = L(Table_.Options, sampleOption);
        } else {
            Table_.Options = Y(sampleOption);
        }
        return true;
    }

    bool SetTableHints(TContext& ctx, TPosition pos, const TTableHints& hints, const TTableHints& contextHints) override {
        Y_UNUSED(ctx);
        TTableHints merged = contextHints;
        MergeHints(merged, hints);
        Table_.Options = BuildInputOptions(pos, merged);
        return true;
    }

    bool SetViewName(TContext& ctx, TPosition pos, const TString& view) override {
        return Table_.Keys->SetViewName(ctx, pos, view);
    }

    TNodePtr Build(TContext& ctx) override {
        if (!Table_.Keys->Init(ctx, nullptr)) {
            return nullptr;
        }
        return AstNode(Table_.RefName);
    }

    bool IsStream() const override {
        return IsStreamingService(Table_.Service);
    }

    TPtr DoClone() const final {
        return new TTableSource(Pos_, Table_, GetLabel());
    }

    bool IsTableSource() const override {
        return true;
    }

protected:
    TTableRef Table_;

private:
    const TSourcePtr FakeSource_;
};

TSourcePtr BuildTableSource(TPosition pos, const TTableRef& table, const TString& label) {
    return new TTableSource(pos, table, label);
}

class TInnerSource: public IProxySource {
public:
    TInnerSource(TPosition pos, TNodePtr node, const TString& service, const TDeferredAtom& cluster, const TString& label)
        : IProxySource(pos, nullptr)
        , Node_(node)
        , Service_(service)
        , Cluster_(cluster)
    {
        SetLabel(label);
    }

    bool SetSamplingOptions(TContext& ctx, TPosition pos, ESampleClause sampleClause, ESampleMode mode, TNodePtr samplingRate, TNodePtr samplingSeed) override {
        Y_UNUSED(ctx);
        SamplingPos_ = pos;
        SamplingClause_ = sampleClause;
        SamplingMode_ = mode;
        SamplingRate_ = samplingRate;
        SamplingSeed_ = samplingSeed;
        return true;
    }

    bool SetTableHints(TContext& ctx, TPosition pos, const TTableHints& hints, const TTableHints& contextHints) override {
        Y_UNUSED(ctx);
        HintsPos_ = pos;
        Hints_ = hints;
        ContextHints_ = contextHints;
        return true;
    }

    bool SetViewName(TContext& ctx, TPosition pos, const TString& view) override {
        Y_UNUSED(ctx);
        ViewPos_ = pos;
        View_ = view;
        return true;
    }

    bool ShouldUseSourceAsColumn(const TString& source) const override {
        return source && source != GetLabel();
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        if (const TString* columnName = column.GetColumnName()) {
            if (columnName && IsExprAlias(*columnName)) {
                return true;
            }
        }
        return IProxySource::AddColumn(ctx, column);
    }

    bool DoInit(TContext& ctx, ISource* initSrc) override {
        Y_UNUSED(initSrc);
        auto source = Node_->GetSource();
        if (!source) {
            NewSource_ = TryMakeSourceFromExpression(Pos_, ctx, Service_, Cluster_, Node_);
            source = NewSource_.Get();
        }

        if (!source) {
            ctx.Error(Pos_) << "Invalid inner source node";
            return false;
        }

        if (SamplingPos_) {
            if (!source->SetSamplingOptions(ctx, *SamplingPos_, SamplingClause_, SamplingMode_, SamplingRate_, SamplingSeed_)) {
                return false;
            }
        }

        if (ViewPos_) {
            if (!source->SetViewName(ctx, *ViewPos_, View_)) {
                return false;
            }
        }

        if (HintsPos_) {
            if (!source->SetTableHints(ctx, *HintsPos_, Hints_, ContextHints_)) {
                return false;
            }
        }

        source->SetLabel(Label_);
        if (!NewSource_) {
            Node_->UseAsInner();
            if (!Node_->Init(ctx, nullptr)) {
                return false;
            }
        }

        SetSource(source);
        if (NewSource_ && !NewSource_->Init(ctx, nullptr)) {
            return false;
        }

        return ISource::DoInit(ctx, source);
    }

    TNodePtr Build(TContext& ctx) override {
        Y_UNUSED(ctx);
        return NewSource_ ? NewSource_->Build(ctx) : Node_;
    }

    bool IsStream() const override {
        auto source = Node_->GetSource();
        if (source) {
            return source->IsStream();
        }
        // NewSource will be built later in DoInit->TryMakeSourceFromExpression
        // where Service will be used in all situations
        // let's detect IsStream by Service value
        return IsStreamingService(Service_);
    }

    TPtr DoClone() const final {
        return new TInnerSource(Pos_, SafeClone(Node_), Service_, Cluster_, GetLabel());
    }

protected:
    TNodePtr Node_;
    TString Service_;
    TDeferredAtom Cluster_;
    TSourcePtr NewSource_;

private:
    TMaybe<TPosition> SamplingPos_;
    ESampleClause SamplingClause_;
    ESampleMode SamplingMode_;
    TNodePtr SamplingRate_;
    TNodePtr SamplingSeed_;

    TMaybe<TPosition> ViewPos_;
    TString View_;

    TMaybe<TPosition> HintsPos_;
    TTableHints Hints_;
    TTableHints ContextHints_;
};

TSourcePtr BuildInnerSource(TPosition pos, TNodePtr node, const TString& service, const TDeferredAtom& cluster, const TString& label) {
    return new TInnerSource(pos, node, service, cluster, label);
}

namespace {

bool IsComparableExpression(TContext& ctx, const TNodePtr& expr, bool assume, const char* sqlConstruction) {
    if (assume && !expr->IsPlainColumn()) {
        ctx.Error(expr->GetPos()) << "Only column names can be used in " << sqlConstruction;
        return false;
    }

    if (expr->IsConstant()) {
        ctx.Error(expr->GetPos()) << "Unable to " << sqlConstruction << " constant expression";
        return false;
    }
    if (expr->IsAggregated() && !expr->HasState(ENodeState::AggregationKey)) {
        ctx.Error(expr->GetPos()) << "Unable to " << sqlConstruction << " aggregated values";
        return false;
    }
    if (expr->IsPlainColumn()) {
        return true;
    }
    if (expr->GetOpName().empty()) {
        ctx.Error(expr->GetPos()) << "You should use in " << sqlConstruction << " column name, qualified field, callable function or expression";
        return false;
    }
    return true;
}

} // namespace

/// \todo move to reduce.cpp? or mapreduce.cpp?
class TReduceSource: public IRealSource {
public:
    TReduceSource(TPosition pos,
                  EReduceMode mode,
                  TSourcePtr source,
                  TVector<TSortSpecificationPtr>&& orderBy,
                  TVector<TNodePtr>&& keys,
                  TVector<TNodePtr>&& args,
                  TNodePtr udf,
                  TNodePtr having,
                  const TWriteSettings& settings,
                  const TVector<TSortSpecificationPtr>& assumeOrderBy,
                  bool listCall)
        : IRealSource(pos)
        , Mode_(mode)
        , Source_(std::move(source))
        , OrderBy_(std::move(orderBy))
        , Keys_(std::move(keys))
        , Args_(std::move(args))
        , Udf_(udf)
        , Having_(having)
        , Settings_(settings)
        , AssumeOrderBy_(assumeOrderBy)
        , ListCall_(listCall)
    {
        YQL_ENSURE(!Keys_.empty());
        YQL_ENSURE(Source_);
    }

    void GetInputTables(TTableList& tableList) const override {
        Source_->GetInputTables(tableList);
        ISource::GetInputTables(tableList);
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (AsInner_) {
            Source_->UseAsInner();
        }

        YQL_ENSURE(!src);
        if (!Source_->Init(ctx, src)) {
            return false;
        }
        if (!Source_->InitFilters(ctx)) {
            return false;
        }
        src = Source_.Get();
        for (auto& key : Keys_) {
            if (!key->Init(ctx, src)) {
                return false;
            }
            auto keyNamePtr = key->GetColumnName();
            YQL_ENSURE(keyNamePtr);
            if (!src->AddGroupKey(ctx, *keyNamePtr)) {
                return false;
            }
        }
        if (Having_ && !Having_->Init(ctx, nullptr)) {
            return false;
        }

        /// SIN: verify reduce one argument
        if (Args_.size() != 1) {
            ctx.Error(Pos_) << "REDUCE requires exactly one UDF argument";
            return false;
        }
        if (!Args_[0]->Init(ctx, src)) {
            return false;
        }

        for (auto orderSpec : OrderBy_) {
            if (!orderSpec->OrderExpr->Init(ctx, src)) {
                return false;
            }
        }

        if (!Udf_->Init(ctx, src)) {
            return false;
        }

        if (Udf_->GetLabel().empty()) {
            Columns_.SetAll();
        } else {
            Columns_.Add(&Udf_->GetLabel(), false);
        }

        const auto label = GetLabel();
        for (const auto& sortSpec : AssumeOrderBy_) {
            auto& expr = sortSpec->OrderExpr;
            SetLabel(Source_->GetLabel());
            if (!expr->Init(ctx, this)) {
                return false;
            }
            if (!IsComparableExpression(ctx, expr, true, "ASSUME ORDER BY")) {
                return false;
            }
        }
        SetLabel(label);

        return true;
    }

    TNodePtr Build(TContext& ctx) final {
        auto input = Source_->Build(ctx);
        if (!input) {
            return nullptr;
        }

        auto keysTuple = Y();
        if (Keys_.size() == 1) {
            keysTuple = Y("Member", "row", BuildQuotedAtom(Pos_, *Keys_.back()->GetColumnName()));
        } else {
            for (const auto& key : Keys_) {
                keysTuple = L(keysTuple, Y("Member", "row", BuildQuotedAtom(Pos_, *key->GetColumnName())));
            }
            keysTuple = Q(keysTuple);
        }
        auto extractKey = Y("SqlExtractKey", "row", BuildLambda(Pos_, Y("row"), keysTuple));
        auto extractKeyLambda = BuildLambda(Pos_, Y("row"), extractKey);

        TNodePtr processPartitions;
        if (ListCall_) {
            if (Mode_ != EReduceMode::ByAll) {
                ctx.Error(Pos_) << "TableRows() must be used only with USING ALL";
                return nullptr;
            }

            TNodePtr expr = BuildAtom(Pos_, "partitionStream");
            processPartitions = Y("SqlReduce", "partitionStream", BuildQuotedAtom(Pos_, "byAllList", TNodeFlags::Default), Udf_, expr);
        } else {
            switch (Mode_) {
                case EReduceMode::ByAll: {
                    auto columnPtr = Args_[0]->GetColumnName();
                    TNodePtr expr = BuildAtom(Pos_, "partitionStream");
                    if (!columnPtr || *columnPtr != "*") {
                        expr = Y("Map", "partitionStream", BuildLambda(Pos_, Y("keyPair"), Q(L(Y(),
                                                                                               Y("Nth", "keyPair", Q(ToString("0"))),
                                                                                               Y("Map", Y("Nth", "keyPair", Q(ToString("1"))), BuildLambda(Pos_, Y("row"), Args_[0]))))));
                    }
                    processPartitions = Y("SqlReduce", "partitionStream", BuildQuotedAtom(Pos_, "byAll", TNodeFlags::Default), Udf_, expr);
                    break;
                }
                case EReduceMode::ByPartition: {
                    processPartitions = Y("SqlReduce", "partitionStream", extractKeyLambda, Udf_,
                                          BuildLambda(Pos_, Y("row"), Args_[0]));
                    break;
                }
            }
        }

        TNodePtr sortDirection;
        TNodePtr sortKeySelector;
        FillSortParts(OrderBy_, sortDirection, sortKeySelector);
        if (!OrderBy_.empty()) {
            sortKeySelector = BuildLambda(Pos_, Y("row"), Y("SqlExtractKey", "row", sortKeySelector));
        }

        auto partitionByKey = Y(!ListCall_ && Mode_ == EReduceMode::ByAll ? "PartitionByKey" : "PartitionsByKeys", "core", extractKeyLambda,
                                sortDirection, sortKeySelector, BuildLambda(Pos_, Y("partitionStream"), processPartitions));

        auto inputLabel = ListCall_ ? "inputRowsList" : "core";
        auto block(Y(Y("let", inputLabel, input)));
        auto filter = Source_->BuildFilter(ctx, inputLabel);
        if (filter) {
            block = L(block, Y("let", inputLabel, filter));
        }
        if (ListCall_) {
            block = L(block, Y("let", "core", "inputRowsList"));
        }

        if (ctx.EnableSystemColumns) {
            block = L(block, Y("let", "core", Y("RemoveSystemMembers", "core")));
        }
        block = L(block, Y("let", "core", Y("AutoDemux", partitionByKey)));
        if (Having_) {
            block = L(block, Y("let", "core",
                               Y("Filter", "core", BuildLambda(Pos_, Y("row"), Y("Coalesce", Having_, Y("Bool", Q("false")))))));
        }
        return Y("block", Q(L(block, Y("return", "core"))));
    }

    TNodePtr BuildSort(TContext& ctx, const TString& label) override {
        Y_UNUSED(ctx);
        if (AssumeOrderBy_.empty()) {
            return nullptr;
        }

        return Y("let", label, BuildSortSpec(AssumeOrderBy_, label, false, true));
    }

    EOrderKind GetOrderKind() const override {
        return AssumeOrderBy_.empty() ? EOrderKind::None : EOrderKind::Assume;
    }

    TWriteSettings GetWriteSettings() const final {
        return Settings_;
    }

    bool HasSelectResult() const final {
        return !Settings_.Discard;
    }

    TPtr DoClone() const final {
        return new TReduceSource(Pos_, Mode_, Source_->CloneSource(), CloneContainer(OrderBy_),
                                 CloneContainer(Keys_), CloneContainer(Args_), SafeClone(Udf_), SafeClone(Having_), Settings_,
                                 CloneContainer(AssumeOrderBy_), ListCall_);
    }

private:
    EReduceMode Mode_;
    TSourcePtr Source_;
    TVector<TSortSpecificationPtr> OrderBy_;
    TVector<TNodePtr> Keys_;
    TVector<TNodePtr> Args_;
    TNodePtr Udf_;
    TNodePtr Having_;
    const TWriteSettings Settings_;
    TVector<TSortSpecificationPtr> AssumeOrderBy_;
    const bool ListCall_;
};

TSourcePtr BuildReduce(TPosition pos,
                       EReduceMode mode,
                       TSourcePtr source,
                       TVector<TSortSpecificationPtr>&& orderBy,
                       TVector<TNodePtr>&& keys,
                       TVector<TNodePtr>&& args,
                       TNodePtr udf,
                       TNodePtr having,
                       const TWriteSettings& settings,
                       const TVector<TSortSpecificationPtr>& assumeOrderBy,
                       bool listCall) {
    return new TReduceSource(pos, mode, std::move(source), std::move(orderBy), std::move(keys),
                             std::move(args), udf, having, settings, assumeOrderBy, listCall);
}

namespace {

bool InitAndGetGroupKey(TContext& ctx, const TNodePtr& expr, ISource* src, TStringBuf where, TString& keyColumn) {
    keyColumn.clear();

    YQL_ENSURE(src);
    const bool isJoin = src->GetJoin();

    if (!expr->Init(ctx, src)) {
        return false;
    }

    auto keyNamePtr = expr->GetColumnName();
    if (keyNamePtr && expr->GetLabel().empty()) {
        keyColumn = *keyNamePtr;
        auto sourceNamePtr = expr->GetSourceName();
        auto columnNode = expr->GetColumnNode();
        if (isJoin && (!columnNode || !columnNode->IsArtificial())) {
            if (!sourceNamePtr || sourceNamePtr->empty()) {
                if (!src->IsAlias(EExprSeat::GroupBy, keyColumn)) {
                    ctx.Error(expr->GetPos()) << "Columns in " << where << " should have correlation name, error in key: " << keyColumn;
                    return false;
                }
            } else {
                keyColumn = DotJoin(*sourceNamePtr, keyColumn);
            }
        }
    }

    return true;
}

} // namespace

class TCompositeSelect: public IRealSource {
public:
    TCompositeSelect(TPosition pos, TSourcePtr source, TSourcePtr originalSource, const TWriteSettings& settings)
        : IRealSource(pos)
        , Source_(std::move(source))
        , OriginalSource_(std::move(originalSource))
        , Settings_(settings)
    {
        YQL_ENSURE(Source_);
    }

    void SetSubselects(TVector<TSourcePtr>&& subselects, TVector<TNodePtr>&& grouping, TVector<TNodePtr>&& groupByExpr) {
        Subselects_ = std::move(subselects);
        Grouping_ = std::move(grouping);
        GroupByExpr_ = std::move(groupByExpr);
        Y_DEBUG_ABORT_UNLESS(Subselects_.size() > 1);
    }

    void GetInputTables(TTableList& tableList) const override {
        for (const auto& select : Subselects_) {
            select->GetInputTables(tableList);
        }
        ISource::GetInputTables(tableList);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (AsInner_) {
            Source_->UseAsInner();
        }

        if (src) {
            src->AddDependentSource(Source_);
        }
        if (!Source_->Init(ctx, src)) {
            return false;
        }
        if (!Source_->InitFilters(ctx)) {
            return false;
        }

        if (!CalculateGroupingCols(ctx, src)) {
            return false;
        }

        auto origSrc = OriginalSource_.Get();
        if (!origSrc->Init(ctx, src)) {
            return false;
        }

        if (origSrc->IsFlattenByColumns() || origSrc->IsFlattenColumns()) {
            Flatten_ = origSrc->IsFlattenByColumns() ? origSrc->BuildFlattenByColumns("row") : origSrc->BuildFlattenColumns("row");
            if (!Flatten_ || !Flatten_->Init(ctx, src)) {
                return false;
            }
        }

        if (origSrc->IsFlattenByExprs()) {
            for (auto& expr : static_cast<ISource const*>(origSrc)->Expressions(EExprSeat::FlattenByExpr)) {
                if (!expr->Init(ctx, origSrc)) {
                    return false;
                }
            }
            PreFlattenMap_ = origSrc->BuildPreFlattenMap(ctx);
            if (!PreFlattenMap_) {
                return false;
            }
        }

        for (const auto& select : Subselects_) {
            select->SetLabel(Label_);
            if (AsInner_) {
                select->UseAsInner();
            }

            if (!select->Init(ctx, Source_.Get())) {
                return false;
            }
        }

        TMaybe<size_t> groupingColumnsCount;
        size_t idx = 0;
        for (const auto& select : Subselects_) {
            size_t count = select->GetGroupingColumnsCount();
            if (!groupingColumnsCount.Defined()) {
                groupingColumnsCount = count;
            } else if (*groupingColumnsCount != count) {
                ctx.Error(select->GetPos()) << TStringBuilder() << "Mismatch GROUPING() column count in composite select input #"
                                            << idx << ": expected " << *groupingColumnsCount << ", got: " << count << ". Please submit bug report";
                return false;
            }
            ++idx;
        }
        return true;
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        for (const auto& select : Subselects_) {
            if (!select->AddColumn(ctx, column)) {
                return {};
            }
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        auto input = Source_->Build(ctx);
        auto block(Y(Y("let", "composite", input)));

        bool ordered = ctx.UseUnordered(*this);
        if (PreFlattenMap_) {
            block = L(block, Y("let", "composite", Y(ordered ? "OrderedFlatMap" : "FlatMap", "composite", BuildLambda(Pos_, Y("row"), PreFlattenMap_))));
        }
        if (Flatten_) {
            block = L(block, Y("let", "composite", Y(ordered ? "OrderedFlatMap" : "FlatMap", "composite", BuildLambda(Pos_, Y("row"), Flatten_, "res"))));
        }
        auto filter = Source_->BuildFilter(ctx, "composite");
        if (filter) {
            block = L(block, Y("let", "composite", filter));
        }

        TNodePtr compositeNode = Y("UnionAll");
        for (const auto& select : Subselects_) {
            YQL_ENSURE(dynamic_cast<IComposableSource*>(select.Get()));
            auto addNode = select->Build(ctx);
            if (!addNode) {
                return nullptr;
            }
            compositeNode->Add(addNode);
        }

        block = L(block, Y("let", "core", compositeNode));
        YQL_ENSURE(!Subselects_.empty());
        dynamic_cast<IComposableSource*>(Subselects_.front().Get())->BuildProjectWindowDistinct(block, ctx, false);
        return Y("block", Q(L(block, Y("return", "core"))));
    }

    bool IsGroupByColumn(const TString& column) const override {
        YQL_ENSURE(!GroupingCols_.empty());
        return GroupingCols_.contains(column);
    }

    const TSet<TString>& GetGroupingCols() const {
        return GroupingCols_;
    }

    TNodePtr BuildSort(TContext& ctx, const TString& label) override {
        return Subselects_.front()->BuildSort(ctx, label);
    }

    EOrderKind GetOrderKind() const override {
        return Subselects_.front()->GetOrderKind();
    }

    const TColumns* GetColumns() const override {
        return Subselects_.front()->GetColumns();
    }

    ISource* RealSource() const {
        return Source_.Get();
    }

    TWriteSettings GetWriteSettings() const override {
        return Settings_;
    }

    bool HasSelectResult() const override {
        return !Settings_.Discard;
    }

    TNodePtr DoClone() const final {
        auto newSource = MakeIntrusive<TCompositeSelect>(Pos_, Source_->CloneSource(), OriginalSource_->CloneSource(), Settings_);
        newSource->SetSubselects(CloneContainer(Subselects_), CloneContainer(Grouping_), CloneContainer(GroupByExpr_));
        return newSource;
    }

private:
    bool CalculateGroupingCols(TContext& ctx, ISource* initSrc) {
        auto origSrc = OriginalSource_->CloneSource();
        if (!origSrc->Init(ctx, initSrc)) {
            return false;
        }

        bool hasError = false;
        for (auto& expr : GroupByExpr_) {
            if (!expr->Init(ctx, origSrc.Get()) || !IsComparableExpression(ctx, expr, false, "GROUP BY")) {
                hasError = true;
            }
        }
        if (!origSrc->AddExpressions(ctx, GroupByExpr_, EExprSeat::GroupBy)) {
            hasError = true;
        }

        YQL_ENSURE(!Grouping_.empty());
        for (auto& grouping : Grouping_) {
            TString keyColumn;
            if (!InitAndGetGroupKey(ctx, grouping, origSrc.Get(), "grouping sets", keyColumn)) {
                hasError = true;
            } else if (!keyColumn.empty()) {
                GroupingCols_.insert(keyColumn);
            }
        }

        return !hasError;
    }

    TSourcePtr Source_;
    TSourcePtr OriginalSource_;
    TNodePtr Flatten_;
    TNodePtr PreFlattenMap_;
    const TWriteSettings Settings_;
    TVector<TSourcePtr> Subselects_;
    TVector<TNodePtr> Grouping_;
    TVector<TNodePtr> GroupByExpr_;
    TSet<TString> GroupingCols_;
};

namespace {
TString FullColumnName(const TColumnNode& column) {
    YQL_ENSURE(column.GetColumnName());
    TString columnName = *column.GetColumnName();
    if (column.IsUseSource()) {
        columnName = DotJoin(*column.GetSourceName(), columnName);
    }
    return columnName;
}
} // namespace

/// \todo simplify class
class TSelectCore: public IRealSource, public IComposableSource {
public:
    TSelectCore(
        TPosition pos,
        TSourcePtr source,
        const TVector<TNodePtr>& groupByExpr,
        const TVector<TNodePtr>& groupBy,
        bool compactGroupBy,
        const TString& groupBySuffix,
        bool assumeSorted,
        const TVector<TSortSpecificationPtr>& orderBy,
        TNodePtr having,
        const TWinSpecs& winSpecs,
        TLegacyHoppingWindowSpecPtr legacyHoppingWindowSpec,
        const TVector<TNodePtr>& terms,
        bool distinct,
        const TVector<TNodePtr>& without,
        bool forceWithout,
        bool selectStream,
        const TWriteSettings& settings,
        TColumnsSets&& uniqueSets,
        TColumnsSets&& distinctSets)
        : IRealSource(pos)
        , Source_(std::move(source))
        , GroupByExpr_(groupByExpr)
        , GroupBy_(groupBy)
        , AssumeSorted_(assumeSorted)
        , CompactGroupBy_(compactGroupBy)
        , GroupBySuffix_(groupBySuffix)
        , OrderBy_(orderBy)
        , Having_(having)
        , WinSpecs_(winSpecs)
        , Terms_(terms)
        , Without_(without)
        , ForceWithout_(forceWithout)
        , Distinct_(distinct)
        , LegacyHoppingWindowSpec_(legacyHoppingWindowSpec)
        , SelectStream_(selectStream)
        , Settings_(settings)
        , UniqueSets_(std::move(uniqueSets))
        , DistinctSets_(std::move(distinctSets))
    {
    }

    void AllColumns() override {
        if (!OrderByInit_) {
            Columns_.SetAll();
        }
    }

    void GetInputTables(TTableList& tableList) const override {
        Source_->GetInputTables(tableList);
        ISource::GetInputTables(tableList);
    }

    size_t GetGroupingColumnsCount() const override {
        return Source_->GetGroupingColumnsCount();
    }

    bool DoInit(TContext& ctx, ISource* initSrc) override {
        if (AsInner_) {
            Source_->UseAsInner();
        }

        if (!Source_->Init(ctx, initSrc)) {
            return false;
        }
        if (SelectStream_ && !Source_->IsStream()) {
            ctx.Error(Pos_) << "SELECT STREAM is unsupported for non-streaming sources";
            return false;
        }

        auto src = Source_.Get();
        bool hasError = false;

        if (src->IsFlattenByExprs()) {
            for (auto& expr : static_cast<ISource const*>(src)->Expressions(EExprSeat::FlattenByExpr)) {
                if (!expr->Init(ctx, src)) {
                    hasError = true;
                    continue;
                }
            }
        }

        if (hasError) {
            return false;
        }

        src->SetCompactGroupBy(CompactGroupBy_);
        src->SetGroupBySuffix(GroupBySuffix_);

        for (auto& term : Terms_) {
            term->CollectPreaggregateExprs(ctx, *src, DistinctAggrExpr_);
        }

        if (Having_) {
            Having_->CollectPreaggregateExprs(ctx, *src, DistinctAggrExpr_);
        }

        for (auto& expr : GroupByExpr_) {
            if (auto sessionWindow = dynamic_cast<TSessionWindow*>(expr.Get())) {
                if (Source_->IsStream()) {
                    ctx.Error(Pos_) << "SessionWindow is unsupported for streaming sources";
                    return false;
                }
                sessionWindow->MarkValid();
            }

            if (auto hoppingWindow = dynamic_cast<THoppingWindow*>(expr.Get())) {
                hoppingWindow->MarkValid();
            }

            // need to collect and Init() preaggregated exprs before calling Init() on GROUP BY expression
            TVector<TNodePtr> distinctAggrsInGroupBy;
            expr->CollectPreaggregateExprs(ctx, *src, distinctAggrsInGroupBy);
            for (auto& distinct : distinctAggrsInGroupBy) {
                if (!distinct->Init(ctx, src)) {
                    return false;
                }
            }
            DistinctAggrExpr_.insert(DistinctAggrExpr_.end(), distinctAggrsInGroupBy.begin(), distinctAggrsInGroupBy.end());

            if (!expr->Init(ctx, src) || !IsComparableExpression(ctx, expr, false, "GROUP BY")) {
                hasError = true;
            }
        }
        if (hasError || !src->AddExpressions(ctx, GroupByExpr_, EExprSeat::GroupBy)) {
            return false;
        }

        for (auto& expr : DistinctAggrExpr_) {
            if (!expr->Init(ctx, src)) {
                hasError = true;
            }
        }
        if (hasError || !src->AddExpressions(ctx, DistinctAggrExpr_, EExprSeat::DistinctAggr)) {
            return false;
        }

        /// grouped expressions are available in filters
        if (!Source_->InitFilters(ctx)) {
            return false;
        }

        for (auto& expr : GroupBy_) {
            TString usedColumn;
            if (!InitAndGetGroupKey(ctx, expr, src, "GROUP BY", usedColumn)) {
                hasError = true;
            } else if (usedColumn) {
                if (!src->AddGroupKey(ctx, usedColumn)) {
                    hasError = true;
                }
            }
        }

        if (hasError) {
            return false;
        }

        if (Having_ && !Having_->Init(ctx, src)) {
            return false;
        }
        src->AddWindowSpecs(WinSpecs_);

        const bool isJoin = Source_->GetJoin();
        if (!InitSelect(ctx, src, isJoin, hasError)) {
            return false;
        }

        src->FinishColumns();
        auto aggRes = src->BuildAggregation("core", ctx);
        if (!aggRes.second) {
            return false;
        }

        Aggregate_ = aggRes.first;
        if (src->IsFlattenByColumns() || src->IsFlattenColumns()) {
            Flatten_ = src->IsFlattenByColumns() ? src->BuildFlattenByColumns("row") : src->BuildFlattenColumns("row");
            if (!Flatten_ || !Flatten_->Init(ctx, src)) {
                return false;
            }
        }

        if (src->IsFlattenByExprs()) {
            PreFlattenMap_ = src->BuildPreFlattenMap(ctx);
            if (!PreFlattenMap_) {
                return false;
            }
        }

        if (GroupByExpr_ || DistinctAggrExpr_) {
            PreaggregatedMap_ = src->BuildPreaggregatedMap(ctx);
            if (!PreaggregatedMap_) {
                return false;
            }
        }
        if (Aggregate_) {
            if (!Aggregate_->Init(ctx, src)) {
                return false;
            }
            if (Having_) {
                Aggregate_ = Y(
                    "Filter",
                    Aggregate_,
                    BuildLambda(Pos_, Y("row"), Y("Coalesce", Having_, Y("Bool", Q("false")))));
            }
        } else if (Having_) {
            if (Distinct_) {
                Aggregate_ = Y(
                    "Filter",
                    "core",
                    BuildLambda(Pos_, Y("row"), Y("Coalesce", Having_, Y("Bool", Q("false")))));
                if (!ctx.Warning(Having_->GetPos(), TIssuesIds::YQL_HAVING_WITHOUT_AGGREGATION_IN_SELECT_DISTINCT, [](auto& out) {
                        out << "The usage of HAVING without aggregations with SELECT DISTINCT is "
                            << "non-standard and will stop working soon. Please use WHERE instead.";
                    })) {
                    return false;
                }
            } else {
                ctx.Error(Having_->GetPos()) << "HAVING with meaning GROUP BY () should be with aggregation function.";
                return false;
            }
        } else if (!Distinct_ && !GroupBy_.empty()) {
            ctx.Error(Pos_) << "No aggregations were specified";
            return false;
        }
        if (hasError) {
            return false;
        }

        if (src->IsCalcOverWindow()) {
            if (src->IsExprSeat(EExprSeat::WindowPartitionBy, EExprType::WithExpression)) {
                PrewindowMap_ = src->BuildPrewindowMap(ctx);
                if (!PrewindowMap_) {
                    return false;
                }
            }
            CalcOverWindow_ = src->BuildCalcOverWindow(ctx, "core");
            if (!CalcOverWindow_ || !CalcOverWindow_->Init(ctx, src)) {
                return false;
            }
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        auto input = Source_->Build(ctx);
        if (!input) {
            return nullptr;
        }

        auto block(Y(Y("let", "core", input)));

        if (Source_->HasMatchRecognize()) {
            if (auto matchRecognize = Source_->BuildMatchRecognize(ctx, "core")) {
                // use unique name match_recognize to find this block easily in unit tests
                block = L(block, Y("let", "match_recognize", matchRecognize));
                // then bind to the conventional name
                block = L(block, Y("let", "core", "match_recognize"));
            } else {
                return nullptr;
            }
        }

        bool ordered = ctx.UseUnordered(*this);
        if (PreFlattenMap_) {
            block = L(block, Y("let", "core", Y(ordered ? "OrderedFlatMap" : "FlatMap", "core", BuildLambda(Pos_, Y("row"), PreFlattenMap_))));
        }
        if (Flatten_) {
            block = L(block, Y("let", "core", Y(ordered ? "OrderedFlatMap" : "FlatMap", "core", BuildLambda(Pos_, Y("row"), Flatten_, "res"))));
        }
        if (ctx.GroupByExprAfterWhere) {
            if (auto filter = Source_->BuildFilter(ctx, "core"); filter) {
                block = L(block, Y("let", "core", filter));
            }
        }
        if (PreaggregatedMap_) {
            block = L(block, Y("let", "core", PreaggregatedMap_));
            if (Source_->IsCompositeSource() && !Columns_.QualifiedAll) {
                block = L(block, Y("let", "preaggregated", "core"));
            }
        } else if (Source_->IsCompositeSource() && !Columns_.QualifiedAll) {
            block = L(block, Y("let", "origcore", "core"));
        }
        if (!ctx.GroupByExprAfterWhere) {
            if (auto filter = Source_->BuildFilter(ctx, "core"); filter) {
                block = L(block, Y("let", "core", filter));
            }
        }
        if (Aggregate_) {
            block = L(block, Y("let", "core", Aggregate_));
            ordered = false;
        }

        const bool haveCompositeTerms = Source_->IsCompositeSource() && !Columns_.All && !Columns_.QualifiedAll && !Columns_.List.empty();
        if (haveCompositeTerms) {
            // column order does not matter here - it will be set in projection
            YQL_ENSURE(Aggregate_);
            block = L(block, Y("let", "core", Y("Map", "core", BuildLambda(Pos_, Y("row"), CompositeTerms_, "row"))));
        }

        if (auto grouping = Source_->BuildGroupingColumns("core")) {
            block = L(block, Y("let", "core", grouping));
        }

        if (!Source_->GetCompositeSource()) {
            BuildProjectWindowDistinct(block, ctx, ordered);
        }

        return Y("block", Q(L(block, Y("return", "core"))));
    }

    void BuildProjectWindowDistinct(TNodePtr& block, TContext& ctx, bool ordered) override {
        if (PrewindowMap_) {
            block = L(block, Y("let", "core", PrewindowMap_));
        }
        if (CalcOverWindow_) {
            block = L(block, Y("let", "core", CalcOverWindow_));
        }

        block = L(block, Y("let", "core", Y("PersistableRepr", BuildSqlProject(ctx, ordered))));

        if (Distinct_) {
            block = L(block, Y("let", "core", Y("PersistableRepr", Y("SqlAggregateAll", Y("RemoveSystemMembers", "core")))));
        }
    }

    TNodePtr BuildSort(TContext& ctx, const TString& label) override {
        Y_UNUSED(ctx);
        if (OrderBy_.empty() || DisableSort_) {
            return nullptr;
        }

        auto sorted = BuildSortSpec(OrderBy_, label, false, AssumeSorted_);
        if (ExtraSortColumns_.empty()) {
            return Y("let", label, sorted);
        }
        auto body = Y();
        for (const auto& [column, _] : ExtraSortColumns_) {
            body = L(body, Y("let", "row", Y("RemoveMember", "row", Q(column))));
        }
        body = L(body, Y("let", "res", "row"));
        return Y("let", label, Y("OrderedMap", sorted, BuildLambda(Pos_, Y("row"), body, "res")));
    }

    TNodePtr BuildCleanupColumns(TContext& ctx, const TString& label) override {
        TNodePtr cleanup;
        if (ctx.EnableSystemColumns && ctx.Settings.Mode != NSQLTranslation::ESqlMode::LIMITED_VIEW) {
            if (Columns_.All) {
                cleanup = Y("let", label, Y("RemoveSystemMembers", label));
            } else if (!Columns_.List.empty()) {
                const bool isJoin = Source_->GetJoin();
                if (!isJoin && Columns_.QualifiedAll) {
                    if (ctx.SimpleColumns) {
                        cleanup = Y("let", label, Y("RemoveSystemMembers", label));
                    } else {
                        TNodePtr members;
                        for (auto& term : Terms_) {
                            if (term->IsAsterisk()) {
                                auto sourceName = term->GetSourceName();
                                YQL_ENSURE(*sourceName && !sourceName->empty());
                                auto prefix = *sourceName + "._yql_";
                                members = members ? L(members, Q(prefix)) : Y(Q(prefix));
                            }
                        }
                        if (members) {
                            cleanup = Y("let", label, Y("RemovePrefixMembers", label, Q(members)));
                        }
                    }
                }
            }
        }
        return cleanup;
    }

    bool IsSelect() const override {
        return true;
    }

    bool HasSelectResult() const override {
        return !Settings_.Discard;
    }

    bool IsStream() const override {
        return Source_->IsStream();
    }

    EOrderKind GetOrderKind() const override {
        if (OrderBy_.empty()) {
            return EOrderKind::None;
        }
        return AssumeSorted_ ? EOrderKind::Assume : EOrderKind::Sort;
    }

    TWriteSettings GetWriteSettings() const override {
        return Settings_;
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        const bool aggregated = Source_->HasAggregations() || Distinct_;
        if (OrderByInit_ && (Source_->GetJoin() || !aggregated)) {
            // ORDER BY will try to find column not only in projection items, but also in Source.
            // ```SELECT a, b FROM T ORDER BY c``` should work if c is present in T
            const bool reliable = column.IsReliable();
            column.SetAsNotReliable();
            auto maybeExist = IRealSource::AddColumn(ctx, column);
            if (reliable && !Source_->GetJoin()) {
                column.ResetAsReliable();
            }
            if (!maybeExist || !maybeExist.GetRef()) {
                maybeExist = Source_->AddColumn(ctx, column);
            }
            if (!maybeExist.Defined()) {
                return maybeExist;
            }
            if (!DisableSort_ && !aggregated && column.GetColumnName() && IsMissingInProjection(ctx, column)) {
                ExtraSortColumns_[FullColumnName(column)] = &column;
            }
            return maybeExist;
        }

        return IRealSource::AddColumn(ctx, column);
    }

    bool IsMissingInProjection(TContext& ctx, const TColumnNode& column) const {
        TString columnName = FullColumnName(column);
        if (Columns_.Real.contains(columnName) || Columns_.Artificial.contains(columnName)) {
            return false;
        }

        if (!ctx.SimpleColumns && Columns_.QualifiedAll && !columnName.Contains('.')) {
            return false;
        }

        if (!Columns_.IsColumnPossible(ctx, columnName)) {
            return true;
        }

        for (auto without : Without_) {
            auto name = *without->GetColumnName();
            if (Source_ && Source_->GetJoin()) {
                name = DotJoin(*without->GetSourceName(), name);
            }
            if (name == columnName) {
                return true;
            }
        }

        return false;
    }

    TNodePtr PrepareWithout(const TNodePtr& base) {
        auto terms = base;
        if (Without_) {
            for (auto without : Without_) {
                auto name = *without->GetColumnName();
                if (Source_ && Source_->GetJoin()) {
                    name = DotJoin(*without->GetSourceName(), name);
                }
                terms = L(terms, Y("let", "row", Y(ForceWithout_ ? "ForceRemoveMember" : "RemoveMember", "row", Q(name))));
            }
        }

        if (Source_) {
            for (auto column : Source_->GetTmpWindowColumns()) {
                terms = L(terms, Y("let", "row", Y("RemoveMember", "row", Q(column))));
            }
        }

        return terms;
    }

    TNodePtr DoClone() const final {
        return new TSelectCore(Pos_, Source_->CloneSource(), CloneContainer(GroupByExpr_),
                               CloneContainer(GroupBy_), CompactGroupBy_, GroupBySuffix_, AssumeSorted_, CloneContainer(OrderBy_),
                               SafeClone(Having_), CloneContainer(WinSpecs_), SafeClone(LegacyHoppingWindowSpec_),
                               CloneContainer(Terms_), Distinct_, Without_, ForceWithout_, SelectStream_, Settings_, TColumnsSets(UniqueSets_), TColumnsSets(DistinctSets_));
    }

private:
    bool InitSelect(TContext& ctx, ISource* src, bool isJoin, bool& hasError) {
        for (auto& [name, winSpec] : WinSpecs_) {
            for (size_t i = 0; i < winSpec->Partitions.size(); ++i) {
                auto partitionNode = winSpec->Partitions[i];
                if (auto sessionWindow = dynamic_cast<TSessionWindow*>(partitionNode.Get())) {
                    if (winSpec->Session) {
                        ctx.Error(partitionNode->GetPos()) << "Duplicate session window specification:";
                        ctx.Error(winSpec->Session->GetPos()) << "Previous session window is declared here";
                        hasError = true;
                        continue;
                    }
                    sessionWindow->MarkValid();
                    winSpec->Session = partitionNode;
                }

                if (!partitionNode->Init(ctx, src)) {
                    hasError = true;
                    continue;
                }
                if (!partitionNode->GetLabel() && !partitionNode->GetColumnName()) {
                    TString label = TStringBuilder() << "group_" << name << "_" << i;
                    partitionNode->SetLabel(label);
                    src->AddTmpWindowColumn(label);
                }
            }
            if (!src->AddExpressions(ctx, winSpec->Partitions, EExprSeat::WindowPartitionBy)) {
                hasError = true;
            }
        }

        if (LegacyHoppingWindowSpec_) {
            if (!LegacyHoppingWindowSpec_->TimeExtractor->Init(ctx, src)) {
                hasError = true;
            }
            src->SetLegacyHoppingWindowSpec(LegacyHoppingWindowSpec_);
        }

        for (auto iter : WinSpecs_) {
            auto winSpec = *iter.second;
            for (auto orderSpec : winSpec.OrderBy) {
                // Ensure the ORDER BY is correct before terms
                // (potentially containing window functions)
                // initialization to avoid window function
                // recursive dependency, e.g.:
                // `Rank() OVER (PARTITION BY x ORDER BY Rank())`
                if (!orderSpec->OrderExpr->Init(ctx, src)) {
                    hasError = true;
                }
            }
        }

        for (auto& term : Terms_) {
            if (!term->Init(ctx, src)) {
                hasError = true;
                continue;
            }
            auto column = term->GetColumnName();
            TString label(term->GetLabel());
            bool hasName = true;
            if (label.empty()) {
                auto source = term->GetSourceName();
                if (term->IsAsterisk() && !source->empty()) {
                    Columns_.QualifiedAll = true;
                    label = DotJoin(*source, "*");
                } else if (column) {
                    label = isJoin && source && *source ? DotJoin(*source, *column) : *column;
                } else {
                    label = Columns_.AddUnnamed();
                    hasName = false;
                    if (ctx.WarnUnnamedColumns) {
                        if (!ctx.Warning(term->GetPos(), TIssuesIds::YQL_UNNAMED_COLUMN, [&](auto& out) {
                                out << "Autogenerated column name " << label << " will be used for expression";
                            })) {
                            return false;
                        }
                    }
                }
            }
            if (hasName && !Columns_.Add(&label, false, false, true)) {
                ctx.Error(Pos_) << "Duplicate column: " << label;
                hasError = true;
            }
        }

        CompositeTerms_ = Y();
        if (!hasError && Source_->IsCompositeSource() && !Columns_.All && !Columns_.QualifiedAll && !Columns_.List.empty()) {
            auto compositeSrcPtr = static_cast<TCompositeSelect*>(Source_->GetCompositeSource());
            if (compositeSrcPtr) {
                const auto& groupings = compositeSrcPtr->GetGroupingCols();
                for (const auto& column : groupings) {
                    if (Source_->IsGroupByColumn(column)) {
                        continue;
                    }
                    const TString tableName = (GroupByExpr_ || DistinctAggrExpr_) ? "preaggregated" : "origcore";
                    CompositeTerms_ = L(CompositeTerms_, Y("let", "row", Y("AddMember", "row", BuildQuotedAtom(Pos_, column), Y("Nothing", Y("MatchType",
                                                                                                                                             Y("StructMemberType", Y("ListItemType", Y("TypeOf", tableName)), Q(column)),
                                                                                                                                             Q("Optional"), Y("lambda", Q(Y("item")), "item"), Y("lambda", Q(Y("item")), Y("OptionalType", "item")))))));
                }
            }
        }

        if (Columns_.All || Columns_.QualifiedAll) {
            Source_->AllColumns();
        }
        for (const auto& without : Without_) {
            auto namePtr = without->GetColumnName();
            auto sourcePtr = without->GetSourceName();
            YQL_ENSURE(namePtr && *namePtr);
            if (isJoin && !(sourcePtr && *sourcePtr)) {
                ctx.Error(without->GetPos()) << "Expected correlation name for WITHOUT in JOIN";
                hasError = true;
                continue;
            }
        }
        if (Having_ && !Having_->Init(ctx, src)) {
            hasError = true;
        }
        if (!src->IsCompositeSource() && !Columns_.All && src->HasAggregations()) {
            if (!WarnIfAliasFromSelectIsUsedInGroupBy(ctx, Terms_, GroupBy_, GroupByExpr_)) {
                hasError = true;
            }

            /// verify select aggregation compatibility
            TVector<TNodePtr> exprs(Terms_);
            if (Having_) {
                exprs.push_back(Having_);
            }
            for (const auto& iter : WinSpecs_) {
                for (const auto& sortSpec : iter.second->OrderBy) {
                    exprs.push_back(sortSpec->OrderExpr);
                }
            }
            if (!ValidateAllNodesForAggregation(ctx, exprs)) {
                hasError = true;
            }
        }
        const auto label = GetLabel();
        for (const auto& sortSpec : OrderBy_) {
            auto& expr = sortSpec->OrderExpr;
            SetLabel(Source_->GetLabel());
            OrderByInit_ = true;
            if (!expr->Init(ctx, this)) {
                hasError = true;
                continue;
            }
            OrderByInit_ = false;
            if (!IsComparableExpression(ctx, expr, AssumeSorted_, AssumeSorted_ ? "ASSUME ORDER BY" : "ORDER BY")) {
                hasError = true;
                continue;
            }
        }
        SetLabel(label);

        return !hasError;
    }

    TNodePtr PrepareJoinCoalesce(TContext& ctx, const TNodePtr& base, bool multipleQualifiedAll, const TVector<TString>& coalesceLabels) {
        const bool isJoin = Source_->GetJoin();
        const bool needCoalesce = isJoin && ctx.SimpleColumns &&
                                  (Columns_.All || multipleQualifiedAll || ctx.CoalesceJoinKeysOnQualifiedAll);

        if (!needCoalesce) {
            return base;
        }

        auto terms = base;
        const auto& sameKeyMap = Source_->GetJoin()->GetSameKeysMap();
        if (sameKeyMap) {
            terms = L(terms, Y("let", "flatSameKeys", "row"));
            for (const auto& [key, sources] : sameKeyMap) {
                auto coalesceKeys = Y();
                for (const auto& label : coalesceLabels) {
                    if (sources.contains(label)) {
                        coalesceKeys = L(coalesceKeys, Q(DotJoin(label, key)));
                    }
                }
                terms = L(terms, Y("let", "flatSameKeys", Y("CoalesceMembers", "flatSameKeys", Q(coalesceKeys))));
            }
            terms = L(terms, Y("let", "row", "flatSameKeys"));
        }

        return terms;
    }

    TNodePtr BuildSqlProject(TContext& ctx, bool ordered) {
        auto sqlProjectArgs = Y();
        const bool isJoin = Source_->GetJoin();

        if (Columns_.All) {
            YQL_ENSURE(Columns_.List.empty());
            auto terms = PrepareWithout(Y());
            auto options = Y();
            if (isJoin && ctx.SimpleColumns) {
                terms = PrepareJoinCoalesce(ctx, terms, false, Source_->GetJoin()->GetJoinLabels());

                auto members = Y();
                for (auto& source : Source_->GetJoin()->GetJoinLabels()) {
                    YQL_ENSURE(!source.empty());
                    members = L(members, BuildQuotedAtom(Pos_, source + "."));
                }
                if (GroupByExpr_.empty() || ctx.BogousStarInGroupByOverJoin) {
                    terms = L(terms, Y("let", "res", Y("DivePrefixMembers", "row", Q(members))));
                } else {
                    auto groupExprStruct = Y("AsStruct");
                    for (auto node : GroupByExpr_) {
                        auto label = node->GetLabel();
                        YQL_ENSURE(label);
                        if (Source_->IsGroupByColumn(label)) {
                            auto name = BuildQuotedAtom(Pos_, label);
                            groupExprStruct = L(groupExprStruct, Q(Y(name, Y("Member", "row", name))));
                        }
                    }
                    auto groupColumnsStruct = Y("DivePrefixMembers", "row", Q(members));

                    terms = L(terms, Y("let", "res", Y("FlattenMembers", Q(Y(BuildQuotedAtom(Pos_, ""), groupExprStruct)),
                                                       Q(Y(BuildQuotedAtom(Pos_, ""), groupColumnsStruct)))));
                }
                options = L(options, Q(Y(Q("divePrefix"), Q(members))));
            } else {
                terms = L(terms, Y("let", "res", "row"));
            }
            sqlProjectArgs = L(sqlProjectArgs, Y("SqlProjectStarItem", "projectCoreType", BuildQuotedAtom(Pos_, ""), BuildLambda(Pos_, Y("row"), terms, "res"), Q(options)));
        } else {
            YQL_ENSURE(!Columns_.List.empty());
            YQL_ENSURE(Columns_.List.size() == Terms_.size());

            TVector<TString> coalesceLabels;
            bool multipleQualifiedAll = false;

            if (isJoin && ctx.SimpleColumns) {
                THashSet<TString> starTerms;
                for (auto& term : Terms_) {
                    if (term->IsAsterisk()) {
                        auto sourceName = term->GetSourceName();
                        YQL_ENSURE(*sourceName && !sourceName->empty());
                        YQL_ENSURE(Columns_.QualifiedAll);
                        starTerms.insert(*sourceName);
                    }
                }

                TVector<TString> matched;
                TVector<TString> unmatched;
                for (auto& label : Source_->GetJoin()->GetJoinLabels()) {
                    if (starTerms.contains(label)) {
                        matched.push_back(label);
                    } else {
                        unmatched.push_back(label);
                    }
                }

                coalesceLabels.insert(coalesceLabels.end(), matched.begin(), matched.end());
                coalesceLabels.insert(coalesceLabels.end(), unmatched.begin(), unmatched.end());

                multipleQualifiedAll = starTerms.size() > 1;
            }

            auto column = Columns_.List.begin();
            auto isNamedColumn = Columns_.NamedColumns.begin();
            for (auto& term : Terms_) {
                auto sourceName = term->GetSourceName();
                if (!term->IsAsterisk()) {
                    auto body = Y();
                    body = L(body, Y("let", "res", term));
                    TPosition lambdaPos = Pos_;
                    TPosition aliasPos = Pos_;
                    if (term->IsImplicitLabel() && ctx.WarnOnAnsiAliasShadowing) {
                        // TODO: recanonize for positions below
                        lambdaPos = term->GetPos();
                        aliasPos = term->GetLabelPos() ? *term->GetLabelPos() : lambdaPos;
                    }
                    auto projectItem = Y("SqlProjectItem", "projectCoreType", BuildQuotedAtom(aliasPos, *isNamedColumn ? *column : ""), BuildLambda(lambdaPos, Y("row"), body, "res"));
                    if (term->IsImplicitLabel() && ctx.WarnOnAnsiAliasShadowing) {
                        projectItem = L(projectItem, Q(Y(Q(Y(Q("warnShadow"))))));
                    }
                    if (!*isNamedColumn) {
                        projectItem = L(projectItem, Q(Y(Q(Y(Q("autoName"))))));
                    }
                    sqlProjectArgs = L(sqlProjectArgs, projectItem);
                } else {
                    auto terms = PrepareWithout(Y());
                    auto options = Y();
                    if (ctx.SimpleColumns && !isJoin) {
                        terms = L(terms, Y("let", "res", "row"));
                    } else {
                        terms = PrepareJoinCoalesce(ctx, terms, multipleQualifiedAll, coalesceLabels);

                        auto members = isJoin ? Y() : Y("FlattenMembers");
                        if (isJoin) {
                            members = L(members, BuildQuotedAtom(Pos_, *sourceName + "."));
                            if (ctx.SimpleColumns) {
                                options = L(options, Q(Y(Q("divePrefix"), Q(members))));
                            }
                            members = Y(ctx.SimpleColumns ? "DivePrefixMembers" : "SelectMembers", "row", Q(members));
                        } else {
                            auto prefix = BuildQuotedAtom(Pos_, ctx.SimpleColumns ? "" : *sourceName + ".");
                            members = L(members, Q(Y(prefix, "row")));
                            if (!ctx.SimpleColumns) {
                                options = L(options, Q(Y(Q("addPrefix"), prefix)));
                            }
                        }

                        terms = L(terms, Y("let", "res", members));
                    }
                    sqlProjectArgs = L(sqlProjectArgs, Y("SqlProjectStarItem", "projectCoreType", BuildQuotedAtom(Pos_, *sourceName), BuildLambda(Pos_, Y("row"), terms, "res"), Q(options)));
                }
                ++column;
                ++isNamedColumn;
            }
        }

        for (const auto& [columnName, column] : ExtraSortColumns_) {
            auto body = Y();
            body = L(body, Y("let", "res", column));
            TPosition pos = column->GetPos();
            auto projectItem = Y("SqlProjectItem", "projectCoreType", BuildQuotedAtom(pos, columnName), BuildLambda(pos, Y("row"), body, "res"));
            sqlProjectArgs = L(sqlProjectArgs, projectItem);
        }

        auto block(Y(Y("let", "projectCoreType", Y("TypeOf", "core"))));
        block = L(block, Y("let", "core", Y(ordered ? "OrderedSqlProject" : "SqlProject", "core", Q(sqlProjectArgs))));
        if (!(UniqueSets_.empty() && DistinctSets_.empty())) {
            block = L(block, Y("let", "core", Y("RemoveSystemMembers", "core")));
            const auto MakeUniqueHint = [this](INode::TPtr& block, const TColumnsSets& sets, bool distinct) {
                if (!sets.empty()) {
                    auto assume = Y(distinct ? "AssumeDistinctHint" : "AssumeUniqueHint", "core");
                    if (!sets.front().empty()) {
                        for (const auto& columns : sets) {
                            auto set = Y();
                            for (const auto& column : columns) {
                                set = L(set, Q(column));
                            }

                            assume = L(assume, Q(set));
                        }
                    }
                    block = L(block, Y("let", "core", assume));
                }
            };

            MakeUniqueHint(block, DistinctSets_, true);
            MakeUniqueHint(block, UniqueSets_, false);
        }

        return Y("block", Q(L(block, Y("return", "core"))));
    }

private:
    TSourcePtr Source_;
    TVector<TNodePtr> GroupByExpr_;
    TVector<TNodePtr> DistinctAggrExpr_;
    TVector<TNodePtr> GroupBy_;
    bool AssumeSorted_ = false;
    bool CompactGroupBy_ = false;
    TString GroupBySuffix_;
    TVector<TSortSpecificationPtr> OrderBy_;
    TNodePtr Having_;
    TWinSpecs WinSpecs_;
    TNodePtr Flatten_;
    TNodePtr PreFlattenMap_;
    TNodePtr PreaggregatedMap_;
    TNodePtr PrewindowMap_;
    TNodePtr Aggregate_;
    TNodePtr CalcOverWindow_;
    TNodePtr CompositeTerms_;
    TVector<TNodePtr> Terms_;
    TVector<TNodePtr> Without_;
    const bool ForceWithout_;
    const bool Distinct_;
    bool OrderByInit_ = false;
    TLegacyHoppingWindowSpecPtr LegacyHoppingWindowSpec_;
    const bool SelectStream_;
    const TWriteSettings Settings_;
    const TColumnsSets UniqueSets_, DistinctSets_;
    TMap<TString, TNodePtr> ExtraSortColumns_;
};

class TProcessSource: public IRealSource {
public:
    TProcessSource(
        TPosition pos,
        TSourcePtr source,
        TNodePtr with,
        bool withExtFunction,
        TVector<TNodePtr>&& terms,
        bool listCall,
        bool processStream,
        const TWriteSettings& settings,
        const TVector<TSortSpecificationPtr>& assumeOrderBy)
        : IRealSource(pos)
        , Source_(std::move(source))
        , With_(with)
        , WithExtFunction_(withExtFunction)
        , Terms_(std::move(terms))
        , ListCall_(listCall)
        , ProcessStream_(processStream)
        , Settings_(settings)
        , AssumeOrderBy_(assumeOrderBy)
    {
    }

    void GetInputTables(TTableList& tableList) const override {
        Source_->GetInputTables(tableList);
        ISource::GetInputTables(tableList);
    }

    bool DoInit(TContext& ctx, ISource* initSrc) override {
        if (AsInner_) {
            Source_->UseAsInner();
        }

        if (!Source_->Init(ctx, initSrc)) {
            return false;
        }

        if (ProcessStream_ && !Source_->IsStream()) {
            ctx.Error(Pos_) << "PROCESS STREAM is unsupported for non-streaming sources";
            return false;
        }

        auto src = Source_.Get();
        if (!With_) {
            src->AllColumns();
            Columns_.SetAll();
            src->FinishColumns();
            return true;
        }

        /// grouped expressions are available in filters
        if (!Source_->InitFilters(ctx)) {
            return false;
        }

        TSourcePtr fakeSource = nullptr;
        if (ListCall_ && !WithExtFunction_) {
            fakeSource = BuildFakeSource(src->GetPos());
            src->AllColumns();
        }

        auto processSource = fakeSource != nullptr ? fakeSource.Get() : src;
        Y_DEBUG_ABORT_UNLESS(processSource != nullptr);
        if (!With_->Init(ctx, processSource)) {
            return false;
        }
        if (With_->GetLabel().empty()) {
            Columns_.SetAll();
        } else {
            if (ListCall_) {
                ctx.Error(With_->GetPos()) << "Label is not allowed to use with TableRows()";
                return false;
            }
            Columns_.Add(&With_->GetLabel(), false);
        }

        bool hasError = false;

        TNodePtr produce;
        if (WithExtFunction_) {
            produce = Y();
        } else {
            TString processCall = (ListCall_ ? "SqlProcess" : "Apply");
            produce = Y(processCall, With_);
        }
        TMaybe<ui32> listPosIndex;
        ui32 termIndex = 0;
        for (auto& term : Terms_) {
            if (!term->GetLabel().empty()) {
                ctx.Error(term->GetPos()) << "Labels are not allowed for PROCESS terms";
                hasError = true;
                continue;
            }

            if (!term->Init(ctx, processSource)) {
                hasError = true;
                continue;
            }

            if (ListCall_) {
                if (/* auto atom = */ dynamic_cast<TTableRows*>(term.Get())) {
                    listPosIndex = termIndex;
                }
            }
            ++termIndex;

            produce = L(produce, term);
        }

        if (hasError) {
            return false;
        }

        if (ListCall_ && !WithExtFunction_) {
            YQL_ENSURE(listPosIndex.Defined());
            produce = L(produce, Q(ToString(*listPosIndex)));
        }

        if (!produce->Init(ctx, src)) {
            hasError = true;
        }

        if (!(WithExtFunction_ && Terms_.empty())) {
            TVector<TNodePtr>(1, produce).swap(Terms_);
        }

        src->FinishColumns();

        const auto label = GetLabel();
        for (const auto& sortSpec : AssumeOrderBy_) {
            auto& expr = sortSpec->OrderExpr;
            SetLabel(Source_->GetLabel());
            if (!expr->Init(ctx, this)) {
                hasError = true;
                continue;
            }
            if (!IsComparableExpression(ctx, expr, true, "ASSUME ORDER BY")) {
                hasError = true;
                continue;
            }
        }
        SetLabel(label);

        return !hasError;
    }

    TNodePtr Build(TContext& ctx) override {
        auto input = Source_->Build(ctx);
        if (!input) {
            return nullptr;
        }

        if (!With_) {
            auto res = input;
            if (ctx.EnableSystemColumns) {
                res = Y("RemoveSystemMembers", res);
            }

            return res;
        }

        TString inputLabel = ListCall_ ? "inputRowsList" : "core";

        auto block(Y(Y("let", inputLabel, input)));

        auto filter = Source_->BuildFilter(ctx, inputLabel);
        if (filter) {
            block = L(block, Y("let", inputLabel, filter));
        }

        if (WithExtFunction_) {
            auto preTransform = Y("RemoveSystemMembers", inputLabel);
            if (Terms_.size() > 0) {
                preTransform = Y("Map", preTransform, BuildLambda(Pos_, Y("row"), Q(Terms_[0])));
            }
            block = L(block, Y("let", inputLabel, preTransform));
            block = L(block, Y("let", "transform", With_));
            block = L(block, Y("let", "core", Y("Apply", "transform", inputLabel)));
        } else if (ListCall_) {
            block = L(block, Y("let", "core", Terms_[0]));
        } else {
            auto terms = BuildColumnsTerms(ctx);
            block = L(block, Y("let", "core", Y(ctx.UseUnordered(*this) ? "OrderedFlatMap" : "FlatMap", "core", BuildLambda(Pos_, Y("row"), terms, "res"))));
        }
        block = L(block, Y("let", "core", Y("AutoDemux", Y("PersistableRepr", "core"))));
        return Y("block", Q(L(block, Y("return", "core"))));
    }

    TNodePtr BuildSort(TContext& ctx, const TString& label) override {
        Y_UNUSED(ctx);
        if (AssumeOrderBy_.empty()) {
            return nullptr;
        }

        return Y("let", label, BuildSortSpec(AssumeOrderBy_, label, false, true));
    }

    EOrderKind GetOrderKind() const override {
        if (!With_) {
            return EOrderKind::Passthrough;
        }
        return AssumeOrderBy_.empty() ? EOrderKind::None : EOrderKind::Assume;
    }

    bool IsSelect() const override {
        return false;
    }

    bool HasSelectResult() const override {
        return !Settings_.Discard;
    }

    bool IsStream() const override {
        return Source_->IsStream();
    }

    TWriteSettings GetWriteSettings() const override {
        return Settings_;
    }

    TNodePtr DoClone() const final {
        return new TProcessSource(Pos_, Source_->CloneSource(), SafeClone(With_), WithExtFunction_,
                                  CloneContainer(Terms_), ListCall_, ProcessStream_, Settings_, CloneContainer(AssumeOrderBy_));
    }

private:
    TNodePtr BuildColumnsTerms(TContext& ctx) {
        Y_UNUSED(ctx);
        TNodePtr terms;
        Y_DEBUG_ABORT_UNLESS(Terms_.size() == 1);
        if (Columns_.All) {
            terms = Y(Y("let", "res", Y("ToSequence", Terms_.front())));
        } else {
            Y_DEBUG_ABORT_UNLESS(Columns_.List.size() == Terms_.size());
            terms = L(Y(), Y("let", "res",
                             L(Y("AsStructUnordered"), Q(Y(BuildQuotedAtom(Pos_, Columns_.List.front()), Terms_.front())))));
            terms = L(terms, Y("let", "res", Y("Just", "res")));
        }
        return terms;
    }

private:
    TSourcePtr Source_;
    TNodePtr With_;
    const bool WithExtFunction_;
    TVector<TNodePtr> Terms_;
    const bool ListCall_;
    const bool ProcessStream_;
    const TWriteSettings Settings_;
    TVector<TSortSpecificationPtr> AssumeOrderBy_;
};

TSourcePtr BuildProcess(
    TPosition pos,
    TSourcePtr source,
    TNodePtr with,
    bool withExtFunction,
    TVector<TNodePtr>&& terms,
    bool listCall,
    bool processStream,
    const TWriteSettings& settings,
    const TVector<TSortSpecificationPtr>& assumeOrderBy) {
    return new TProcessSource(pos, std::move(source), with, withExtFunction, std::move(terms), listCall, processStream, settings, assumeOrderBy);
}

class TNestedProxySource: public IProxySource {
public:
    TNestedProxySource(TPosition pos, const TVector<TNodePtr>& groupBy, TSourcePtr source)
        : IProxySource(pos, source.Get())
        , CompositeSelect_(nullptr)
        , Holder_(std::move(source))
        , GroupBy_(groupBy)
    {
    }

    TNestedProxySource(TCompositeSelect* compositeSelect, const TVector<TNodePtr>& groupBy)
        : IProxySource(compositeSelect->GetPos(), compositeSelect->RealSource())
        , CompositeSelect_(compositeSelect)
        , GroupBy_(groupBy)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        return Source_->Init(ctx, src);
    }

    TNodePtr Build(TContext& ctx) override {
        return CompositeSelect_ ? BuildAtom(Pos_, "composite", TNodeFlags::Default) : Source_->Build(ctx);
    }

    bool InitFilters(TContext& ctx) override {
        return CompositeSelect_ ? true : Source_->InitFilters(ctx);
    }

    TNodePtr BuildFilter(TContext& ctx, const TString& label) override {
        return CompositeSelect_ ? nullptr : Source_->BuildFilter(ctx, label);
    }

    IJoin* GetJoin() override {
        return Source_->GetJoin();
    }

    bool IsCompositeSource() const override {
        return true;
    }

    ISource* GetCompositeSource() override {
        return CompositeSelect_;
    }

    bool AddGrouping(TContext& ctx, const TVector<TString>& columns, TString& hintColumn) override {
        Y_UNUSED(ctx);
        hintColumn = TStringBuilder() << "GroupingHint" << Hints_.size();
        ui64 hint = 0;
        if (GroupByColumns_.empty()) {
            const bool isJoin = GetJoin();
            for (const auto& groupByNode : GroupBy_) {
                auto namePtr = groupByNode->GetColumnName();
                YQL_ENSURE(namePtr);
                TString column = *namePtr;
                if (isJoin) {
                    auto sourceNamePtr = groupByNode->GetSourceName();
                    if (sourceNamePtr && !sourceNamePtr->empty()) {
                        column = DotJoin(*sourceNamePtr, column);
                    }
                }
                GroupByColumns_.insert(column);
            }
        }
        for (const auto& column : columns) {
            hint <<= 1;
            if (!GroupByColumns_.contains(column)) {
                hint += 1;
            }
        }
        Hints_.push_back(hint);
        return true;
    }

    size_t GetGroupingColumnsCount() const override {
        return Hints_.size();
    }

    TNodePtr BuildGroupingColumns(const TString& label) override {
        if (Hints_.empty()) {
            return nullptr;
        }

        auto body = Y();
        for (size_t i = 0; i < Hints_.size(); ++i) {
            TString hintColumn = TStringBuilder() << "GroupingHint" << i;
            TString hintValue = ToString(Hints_[i]);
            body = L(body, Y("let", "row", Y("AddMember", "row", Q(hintColumn), Y("Uint64", Q(hintValue)))));
        }
        return Y("Map", label, BuildLambda(Pos_, Y("row"), body, "row"));
    }

    void FinishColumns() override {
        Source_->FinishColumns();
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        if (const TString* columnName = column.GetColumnName()) {
            if (columnName && IsExprAlias(*columnName)) {
                return true;
            }
        }
        return Source_->AddColumn(ctx, column);
    }

    TPtr DoClone() const final {
        YQL_ENSURE(Hints_.empty());
        return Holder_.Get() ? new TNestedProxySource(Pos_, CloneContainer(GroupBy_), Holder_->CloneSource()) : new TNestedProxySource(CompositeSelect_, CloneContainer(GroupBy_));
    }

private:
    TCompositeSelect* CompositeSelect_;
    TSourcePtr Holder_;
    TVector<TNodePtr> GroupBy_;
    mutable TSet<TString> GroupByColumns_;
    mutable TVector<ui64> Hints_;
};

namespace {
TSourcePtr DoBuildSelectCore(
    TContext& ctx,
    TPosition pos,
    TSourcePtr originalSource,
    TSourcePtr source,
    const TVector<TNodePtr>& groupByExpr,
    const TVector<TNodePtr>& groupBy,
    bool compactGroupBy,
    const TString& groupBySuffix,
    bool assumeSorted,
    const TVector<TSortSpecificationPtr>& orderBy,
    TNodePtr having,
    TWinSpecs&& winSpecs,
    TLegacyHoppingWindowSpecPtr legacyHoppingWindowSpec,
    TVector<TNodePtr>&& terms,
    bool distinct,
    TVector<TNodePtr>&& without,
    bool forceWithout,
    bool selectStream,
    const TWriteSettings& settings,
    TColumnsSets&& uniqueSets,
    TColumnsSets&& distinctSets) {
    if (groupBy.empty() || !groupBy.front()->ContentListPtr()) {
        return new TSelectCore(pos, std::move(source), groupByExpr, groupBy, compactGroupBy, groupBySuffix, assumeSorted,
                               orderBy, having, winSpecs, legacyHoppingWindowSpec, terms, distinct, without, forceWithout, selectStream, settings, std::move(uniqueSets), std::move(distinctSets));
    }
    if (groupBy.size() == 1) {
        /// actualy no big idea to use grouping function in this case (result allways 0)
        auto contentPtr = groupBy.front()->ContentListPtr();
        source = new TNestedProxySource(pos, *contentPtr, source);
        return DoBuildSelectCore(ctx, pos, originalSource, source, groupByExpr, *contentPtr, compactGroupBy, groupBySuffix,
                                 assumeSorted, orderBy, having, std::move(winSpecs),
                                 legacyHoppingWindowSpec, std::move(terms), distinct, std::move(without), forceWithout, selectStream, settings, std::move(uniqueSets), std::move(distinctSets));
    }
    /// \todo some smart merge logic, generalize common part of grouping (expr, flatten, etc)?
    TIntrusivePtr<TCompositeSelect> compositeSelect = new TCompositeSelect(pos, std::move(source), originalSource->CloneSource(), settings);
    size_t totalGroups = 0;
    TVector<TSourcePtr> subselects;
    TVector<TNodePtr> groupingCols;
    for (auto& grouping : groupBy) {
        auto contentPtr = grouping->ContentListPtr();
        TVector<TNodePtr> cache(1, nullptr);
        if (!contentPtr) {
            cache[0] = grouping;
            contentPtr = &cache;
        }
        groupingCols.insert(groupingCols.end(), contentPtr->cbegin(), contentPtr->cend());
        TSourcePtr proxySource = new TNestedProxySource(compositeSelect.Get(), CloneContainer(*contentPtr));
        if (!subselects.empty()) {
            /// clone terms for others usage
            TVector<TNodePtr> termsCopy;
            for (const auto& term : terms) {
                termsCopy.emplace_back(term->Clone());
            }
            std::swap(terms, termsCopy);
        }
        totalGroups += contentPtr->size();
        TSelectCore* selectCore = new TSelectCore(pos, std::move(proxySource), CloneContainer(groupByExpr),
                                                  CloneContainer(*contentPtr), compactGroupBy, groupBySuffix, assumeSorted, orderBy, SafeClone(having), CloneContainer(winSpecs),
                                                  legacyHoppingWindowSpec, terms, distinct, without, forceWithout, selectStream, settings, TColumnsSets(uniqueSets), TColumnsSets(distinctSets));
        subselects.emplace_back(selectCore);
    }
    if (totalGroups > ctx.PragmaGroupByLimit) {
        ctx.Error(pos) << "Unable to GROUP BY more than " << ctx.PragmaGroupByLimit << " groups, you try use " << totalGroups << " groups";
        return nullptr;
    }
    compositeSelect->SetSubselects(std::move(subselects), std::move(groupingCols), CloneContainer(groupByExpr));
    return compositeSelect;
}

} // namespace

TSourcePtr BuildSelectCore(
    TContext& ctx,
    TPosition pos,
    TSourcePtr source,
    const TVector<TNodePtr>& groupByExpr,
    const TVector<TNodePtr>& groupBy,
    bool compactGroupBy,
    const TString& groupBySuffix,
    bool assumeSorted,
    const TVector<TSortSpecificationPtr>& orderBy,
    TNodePtr having,
    TWinSpecs&& winSpecs,
    TLegacyHoppingWindowSpecPtr legacyHoppingWindowSpec,
    TVector<TNodePtr>&& terms,
    bool distinct,
    TVector<TNodePtr>&& without,
    bool forceWithout,
    bool selectStream,
    const TWriteSettings& settings,
    TColumnsSets&& uniqueSets,
    TColumnsSets&& distinctSets)
{
    return DoBuildSelectCore(ctx, pos, source, source, groupByExpr, groupBy, compactGroupBy, groupBySuffix, assumeSorted, orderBy,
                             having, std::move(winSpecs), legacyHoppingWindowSpec, std::move(terms), distinct, std::move(without), forceWithout, selectStream, settings, std::move(uniqueSets), std::move(distinctSets));
}

class TSelectOp: public IRealSource {
public:
    TSelectOp(TPosition pos, TVector<TSourcePtr>&& sources, const TString& op, bool quantifierAll, const TWriteSettings& settings)
        : IRealSource(pos)
        , Sources_(std::move(sources))
        , Operator_(op)
        , QuantifierAll_(quantifierAll)
        , Settings_(settings)
    {
    }

    const TColumns* GetColumns() const override {
        return IRealSource::GetColumns();
    }

    void GetInputTables(TTableList& tableList) const override {
        for (auto& x : Sources_) {
            x->GetInputTables(tableList);
        }

        ISource::GetInputTables(tableList);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        bool first = true;
        for (auto& s : Sources_) {
            s->UseAsInner();
            if (!s->Init(ctx, src)) {
                return false;
            }
            if (!ctx.PositionalUnionAll || first) {
                auto c = s->GetColumns();
                Y_DEBUG_ABORT_UNLESS(c);
                Columns_.Merge(*c);
                first = false;
            }
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        TString op;

        if (Operator_ == "union") {
            op = "Union";
        } else if (Operator_ == "intersect") {
            op = "Intersect";
        } else if (Operator_ == "except") {
            op = "Except";
        } else {
            Y_ABORT("Invalid operator: %s", Operator_.c_str());
        }

        if (QuantifierAll_) {
            if (Operator_ != "union" || !ctx.EmitUnionMerge) {
                op += "All";
            } else {
                op += "Merge";
            }
        }
        if (ctx.PositionalUnionAll) {
            op += "Positional";
        }

        TPtr res = Y(op);

        for (auto& s : Sources_) {
            auto input = s->Build(ctx);
            if (!input) {
                return nullptr;
            }
            res->Add(input);
        }
        return res;
    }

    bool IsStream() const override {
        for (auto& s : Sources_) {
            if (!s->IsStream()) {
                return false;
            }
        }
        return true;
    }

    TNodePtr DoClone() const final {
        return MakeIntrusive<TSelectOp>(Pos_, CloneContainer(Sources_), Operator_, QuantifierAll_, Settings_);
    }

    bool IsSelect() const override {
        return true;
    }

    bool HasSelectResult() const override {
        return !Settings_.Discard;
    }

    TWriteSettings GetWriteSettings() const override {
        return Settings_;
    }

private:
    TVector<TSourcePtr> Sources_;
    const TString Operator_;
    bool QuantifierAll_;
    const TWriteSettings Settings_;
};

TSourcePtr BuildSelectOp(
    TPosition pos,
    TVector<TSourcePtr>&& sources,
    const TString& op,
    bool quantifierAll,
    const TWriteSettings& settings) {
    return new TSelectOp(pos, std::move(sources), op, quantifierAll, settings);
}

class TOverWindowSource: public IProxySource {
public:
    TOverWindowSource(TPosition pos, const TString& windowName, ISource* origSource)
        : IProxySource(pos, origSource)
        , WindowName_(windowName)
    {
        Source_->SetLabel(origSource->GetLabel());
    }

    TString MakeLocalName(const TString& name) override {
        return Source_->MakeLocalName(name);
    }

    void AddTmpWindowColumn(const TString& column) override {
        return Source_->AddTmpWindowColumn(column);
    }

    bool AddAggregation(TContext& ctx, TAggregationPtr aggr) override {
        if (aggr->IsOverWindow() || aggr->IsOverWindowDistinct()) {
            return Source_->AddAggregationOverWindow(ctx, WindowName_, aggr);
        }
        return Source_->AddAggregation(ctx, aggr);
    }

    bool AddFuncOverWindow(TContext& ctx, TNodePtr expr) override {
        return Source_->AddFuncOverWindow(ctx, WindowName_, expr);
    }

    bool IsOverWindowSource() const override {
        return true;
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        return Source_->AddColumn(ctx, column);
    }

    TNodePtr Build(TContext& ctx) override {
        Y_UNUSED(ctx);
        Y_ABORT("Unexpected call");
    }

    const TString* GetWindowName() const override {
        return &WindowName_;
    }

    TWindowSpecificationPtr FindWindowSpecification(TContext& ctx, const TString& windowName) const override {
        return Source_->FindWindowSpecification(ctx, windowName);
    }

    TNodePtr GetSessionWindowSpec() const override {
        return Source_->GetSessionWindowSpec();
    }

    IJoin* GetJoin() override {
        return Source_->GetJoin();
    }

    TNodePtr DoClone() const final {
        return {};
    }

private:
    const TString WindowName_;
};

TSourcePtr BuildOverWindowSource(TPosition pos, const TString& windowName, ISource* origSource) {
    return new TOverWindowSource(pos, windowName, origSource);
}

class TSkipTakeNode final: public TAstListNode {
public:
    TSkipTakeNode(TPosition pos, const TNodePtr& skip, const TNodePtr& take)
        : TAstListNode(pos)
        , IsSkipProvided_(!!skip)
    {
        TNodePtr select(AstNode("select"));
        if (skip) {
            select = Y("Skip", select, Y("Coalesce", skip, Y("Uint64", Q("0"))));
        }
        static const TString Max = ::ToString(std::numeric_limits<ui64>::max());
        Add("let", "select", Y("Take", select, Y("Coalesce", take, Y("Uint64", Q(Max)))));
    }

    TPtr DoClone() const final {
        return {};
    }

    bool HasSkip() const override {
        return IsSkipProvided_;
    }

private:
    const bool IsSkipProvided_;
};

TNodePtr BuildSkipTake(TPosition pos, const TNodePtr& skip, const TNodePtr& take) {
    return new TSkipTakeNode(pos, skip, take);
}

class TSelect: public IProxySource {
public:
    TSelect(TPosition pos, TSourcePtr source, TNodePtr skipTake)
        : IProxySource(pos, source.Get())
        , Source_(std::move(source))
        , SkipTake_(skipTake)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Source_->SetLabel(Label_);
        if (AsInner_) {
            Source_->UseAsInner();
        }

        if (IgnoreSort()) {
            Source_->DisableSort();
            if (!ctx.Warning(Source_->GetPos(), TIssuesIds::YQL_ORDER_BY_WITHOUT_LIMIT_IN_SUBQUERY, [](auto& out) {
                    out << "ORDER BY without LIMIT in subquery will be ignored";
                })) {
                return false;
            }
        }

        if (!Source_->Init(ctx, src)) {
            return false;
        }
        src = Source_.Get();
        if (SkipTake_) {
            FakeSource_ = BuildFakeSource(SkipTake_->GetPos());
            if (!SkipTake_->Init(ctx, FakeSource_.Get())) {
                return false;
            }
            if (SkipTake_->HasSkip() && Source_->GetOrderKind() != EOrderKind::Sort && Source_->GetOrderKind() != EOrderKind::Assume) {
                if (!ctx.Warning(Source_->GetPos(), TIssuesIds::YQL_OFFSET_WITHOUT_SORT, [](auto& out) {
                        out << "LIMIT with OFFSET without [ASSUME] ORDER BY may provide "
                            << "different results from run to run";
                    })) {
                    return false;
                }
            }
        }

        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        auto input = Source_->Build(ctx);
        if (!input) {
            return nullptr;
        }
        const auto label = "select";
        auto block(Y(Y("let", label, input)));

        auto sortNode = Source_->BuildSort(ctx, label);
        if (sortNode && !IgnoreSort()) {
            block = L(block, sortNode);
        }

        if (SkipTake_) {
            block = L(block, SkipTake_);
        }

        TNodePtr sample;
        if (!BuildSamplingLambda(sample)) {
            return nullptr;
        } else if (sample) {
            block = L(block, Y("let", "select", Y("OrderedFlatMap", "select", sample)));
        }

        if (auto removeNode = Source_->BuildCleanupColumns(ctx, label)) {
            block = L(block, removeNode);
        }

        block = L(block, Y("return", label));
        return Y("block", Q(block));
    }

    bool SetSamplingOptions(
        TContext& ctx,
        TPosition pos,
        ESampleClause sampleClause,
        ESampleMode mode,
        TNodePtr samplingRate,
        TNodePtr samplingSeed) override {
        if (mode == ESampleMode::System) {
            ctx.Error(pos) << "only Bernoulli sampling mode is supported for subqueries";
            return false;
        }
        if (samplingSeed) {
            ctx.Error(pos) << "'Repeatable' keyword is not supported for subqueries";
            return false;
        }
        return SetSamplingRate(ctx, sampleClause, samplingRate);
    }

    bool IsSelect() const override {
        return Source_->IsSelect();
    }

    bool HasSelectResult() const override {
        return Source_->HasSelectResult();
    }

    TPtr DoClone() const final {
        return MakeIntrusive<TSelect>(Pos_, Source_->CloneSource(), SafeClone(SkipTake_));
    }

protected:
    bool IgnoreSort() const {
        return AsInner_ && !SkipTake_ && EOrderKind::Sort == Source_->GetOrderKind();
    }

    TSourcePtr Source_;
    TNodePtr SkipTake_;
    TSourcePtr FakeSource_;
};

TSourcePtr BuildSelect(TPosition pos, TSourcePtr source, TNodePtr skipTake) {
    return new TSelect(pos, std::move(source), skipTake);
}

class TAnyColumnSource final: public ISource {
public:
    explicit TAnyColumnSource(TPosition pos)
        : ISource(pos)
    {
    }

    bool DoInit(TContext&, ISource*) final {
        return true;
    }

    TNodePtr Build(TContext&) final {
        return nullptr;
    }

    TNodePtr DoClone() const final {
        return MakeIntrusive<TAnyColumnSource>(Pos_);
    }

    TMaybe<bool> AddColumn(TContext&, TColumnNode&) final {
        return {true};
    }
};

TSourcePtr BuildAnyColumnSource(TPosition pos) {
    return new TAnyColumnSource(pos);
}

class TSelectResultNode final: public TAstListNode {
public:
    TSelectResultNode(TPosition pos, TSourcePtr source, bool writeResult, bool inSubquery,
                      TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Source_(std::move(source))
        , WriteResult_(writeResult)
        , InSubquery_(inSubquery)
        , Scoped_(scoped)
    {
        YQL_ENSURE(Source_, "Invalid source node");
        FakeSource_ = BuildFakeSource(pos);
    }

    bool IsSelect() const override {
        return true;
    }

    bool HasSelectResult() const override {
        return Source_->HasSelectResult();
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Source_->Init(ctx, src)) {
            return false;
        }

        src = Source_.Get();
        TTableList tableList;
        Source_->GetInputTables(tableList);

        TNodePtr node(BuildInputTables(Pos_, tableList, InSubquery_, Scoped_));
        if (!node->Init(ctx, src)) {
            return false;
        }

        auto writeSettings = src->GetWriteSettings();
        bool asRef = ctx.PragmaRefSelect;
        bool asAutoRef = true;
        if (ctx.PragmaSampleSelect) {
            asRef = false;
            asAutoRef = false;
        }

        auto settings = Y(Q(Y(Q("type"))));
        if (writeSettings.Discard) {
            settings = L(settings, Q(Y(Q("discard"))));
        }

        if (!writeSettings.Label.Empty()) {
            auto labelNode = writeSettings.Label.Build();
            if (!writeSettings.Label.GetLiteral()) {
                labelNode = Y("EvaluateAtom", labelNode);
            }

            if (!labelNode->Init(ctx, FakeSource_.Get())) {
                return false;
            }

            settings = L(settings, Q(Y(Q("label"), labelNode)));
        }

        if (asRef) {
            settings = L(settings, Q(Y(Q("ref"))));
        } else if (asAutoRef) {
            settings = L(settings, Q(Y(Q("autoref"))));
        }

        auto columns = Source_->GetColumns();
        if (columns && !columns->All && !(columns->QualifiedAll && ctx.SimpleColumns)) {
            auto list = Y();
            YQL_ENSURE(columns->List.size() == columns->NamedColumns.size());
            for (size_t i = 0; i < columns->List.size(); ++i) {
                auto& c = columns->List[i];
                if (c.EndsWith('*')) {
                    list = L(list, Q(Y(Q("prefix"), BuildQuotedAtom(Pos_, c.substr(0, c.size() - 1)))));
                } else if (columns->NamedColumns[i]) {
                    list = L(list, BuildQuotedAtom(Pos_, c));
                } else {
                    list = L(list, Q(Y(Q("auto"))));
                }
            }
            settings = L(settings, Q(Y(Q("columns"), Q(list))));
        }

        if (ctx.ResultRowsLimit > 0) {
            settings = L(settings, Q(Y(Q("take"), Q(ToString(ctx.ResultRowsLimit)))));
        }

        auto output = Source_->Build(ctx);
        if (!output) {
            return false;
        }
        node = L(node, Y("let", "output", output));
        if (WriteResult_ || writeSettings.Discard) {
            if (EOrderKind::None == Source_->GetOrderKind() && ctx.UseUnordered(*Source_)) {
                node = L(node, Y("let", "output", Y("Unordered", "output")));
                if (ctx.UnorderedResult) {
                    settings = L(settings, Q(Y(Q("unordered"))));
                }
            }
            auto writeResult(BuildWriteResult(Pos_, "output", settings));
            if (!writeResult->Init(ctx, src)) {
                return false;
            }
            node = L(node, Y("let", "world", writeResult));
            node = L(node, Y("return", "world"));
        } else {
            node = L(node, Y("return", "output"));
        }

        Add("block", Q(node));
        return true;
    }

    TPtr DoClone() const final {
        return {};
    }

protected:
    TSourcePtr Source_;

    const bool WriteResult_;
    const bool InSubquery_;
    TScopedStatePtr Scoped_;
    TSourcePtr FakeSource_;
};

TNodePtr BuildSelectResult(TPosition pos, TSourcePtr source, bool writeResult, bool inSubquery,
                           TScopedStatePtr scoped) {
    return new TSelectResultNode(pos, std::move(source), writeResult, inSubquery, scoped);
}

} // namespace NSQLTranslationV1
