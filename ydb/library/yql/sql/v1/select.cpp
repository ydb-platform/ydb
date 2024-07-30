#include "sql.h"
#include "source.h"

#include "context.h"
#include "match_recognize.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/charset/ci_string.h>

using namespace NYql;

namespace NSQLTranslationV1 {

class TSubqueryNode: public INode {
public:
    TSubqueryNode(TSourcePtr&& source, const TString& alias, bool inSubquery, int ensureTupleSize, TScopedStatePtr scoped)
        : INode(source->GetPos())
        , Source(std::move(source))
        , Alias(alias)
        , InSubquery(inSubquery)
        , EnsureTupleSize(ensureTupleSize)
        , Scoped(scoped)
    {
        YQL_ENSURE(!Alias.empty());
    }

    ISource* GetSource() override {
        return Source.Get();
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        YQL_ENSURE(!src, "Source not expected for subquery node");
        Source->UseAsInner();
        if (!Source->Init(ctx, nullptr)) {
            return false;
        }

        TTableList tableList;
        Source->GetInputTables(tableList);

        auto tables = BuildInputTables(Pos, tableList, InSubquery, Scoped);
        if (!tables->Init(ctx, Source.Get())) {
            return false;
        }

        auto source = Source->Build(ctx);
        if (!source) {
            return false;
        }
        if (EnsureTupleSize != -1) {
            source = Y("EnsureTupleSize", source, Q(ToString(EnsureTupleSize)));
        }

        Node = Y("let", Alias, Y("block", Q(L(tables, Y("return", Q(Y("world", source)))))));
        IsUsed = true;
        return true;
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const, true);
    }

    bool UsedSubquery() const override {
        return IsUsed;
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node);
        return Node->Translate(ctx);
    }

    const TString* SubqueryAlias() const override {
        return &Alias;
    }

    TPtr DoClone() const final {
        return new TSubqueryNode(Source->CloneSource(), Alias, InSubquery, EnsureTupleSize, Scoped);
    }

protected:
    TSourcePtr Source;
    TNodePtr Node;
    const TString Alias;
    const bool InSubquery;
    const int EnsureTupleSize;
    bool IsUsed = false;
    TScopedStatePtr Scoped;
};

TNodePtr BuildSubquery(TSourcePtr source, const TString& alias, bool inSubquery, int ensureTupleSize, TScopedStatePtr scoped) {
    return new TSubqueryNode(std::move(source), alias, inSubquery, ensureTupleSize, scoped);
}

class TSourceNode: public INode {
public:
    TSourceNode(TPosition pos, TSourcePtr&& source, bool checkExist)
        : INode(pos)
        , Source(std::move(source))
        , CheckExist(checkExist)
    {}

    ISource* GetSource() override {
        return Source.Get();
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (AsInner) {
            Source->UseAsInner();
        }
        if (!Source->Init(ctx, src)) {
            return false;
        }
        Node = Source->Build(ctx);
        if (!Node) {
            return false;
        }
        if (src) {
            if (IsSubquery()) {
                /// should be not used?
                auto columnsPtr = Source->GetColumns();
                if (columnsPtr && (columnsPtr->All || columnsPtr->QualifiedAll || columnsPtr->List.size() == 1)) {
                    Node = Y("SingleMember", Y("SqlAccess", Q("dict"), Y("Take", Node, Y("Uint64", Q("1"))), Y("Uint64", Q("0"))));
                } else {
                    ctx.Error(Pos) << "Source used in expression should contain one concrete column";
                    return false;
                }
            }
            src->AddDependentSource(Source.Get());
        }
        return true;
    }

    bool IsSubquery() const {
        return !AsInner && Source->IsSelect() && !CheckExist;
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const, IsSubquery());
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node);
        return Node->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TSourceNode(Pos, Source->CloneSource(), CheckExist);
    }
protected:
    TSourcePtr Source;
    TNodePtr Node;
    bool CheckExist;
};

TNodePtr BuildSourceNode(TPosition pos, TSourcePtr source, bool checkExist) {
    return new TSourceNode(pos, std::move(source), checkExist);
}

class TFakeSource: public ISource {
public:
    TFakeSource(TPosition pos, bool missingFrom, bool inSubquery)
        : ISource(pos)
        , MissingFrom(missingFrom)
        , InSubquery(inSubquery)
    {}

    bool IsFake() const override {
        return true;
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        // TODO: fix column reference scope - with proper scopes error below should happen earlier
        if (column.CanBeType()) {
            return true;
        }
        ctx.Error(Pos) << (MissingFrom ? "Column references are not allowed without FROM" : "Source does not allow column references");
        ctx.Error(column.GetPos()) << "Column reference "
                                   << (column.GetColumnName() ? "'" + *column.GetColumnName() + "'" : "(expr)");
        return {};
    }

    bool AddFilter(TContext& ctx, TNodePtr filter) override  {
        Y_UNUSED(filter);
        auto pos = filter ? filter->GetPos() : Pos;
        ctx.Error(pos) << (MissingFrom ? "Filtering is not allowed without FROM" : "Source does not allow filtering");
        return false;
    }

    TNodePtr Build(TContext& ctx) override  {
        Y_UNUSED(ctx);
        auto ret = Y("AsList", Y("AsStruct"));
        if (InSubquery) {
            return Y("WithWorld", ret, "world");
        } else {
            return ret;
        }
    }

    bool AddGroupKey(TContext& ctx, const TString& column) override {
        Y_UNUSED(column);
        ctx.Error(Pos) << "Grouping is not allowed " << (MissingFrom ? "without FROM" : "in this context");
        return false;
    }

    bool AddAggregation(TContext& ctx, TAggregationPtr aggr) override {
        YQL_ENSURE(aggr);
        ctx.Error(aggr->GetPos()) << "Aggregation is not allowed " << (MissingFrom ? "without FROM" : "in this context");
        return false;
    }

    bool AddAggregationOverWindow(TContext& ctx, const TString& windowName, TAggregationPtr func) override {
        Y_UNUSED(windowName);
        YQL_ENSURE(func);
        ctx.Error(func->GetPos()) << "Aggregation is not allowed " << (MissingFrom ? "without FROM" : "in this context");
        return false;
    }

    bool AddFuncOverWindow(TContext& ctx, const TString& windowName, TNodePtr func) override {
        Y_UNUSED(windowName);
        YQL_ENSURE(func);
        ctx.Error(func->GetPos()) << "Window functions are not allowed " << (MissingFrom ? "without FROM" : "in this context");
        return false;
    }

    TWindowSpecificationPtr FindWindowSpecification(TContext& ctx, const TString& windowName) const override {
        Y_UNUSED(windowName);
        ctx.Error(Pos) << "Window and aggregation functions are not allowed " << (MissingFrom ? "without FROM" : "in this context");
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
        return { nullptr, true };
    }

    TPtr DoClone() const final {
        return new TFakeSource(Pos, MissingFrom, InSubquery);
    }
private:
    const bool MissingFrom;
    const bool InSubquery;
};

TSourcePtr BuildFakeSource(TPosition pos, bool missingFrom, bool inSubquery) {
    return new TFakeSource(pos, missingFrom, inSubquery);
}

class TNodeSource: public ISource {
public:
    TNodeSource(TPosition pos, const TNodePtr& node, bool wrapToList)
        : ISource(pos)
        , Node(node)
        , WrapToList(wrapToList)
    {
        YQL_ENSURE(Node);
        FakeSource = BuildFakeSource(pos);
    }

    void AllColumns() final {
        UseAllColumns = true;
    }

    bool ShouldUseSourceAsColumn(const TString& source) const final {
        return source && source != GetLabel();
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) final {
        Y_UNUSED(ctx);
        if (UseAllColumns) {
            return true;
        }

        if (column.IsAsterisk()) {
            AllColumns();
        } else {
            if (column.GetColumnName()) {
                Columns.insert(*column.GetColumnName());
            } else {
                AllColumns();
            }
        }

        return true;
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (!Node->Init(ctx, FakeSource.Get())) {
            return false;
        }
        return ISource::DoInit(ctx, src);
    }

    TNodePtr Build(TContext& ctx) final  {
        auto nodeAst = AstNode(Node);
        if (WrapToList) {
            nodeAst = Y("ToList", nodeAst);
        }

        if (UseAllColumns) {
            return nodeAst;
        } else {
            auto members = Y();
            for (auto& column : Columns) {
                members = L(members, BuildQuotedAtom(Pos, column));
            }

            return Y(ctx.UseUnordered(*this) ? "OrderedMap" : "Map", nodeAst, BuildLambda(Pos, Y("row"), Y("SelectMembers", "row", Q(members))));
        }
    }

    TPtr DoClone() const final {
        return new TNodeSource(Pos, SafeClone(Node), WrapToList);
    }

private:
    TNodePtr Node;
    bool WrapToList;
    TSourcePtr FakeSource;
    TSet<TString> Columns;
    bool UseAllColumns = false;
};

TSourcePtr BuildNodeSource(TPosition pos, const TNodePtr& node, bool wrapToList) {
    return new TNodeSource(pos, node, wrapToList);
}

class IProxySource: public ISource {
protected:
    IProxySource(TPosition pos, ISource* src)
        : ISource(pos)
        , Source(src)
    {}

    void AllColumns() override {
        Y_DEBUG_ABORT_UNLESS(Source);
        return Source->AllColumns();
    }

    const TColumns* GetColumns() const override {
        Y_DEBUG_ABORT_UNLESS(Source);
        return Source->GetColumns();
    }

    void GetInputTables(TTableList& tableList) const override {
        Source->GetInputTables(tableList);
        ISource::GetInputTables(tableList);
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        Y_DEBUG_ABORT_UNLESS(Source);
        const TString label(Source->GetLabel());
        Source->SetLabel(Label);
        const auto ret = Source->AddColumn(ctx, column);
        Source->SetLabel(label);
        return ret;
    }

    bool ShouldUseSourceAsColumn(const TString& source) const override {
        return Source->ShouldUseSourceAsColumn(source);
    }

    bool IsStream() const override {
        Y_DEBUG_ABORT_UNLESS(Source);
        return Source->IsStream();
    }

    EOrderKind GetOrderKind() const override {
        Y_DEBUG_ABORT_UNLESS(Source);
        return Source->GetOrderKind();
    }

    TWriteSettings GetWriteSettings() const override {
        Y_DEBUG_ABORT_UNLESS(Source);
        return Source->GetWriteSettings();
    }

protected:
    void SetSource(ISource* source) {
        Source = source;
    }

    ISource* Source;
};

class IRealSource: public ISource {
protected:
    IRealSource(TPosition pos)
        : ISource(pos)
    {
    }

    void AllColumns() override {
        Columns.SetAll();
    }

    const TColumns* GetColumns() const override {
        return &Columns;
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
        if (name && !column.CanBeType() && !Columns.IsColumnPossible(ctx, *name) && !IsAlias(EExprSeat::GroupBy, *name) && !IsAlias(EExprSeat::DistinctAggr, *name)) {
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
        auto result = FindMistypeIn(Columns.Real, name);
        if (!result) {
            auto result = FindMistypeIn(Columns.Artificial, name);
        }
        return result ? result : ISource::FindColumnMistype(name);
    }

protected:
    TColumns Columns;
};

class IComposableSource : private TNonCopyable {
public:
    virtual ~IComposableSource() = default;
    virtual void BuildProjectWindowDistinct(TNodePtr& blocks, TContext& ctx, bool ordered) = 0;
};

using TComposableSourcePtr = TIntrusivePtr<IComposableSource>;

class TMuxSource: public ISource {
public:
    TMuxSource(TPosition pos, TVector<TSourcePtr>&& sources)
        : ISource(pos)
        , Sources(std::move(sources))
    {
        YQL_ENSURE(Sources.size() > 1);
    }

    void AllColumns() final {
        for (auto& source: Sources) {
            source->AllColumns();
        }
    }

    const TColumns* GetColumns() const final {
        // Columns are equal in all sources. Return from the first one
        return Sources.front()->GetColumns();
    }

    void GetInputTables(TTableList& tableList) const final {
        for (auto& source: Sources) {
            source->GetInputTables(tableList);
        }
        ISource::GetInputTables(tableList);
    }

    bool IsStream() const final {
        return AnyOf(Sources, [] (const TSourcePtr& s) { return s->IsStream(); });
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        for (auto& source: Sources) {
            if (AsInner) {
                source->UseAsInner();
            }

            if (src) {
                src->AddDependentSource(source.Get());
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
        for (auto& source: Sources) {
            if (!source->AddColumn(ctx, column)) {
                return {};
            }
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) final {
        TNodePtr block;
        auto muxArgs = Y();
        for (size_t i = 0; i < Sources.size(); ++i) {
            auto& source = Sources[i];
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
        return new TMuxSource(Pos, CloneContainer(Sources));
    }

protected:
    TVector<TSourcePtr> Sources;
};

TSourcePtr BuildMuxSource(TPosition pos, TVector<TSourcePtr>&& sources) {
    return new TMuxSource(pos, std::move(sources));
}

class TSubqueryRefNode: public IRealSource {
public:
    TSubqueryRefNode(const TNodePtr& subquery, const TString& alias, int tupleIndex)
        : IRealSource(subquery->GetPos())
        , Subquery(subquery)
        , Alias(alias)
        , TupleIndex(tupleIndex)
    {
        YQL_ENSURE(subquery->GetSource());
    }

    ISource* GetSource() override {
        return this;
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        // independent subquery should not connect source
        Subquery->UseAsInner();
        if (!Subquery->Init(ctx, nullptr)) {
            return false;
        }
        Columns = *Subquery->GetSource()->GetColumns();
        Node = BuildAtom(Pos, Alias, TNodeFlags::Default);
        if (TupleIndex != -1) {
            Node = Y("Nth", Node, Q(ToString(TupleIndex)));
        }
        if (!Node->Init(ctx, src)) {
            return false;
        }
        if (src && Subquery->GetSource()->IsSelect()) {
            auto columnsPtr = &Columns;
            if (columnsPtr && (columnsPtr->All || columnsPtr->QualifiedAll || columnsPtr->List.size() == 1)) {
                Node = Y("SingleMember", Y("SqlAccess", Q("dict"), Y("Take", Node, Y("Uint64", Q("1"))), Y("Uint64", Q("0"))));
            } else {
                ctx.Error(Pos) << "Source used in expression should contain one concrete column";
                return false;
            }
        }
        TNodePtr sample;
        if (!BuildSamplingLambda(sample)) {
            return false;
        } else if (sample) {
            Node = Y("block", Q(Y(Y("let", Node, Y("OrderedFlatMap", Node, sample)), Y("return", Node))));
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override  {
        Y_UNUSED(ctx);
        return Node;
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
        return Subquery->GetSource()->IsStream();
    }

    void DoUpdateState() const override {
        State.Set(ENodeState::Const, true);
    }

    TAstNode* Translate(TContext& ctx) const override {
        Y_DEBUG_ABORT_UNLESS(Node);
        return Node->Translate(ctx);
    }

    TPtr DoClone() const final {
        return new TSubqueryRefNode(Subquery, Alias, TupleIndex);
    }

protected:
    TNodePtr Subquery;
    const TString Alias;
    const int TupleIndex;
    TNodePtr Node;
};

TNodePtr BuildSubqueryRef(TNodePtr subquery, const TString& alias, int tupleIndex) {
    return new TSubqueryRefNode(std::move(subquery), alias, tupleIndex);
}

class TInvalidSubqueryRefNode: public ISource {
public:
    TInvalidSubqueryRefNode(TPosition pos)
        : ISource(pos)
        , Pos(pos)
    {
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        Y_UNUSED(src);
        ctx.Error(Pos) << "Named subquery can not be used as a top level statement in libraries";
        return false;
    }

    TNodePtr Build(TContext& ctx) override {
        Y_UNUSED(ctx);
        return {};
    }

    TPtr DoClone() const final {
        return new TInvalidSubqueryRefNode(Pos);
    }

protected:
    const TPosition Pos;
};

TNodePtr BuildInvalidSubqueryRef(TPosition subqueryPos) {
    return new TInvalidSubqueryRefNode(subqueryPos);
}

class TTableSource: public IRealSource {
public:
    TTableSource(TPosition pos, const TTableRef& table, const TString& label)
        : IRealSource(pos)
        , Table(table)
        , FakeSource(BuildFakeSource(pos))
    {
        SetLabel(label.empty() ? Table.ShortName() : label);
    }

    void GetInputTables(TTableList& tableList) const override {
        tableList.push_back(Table);
        ISource::GetInputTables(tableList);
    }

    bool ShouldUseSourceAsColumn(const TString& source) const override {
        const auto& label = GetLabel();
        return source && source != label && !(label.StartsWith(source) && label[source.size()] == ':');
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        Columns.Add(column.GetColumnName(), column.GetCountHint(), column.IsArtificial(), column.IsReliable());
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
            TNodePtr samplingSeed) override
    {
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

        if (!samplingRate->Init(ctx, FakeSource.Get())) {
            return false;
        }

        samplingRate = PrepareSamplingRate(pos, sampleClause, samplingRate);

        auto sampleSettings = Q(Y(Q(modeName), Y("EvaluateAtom", Y("ToString", samplingRate)), Y("EvaluateAtom", Y("ToString", samplingSeed))));
        auto sampleOption = Q(Y(Q("sample"), sampleSettings));
        if (Table.Options) {
            if (!Table.Options->Init(ctx, this)) {
                return false;
            }
            Table.Options = L(Table.Options, sampleOption);
        } else {
            Table.Options = Y(sampleOption);
        }
        return true;
    }

    bool SetTableHints(TContext& ctx, TPosition pos, const TTableHints& hints, const TTableHints& contextHints) override {
        Y_UNUSED(ctx);
        TTableHints merged = contextHints;
        MergeHints(merged, hints);
        Table.Options = BuildInputOptions(pos, merged);
        return true;
    }

    bool SetViewName(TContext& ctx, TPosition pos, const TString& view) override {
        return Table.Keys->SetViewName(ctx, pos, view);
    }

    TNodePtr Build(TContext& ctx) override {
        if (!Table.Keys->Init(ctx, nullptr)) {
            return nullptr;
        }
        return AstNode(Table.RefName);
    }

    bool IsStream() const override {
        return IsStreamingService(Table.Service);
    }

    TPtr DoClone() const final {
        return new TTableSource(Pos, Table, GetLabel());
    }

    bool IsTableSource() const override {
        return true;
    }
protected:
    TTableRef Table;
private:
    const TSourcePtr FakeSource;
};

TSourcePtr BuildTableSource(TPosition pos, const TTableRef& table, const TString& label) {
    return new TTableSource(pos, table, label);
}

class TInnerSource: public IProxySource {
public:
    TInnerSource(TPosition pos, TNodePtr node, const TString& service, const TDeferredAtom& cluster, const TString& label)
        : IProxySource(pos, nullptr)
        , Node(node)
        , Service(service)
        , Cluster(cluster)
    {
        SetLabel(label);
    }

    bool SetSamplingOptions(TContext& ctx, TPosition pos, ESampleClause sampleClause, ESampleMode mode, TNodePtr samplingRate, TNodePtr samplingSeed) override {
        Y_UNUSED(ctx);
        SamplingPos = pos;
        SamplingClause = sampleClause;
        SamplingMode = mode;
        SamplingRate = samplingRate;
        SamplingSeed = samplingSeed;
        return true;
    }

    bool SetTableHints(TContext& ctx, TPosition pos, const TTableHints& hints, const TTableHints& contextHints) override {
        Y_UNUSED(ctx);
        HintsPos = pos;
        Hints = hints;
        ContextHints = contextHints;
        return true;
    }

    bool SetViewName(TContext& ctx, TPosition pos, const TString& view) override {
        Y_UNUSED(ctx);
        ViewPos = pos;
        View = view;
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
        auto source = Node->GetSource();
        if (!source) {
            NewSource = TryMakeSourceFromExpression(Pos, ctx, Service, Cluster, Node);
            source = NewSource.Get();
        }

        if (!source) {
            ctx.Error(Pos) << "Invalid inner source node";
            return false;
        }

        if (SamplingPos) {
            if (!source->SetSamplingOptions(ctx, *SamplingPos, SamplingClause, SamplingMode, SamplingRate, SamplingSeed)) {
                return false;
            }
        }

        if (ViewPos) {
            if (!source->SetViewName(ctx, *ViewPos, View)) {
                return false;
            }
        }

        if (HintsPos) {
            if (!source->SetTableHints(ctx, *HintsPos, Hints, ContextHints)) {
                return false;
            }
        }

        source->SetLabel(Label);
        if (!NewSource) {
            Node->UseAsInner();
            if (!Node->Init(ctx, nullptr)) {
                return false;
            }
        }

        SetSource(source);
        if (NewSource && !NewSource->Init(ctx, nullptr)) {
            return false;
        }

        return ISource::DoInit(ctx, source);
    }

    TNodePtr Build(TContext& ctx) override {
        Y_UNUSED(ctx);
        return NewSource ? NewSource->Build(ctx) : Node;
    }

    bool IsStream() const override {
        auto source = Node->GetSource();
        if (source) {
            return source->IsStream();
        }
        // NewSource will be built later in DoInit->TryMakeSourceFromExpression
        // where Service will be used in all situations
        // let's detect IsStream by Service value
        return IsStreamingService(Service);
    }

    TPtr DoClone() const final {
        return new TInnerSource(Pos, SafeClone(Node), Service, Cluster, GetLabel());
    }
protected:
    TNodePtr Node;
    TString Service;
    TDeferredAtom Cluster;
    TSourcePtr NewSource;

private:
    TMaybe<TPosition> SamplingPos;
    ESampleClause SamplingClause;
    ESampleMode SamplingMode;
    TNodePtr SamplingRate;
    TNodePtr SamplingSeed;

    TMaybe<TPosition> ViewPos;
    TString View;

    TMaybe<TPosition> HintsPos;
    TTableHints Hints;
    TTableHints ContextHints;
};

TSourcePtr BuildInnerSource(TPosition pos, TNodePtr node, const TString& service, const TDeferredAtom& cluster, const TString& label) {
    return new TInnerSource(pos, node, service, cluster, label);
}

static bool IsComparableExpression(TContext& ctx, const TNodePtr& expr, bool assume, const char* sqlConstruction) {
    if (assume && !expr->GetColumnName()) {
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
    if (expr->GetColumnName()) {
        return true;
    }
    if (expr->GetOpName().empty()) {
        ctx.Error(expr->GetPos()) << "You should use in " << sqlConstruction << " column name, qualified field, callable function or expression";
        return false;
    }
    return true;
}

/// \todo move to reduce.cpp? or mapreduce.cpp?
class TReduceSource: public IRealSource {
public:
    TReduceSource(TPosition pos,
        ReduceMode mode,
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
        , Mode(mode)
        , Source(std::move(source))
        , OrderBy(std::move(orderBy))
        , Keys(std::move(keys))
        , Args(std::move(args))
        , Udf(udf)
        , Having(having)
        , Settings(settings)
        , AssumeOrderBy(assumeOrderBy)
        , ListCall(listCall)
    {
        YQL_ENSURE(!Keys.empty());
        YQL_ENSURE(Source);
    }

    void GetInputTables(TTableList& tableList) const override {
        Source->GetInputTables(tableList);
        ISource::GetInputTables(tableList);
    }

    bool DoInit(TContext& ctx, ISource* src) final {
        if (AsInner) {
            Source->UseAsInner();
        }

        YQL_ENSURE(!src);
        if (!Source->Init(ctx, src)) {
            return false;
        }
        if (!Source->InitFilters(ctx)) {
            return false;
        }
        src = Source.Get();
        for (auto& key: Keys) {
            if (!key->Init(ctx, src)) {
                return false;
            }
            auto keyNamePtr = key->GetColumnName();
            YQL_ENSURE(keyNamePtr);
            if (!src->AddGroupKey(ctx, *keyNamePtr)) {
                return false;
            }
        }
        if (Having && !Having->Init(ctx, nullptr)) {
            return false;
        }

        /// SIN: verify reduce one argument
        if (Args.size() != 1) {
            ctx.Error(Pos) << "REDUCE requires exactly one UDF argument";
            return false;
        }
        if (!Args[0]->Init(ctx, src)) {
            return false;
        }

        for (auto orderSpec: OrderBy) {
            if (!orderSpec->OrderExpr->Init(ctx, src)) {
                return false;
            }
        }

        if (!Udf->Init(ctx, src)) {
            return false;
        }

        if (Udf->GetLabel().empty()) {
            Columns.SetAll();
        } else {
            Columns.Add(&Udf->GetLabel(), false);
        }

        const auto label = GetLabel();
        for (const auto& sortSpec: AssumeOrderBy) {
            auto& expr = sortSpec->OrderExpr;
            SetLabel(Source->GetLabel());
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
        auto input = Source->Build(ctx);
        if (!input) {
            return nullptr;
        }

        auto keysTuple = Y();
        if (Keys.size() == 1) {
            keysTuple = Y("Member", "row", BuildQuotedAtom(Pos, *Keys.back()->GetColumnName()));
        }
        else {
            for (const auto& key: Keys) {
                keysTuple = L(keysTuple, Y("Member", "row", BuildQuotedAtom(Pos, *key->GetColumnName())));
            }
            keysTuple = Q(keysTuple);
        }
        auto extractKey = Y("SqlExtractKey", "row", BuildLambda(Pos, Y("row"), keysTuple));
        auto extractKeyLambda = BuildLambda(Pos, Y("row"), extractKey);

        TNodePtr processPartitions;
        if (ListCall) {
            if (Mode != ReduceMode::ByAll) {
                ctx.Error(Pos) << "TableRows() must be used only with USING ALL";
                return nullptr;
            }

            TNodePtr expr = BuildAtom(Pos, "partitionStream");
            processPartitions = Y("SqlReduce", "partitionStream", BuildQuotedAtom(Pos, "byAllList", TNodeFlags::Default), Udf, expr);
        } else {
            switch (Mode) {
                case ReduceMode::ByAll: {
                    auto columnPtr = Args[0]->GetColumnName();
                    TNodePtr expr = BuildAtom(Pos, "partitionStream");
                    if (!columnPtr || *columnPtr != "*") {
                        expr = Y("Map", "partitionStream", BuildLambda(Pos, Y("keyPair"), Q(L(Y(),\
                            Y("Nth", "keyPair", Q(ToString("0"))),\
                            Y("Map", Y("Nth", "keyPair", Q(ToString("1"))), BuildLambda(Pos, Y("row"), Args[0]))))));
                    }
                    processPartitions = Y("SqlReduce", "partitionStream", BuildQuotedAtom(Pos, "byAll", TNodeFlags::Default), Udf, expr);
                    break;
                }
                case ReduceMode::ByPartition: {
                    processPartitions = Y("SqlReduce", "partitionStream", extractKeyLambda, Udf,
                        BuildLambda(Pos, Y("row"), Args[0]));
                    break;
                }
                default:
                    YQL_ENSURE(false, "Unexpected REDUCE mode");
            }
        }

        TNodePtr sortDirection;
        TNodePtr sortKeySelector;
        FillSortParts(OrderBy, sortDirection, sortKeySelector);
        if (!OrderBy.empty()) {
            sortKeySelector = BuildLambda(Pos, Y("row"), Y("SqlExtractKey", "row", sortKeySelector));
        }

        auto partitionByKey = Y(!ListCall && Mode == ReduceMode::ByAll ? "PartitionByKey" : "PartitionsByKeys", "core", extractKeyLambda,
            sortDirection, sortKeySelector, BuildLambda(Pos, Y("partitionStream"), processPartitions));

        auto inputLabel = ListCall ? "inputRowsList" : "core";
        auto block(Y(Y("let", inputLabel, input)));
        auto filter = Source->BuildFilter(ctx, inputLabel);
        if (filter) {
            block = L(block, Y("let", inputLabel, filter));
        }
        if (ListCall) {
            block = L(block, Y("let", "core", "inputRowsList"));
        }

        if (ctx.EnableSystemColumns) {
            block = L(block, Y("let", "core", Y("RemoveSystemMembers", "core")));
        }
        block = L(block, Y("let", "core", Y("AutoDemux", partitionByKey)));
        if (Having) {
            block = L(block, Y("let", "core",
                Y("Filter", "core", BuildLambda(Pos, Y("row"), Y("Coalesce", Having, Y("Bool", Q("false")))))
            ));
        }
        return Y("block", Q(L(block, Y("return", "core"))));
    }

    TNodePtr BuildSort(TContext& ctx, const TString& label) override {
        Y_UNUSED(ctx);
        if (AssumeOrderBy.empty()) {
            return nullptr;
        }

        return Y("let", label, BuildSortSpec(AssumeOrderBy, label, false, true));
    }

    EOrderKind GetOrderKind() const override {
        return AssumeOrderBy.empty() ? EOrderKind::None : EOrderKind::Assume;
    }

    TWriteSettings GetWriteSettings() const final {
        return Settings;
    }

    bool HasSelectResult() const final {
        return !Settings.Discard;
    }

    TPtr DoClone() const final {
        return new TReduceSource(Pos, Mode, Source->CloneSource(), CloneContainer(OrderBy),
            CloneContainer(Keys), CloneContainer(Args), SafeClone(Udf), SafeClone(Having), Settings,
            CloneContainer(AssumeOrderBy), ListCall);
    }
private:
    ReduceMode Mode;
    TSourcePtr Source;
    TVector<TSortSpecificationPtr> OrderBy;
    TVector<TNodePtr> Keys;
    TVector<TNodePtr> Args;
    TNodePtr Udf;
    TNodePtr Having;
    const TWriteSettings Settings;
    TVector<TSortSpecificationPtr> AssumeOrderBy;
    const bool ListCall;
};

TSourcePtr BuildReduce(TPosition pos,
    ReduceMode mode,
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

}

class TCompositeSelect: public IRealSource {
public:
    TCompositeSelect(TPosition pos, TSourcePtr source, TSourcePtr originalSource, const TWriteSettings& settings)
        : IRealSource(pos)
        , Source(std::move(source))
        , OriginalSource(std::move(originalSource))
        , Settings(settings)
    {
        YQL_ENSURE(Source);
    }

    void SetSubselects(TVector<TSourcePtr>&& subselects, TVector<TNodePtr>&& grouping, TVector<TNodePtr>&& groupByExpr) {
        Subselects = std::move(subselects);
        Grouping = std::move(grouping);
        GroupByExpr = std::move(groupByExpr);
        Y_DEBUG_ABORT_UNLESS(Subselects.size() > 1);
    }

    void GetInputTables(TTableList& tableList) const override {
        for (const auto& select: Subselects) {
            select->GetInputTables(tableList);
        }
        ISource::GetInputTables(tableList);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (AsInner) {
            Source->UseAsInner();
        }

        if (src) {
            src->AddDependentSource(Source.Get());
        }
        if (!Source->Init(ctx, src)) {
            return false;
        }
        if (!Source->InitFilters(ctx)) {
            return false;
        }

        if (!CalculateGroupingCols(ctx, src)) {
            return false;
        }

        auto origSrc = OriginalSource.Get();
        if (!origSrc->Init(ctx, src)) {
            return false;
        }

        if (origSrc->IsFlattenByColumns() || origSrc->IsFlattenColumns()) {
            Flatten = origSrc->IsFlattenByColumns() ?
                      origSrc->BuildFlattenByColumns("row") :
                      origSrc->BuildFlattenColumns("row");
            if (!Flatten || !Flatten->Init(ctx, src)) {
                return false;
            }
        }

        if (origSrc->IsFlattenByExprs()) {
            for (auto& expr : static_cast<ISource const*>(origSrc)->Expressions(EExprSeat::FlattenByExpr)) {
                if (!expr->Init(ctx, origSrc)) {
                    return false;
                }
            }
            PreFlattenMap = origSrc->BuildPreFlattenMap(ctx);
            if (!PreFlattenMap) {
                return false;
            }
        }

        for (const auto& select: Subselects) {
            select->SetLabel(Label);
            if (AsInner) {
                select->UseAsInner();
            }

            if (!select->Init(ctx, Source.Get())) {
                return false;
            }
        }

        TMaybe<size_t> groupingColumnsCount;
        size_t idx = 0;
        for (const auto& select : Subselects) {
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
        for (const auto& select: Subselects) {
            if (!select->AddColumn(ctx, column)) {
                return {};
            }
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        auto input = Source->Build(ctx);
        auto block(Y(Y("let", "composite", input)));

        bool ordered = ctx.UseUnordered(*this);
        if (PreFlattenMap) {
            block = L(block, Y("let", "composite", Y(ordered ? "OrderedFlatMap" : "FlatMap", "composite", BuildLambda(Pos, Y("row"), PreFlattenMap))));
        }
        if (Flatten) {
            block = L(block, Y("let", "composite", Y(ordered ? "OrderedFlatMap" : "FlatMap", "composite", BuildLambda(Pos, Y("row"), Flatten, "res"))));
        }
        auto filter = Source->BuildFilter(ctx, "composite");
        if (filter) {
            block = L(block, Y("let", "composite", filter));
        }

        TNodePtr compositeNode = Y("UnionAll");
        for (const auto& select: Subselects) {
            YQL_ENSURE(dynamic_cast<IComposableSource*>(select.Get()));
            auto addNode = select->Build(ctx);
            if (!addNode) {
                return nullptr;
            }
            compositeNode->Add(addNode);
        }

        block = L(block, Y("let", "core", compositeNode));
        YQL_ENSURE(!Subselects.empty());
        dynamic_cast<IComposableSource*>(Subselects.front().Get())->BuildProjectWindowDistinct(block, ctx, false);
        return Y("block", Q(L(block, Y("return", "core"))));
    }

    bool IsGroupByColumn(const TString& column) const override {
        YQL_ENSURE(!GroupingCols.empty());
        return GroupingCols.contains(column);
    }

    const TSet<TString>& GetGroupingCols() const {
        return GroupingCols;
    }

    TNodePtr BuildSort(TContext& ctx, const TString& label) override {
        return Subselects.front()->BuildSort(ctx, label);
    }

    EOrderKind GetOrderKind() const override {
        return Subselects.front()->GetOrderKind();
    }

    const TColumns* GetColumns() const override{
        return Subselects.front()->GetColumns();
    }

    ISource* RealSource() const {
        return Source.Get();
    }

    TWriteSettings GetWriteSettings() const override {
        return Settings;
    }

    bool HasSelectResult() const override {
        return !Settings.Discard;
    }

    TNodePtr DoClone() const final {
        auto newSource = MakeIntrusive<TCompositeSelect>(Pos, Source->CloneSource(), OriginalSource->CloneSource(), Settings);
        newSource->SetSubselects(CloneContainer(Subselects), CloneContainer(Grouping), CloneContainer(GroupByExpr));
        return newSource;
    }
private:
    bool CalculateGroupingCols(TContext& ctx, ISource* initSrc) {
        auto origSrc = OriginalSource->CloneSource();
        if (!origSrc->Init(ctx, initSrc)) {
            return false;
        }

        bool hasError = false;
        for (auto& expr: GroupByExpr) {
            if (!expr->Init(ctx, origSrc.Get()) || !IsComparableExpression(ctx, expr, false, "GROUP BY")) {
                hasError = true;
            }
        }
        if (!origSrc->AddExpressions(ctx, GroupByExpr, EExprSeat::GroupBy)) {
            hasError = true;
        }

        YQL_ENSURE(!Grouping.empty());
        for (auto& grouping : Grouping) {
            TString keyColumn;
            if (!InitAndGetGroupKey(ctx, grouping, origSrc.Get(), "grouping sets", keyColumn)) {
                hasError = true;
            } else if (!keyColumn.empty()) {
                GroupingCols.insert(keyColumn);
            }
        }

        return !hasError;
    }

    TSourcePtr Source;
    TSourcePtr OriginalSource;
    TNodePtr Flatten;
    TNodePtr PreFlattenMap;
    const TWriteSettings Settings;
    TVector<TSourcePtr> Subselects;
    TVector<TNodePtr> Grouping;
    TVector<TNodePtr> GroupByExpr;
    TSet<TString> GroupingCols;
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
}

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
        bool selectStream,
        const TWriteSettings& settings,
        TColumnsSets&& uniqueSets,
        TColumnsSets&& distinctSets
    )
        : IRealSource(pos)
        , Source(std::move(source))
        , GroupByExpr(groupByExpr)
        , GroupBy(groupBy)
        , AssumeSorted(assumeSorted)
        , CompactGroupBy(compactGroupBy)
        , GroupBySuffix(groupBySuffix)
        , OrderBy(orderBy)
        , Having(having)
        , WinSpecs(winSpecs)
        , Terms(terms)
        , Without(without)
        , Distinct(distinct)
        , LegacyHoppingWindowSpec(legacyHoppingWindowSpec)
        , SelectStream(selectStream)
        , Settings(settings)
        , UniqueSets(std::move(uniqueSets))
        , DistinctSets(std::move(distinctSets))
    {
    }

    void AllColumns() override {
        if (!OrderByInit) {
            Columns.SetAll();
        }
    }

    void GetInputTables(TTableList& tableList) const override {
        Source->GetInputTables(tableList);
        ISource::GetInputTables(tableList);
    }

    size_t GetGroupingColumnsCount() const override {
        return Source->GetGroupingColumnsCount();
    }

    bool DoInit(TContext& ctx, ISource* initSrc) override {
        if (AsInner) {
            Source->UseAsInner();
        }

        if (!Source->Init(ctx, initSrc)) {
            return false;
        }
        if (SelectStream && !Source->IsStream()) {
            ctx.Error(Pos) << "SELECT STREAM is unsupported for non-streaming sources";
            return false;
        }

        auto src = Source.Get();
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

        src->SetCompactGroupBy(CompactGroupBy);
        src->SetGroupBySuffix(GroupBySuffix);

        for (auto& term: Terms) {
            term->CollectPreaggregateExprs(ctx, *src, DistinctAggrExpr);
        }

        if (Having) {
            Having->CollectPreaggregateExprs(ctx, *src, DistinctAggrExpr);
        }

        for (auto& expr: GroupByExpr) {
            if (auto sessionWindow = dynamic_cast<TSessionWindow*>(expr.Get())) {
                if (Source->IsStream()) {
                    ctx.Error(Pos) << "SessionWindow is unsupported for streaming sources";
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
            DistinctAggrExpr.insert(DistinctAggrExpr.end(), distinctAggrsInGroupBy.begin(), distinctAggrsInGroupBy.end());

            if (!expr->Init(ctx, src) || !IsComparableExpression(ctx, expr, false, "GROUP BY")) {
                hasError = true;
            }
        }
        if (hasError || !src->AddExpressions(ctx, GroupByExpr, EExprSeat::GroupBy)) {
            return false;
        }

        for (auto& expr: DistinctAggrExpr) {
            if (!expr->Init(ctx, src)) {
                hasError = true;
            }
        }
        if (hasError || !src->AddExpressions(ctx, DistinctAggrExpr, EExprSeat::DistinctAggr)) {
            return false;
        }

        /// grouped expressions are available in filters
        if (!Source->InitFilters(ctx)) {
            return false;
        }

        for (auto& expr: GroupBy) {
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

        if (Having && !Having->Init(ctx, src)) {
            return false;
        }
        src->AddWindowSpecs(WinSpecs);

        const bool isJoin = Source->GetJoin();
        if (!InitSelect(ctx, src, isJoin, hasError)) {
            return false;
        }

        src->FinishColumns();
        auto aggRes = src->BuildAggregation("core", ctx);
        if (!aggRes.second) {
            return false;
        }

        Aggregate = aggRes.first;
        if (src->IsFlattenByColumns() || src->IsFlattenColumns()) {
            Flatten = src->IsFlattenByColumns() ?
                src->BuildFlattenByColumns("row") :
                src->BuildFlattenColumns("row");
            if (!Flatten || !Flatten->Init(ctx, src)) {
                return false;
            }
        }

        if (src->IsFlattenByExprs()) {
            PreFlattenMap = src->BuildPreFlattenMap(ctx);
            if (!PreFlattenMap) {
                return false;
            }
        }

        if (GroupByExpr || DistinctAggrExpr) {
            PreaggregatedMap = src->BuildPreaggregatedMap(ctx);
            if (!PreaggregatedMap) {
                return false;
            }
        }
        if (Aggregate) {
            if (!Aggregate->Init(ctx, src)) {
                return false;
            }
            if (Having) {
                Aggregate = Y(
                    "Filter",
                    Aggregate,
                    BuildLambda(Pos, Y("row"), Y("Coalesce", Having, Y("Bool", Q("false"))))
                );
            }
        } else if (Having) {
            if (Distinct) {
                Aggregate = Y(
                    "Filter",
                    "core",
                    BuildLambda(Pos, Y("row"), Y("Coalesce", Having, Y("Bool", Q("false"))))
                );
                ctx.Warning(Having->GetPos(), TIssuesIds::YQL_HAVING_WITHOUT_AGGREGATION_IN_SELECT_DISTINCT)
                    << "The usage of HAVING without aggregations with SELECT DISTINCT is non-standard and will stop working soon. Please use WHERE instead.";
            } else {
                ctx.Error(Having->GetPos()) << "HAVING with meaning GROUP BY () should be with aggregation function.";
                return false;
            }
        } else if (!Distinct && !GroupBy.empty()) {
            ctx.Error(Pos) << "No aggregations were specified";
            return false;
        }
        if (hasError) {
            return false;
        }

        if (src->IsCalcOverWindow()) {
            if (src->IsExprSeat(EExprSeat::WindowPartitionBy, EExprType::WithExpression)) {
                PrewindowMap = src->BuildPrewindowMap(ctx);
                if (!PrewindowMap) {
                    return false;
                }
            }
            CalcOverWindow = src->BuildCalcOverWindow(ctx, "core");
            if (!CalcOverWindow || !CalcOverWindow->Init(ctx, src)) {
                return false;
            }
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        auto input = Source->Build(ctx);
        if (!input) {
            return nullptr;
        }

        auto block(Y(Y("let", "core", input)));

        if (Source->HasMatchRecognize()) {
            if (auto matchRecognize = Source->BuildMatchRecognize(ctx, "core")) {
                //use unique name match_recognize to find this block easily in unit tests
                block = L(block, Y("let", "match_recognize", matchRecognize));
                //then bind to the conventional name
                block = L(block, Y("let", "core", "match_recognize"));
            } else {
                return nullptr;
            }
        }

        bool ordered = ctx.UseUnordered(*this);
        if (PreFlattenMap) {
            block = L(block, Y("let", "core", Y(ordered ? "OrderedFlatMap" : "FlatMap", "core", BuildLambda(Pos, Y("row"), PreFlattenMap))));
        }
        if (Flatten) {
            block = L(block, Y("let", "core", Y(ordered ? "OrderedFlatMap" : "FlatMap", "core", BuildLambda(Pos, Y("row"), Flatten, "res"))));
        }
        if (PreaggregatedMap) {
            block = L(block, Y("let", "core", PreaggregatedMap));
            if (Source->IsCompositeSource() && !Columns.QualifiedAll) {
                block = L(block, Y("let", "preaggregated", "core"));
            }
        } else if (Source->IsCompositeSource() && !Columns.QualifiedAll) {
            block = L(block, Y("let", "origcore", "core"));
        }
        auto filter = Source->BuildFilter(ctx, "core");
        if (filter) {
            block = L(block, Y("let", "core", filter));
        }
        if (Aggregate) {
            block = L(block, Y("let", "core", Aggregate));
            ordered = false;
        }

        const bool haveCompositeTerms = Source->IsCompositeSource() && !Columns.All && !Columns.QualifiedAll && !Columns.List.empty();
        if (haveCompositeTerms) {
            // column order does not matter here - it will be set in projection
            YQL_ENSURE(Aggregate);
            block = L(block, Y("let", "core", Y("Map", "core", BuildLambda(Pos, Y("row"), CompositeTerms, "row"))));
        }

        if (auto grouping = Source->BuildGroupingColumns("core")) {
            block = L(block, Y("let", "core", grouping));
        }

        if (!Source->GetCompositeSource()) {
            BuildProjectWindowDistinct(block, ctx, ordered);
        }

        return Y("block", Q(L(block, Y("return", "core"))));
    }

    void BuildProjectWindowDistinct(TNodePtr& block, TContext& ctx, bool ordered) override {
        if (PrewindowMap) {
            block = L(block, Y("let", "core", PrewindowMap));
        }
        if (CalcOverWindow) {
            block = L(block, Y("let", "core", CalcOverWindow));
        }

        block = L(block, Y("let", "core", Y("PersistableRepr", BuildSqlProject(ctx, ordered))));

        if (Distinct) {
            block = L(block, Y("let", "core", Y("PersistableRepr", Y("SqlAggregateAll", Y("RemoveSystemMembers", "core")))));
        }
    }

    TNodePtr BuildSort(TContext& ctx, const TString& label) override {
        Y_UNUSED(ctx);
        if (OrderBy.empty()) {
            return nullptr;
        }

        auto sorted = BuildSortSpec(OrderBy, label, false, AssumeSorted);
        if (ExtraSortColumns.empty()) {
            return Y("let", label, sorted);
        }
        auto body = Y();
        for (const auto& [column, _] : ExtraSortColumns) {
            body = L(body, Y("let", "row", Y("RemoveMember", "row", Q(column))));
        }
        body = L(body, Y("let", "res", "row"));
        return Y("let", label, Y("OrderedMap", sorted, BuildLambda(Pos, Y("row"), body, "res")));
    }

    TNodePtr BuildCleanupColumns(TContext& ctx, const TString& label) override {
        TNodePtr cleanup;
        if (ctx.EnableSystemColumns && ctx.Settings.Mode != NSQLTranslation::ESqlMode::LIMITED_VIEW) {
            if (Columns.All) {
                cleanup = Y("let", label, Y("RemoveSystemMembers", label));
            } else if (!Columns.List.empty()) {
                const bool isJoin = Source->GetJoin();
                if (!isJoin && Columns.QualifiedAll) {
                    if (ctx.SimpleColumns) {
                        cleanup = Y("let", label, Y("RemoveSystemMembers", label));
                    } else {
                        TNodePtr members;
                        for (auto& term: Terms) {
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
        return !Settings.Discard;
    }

    bool IsStream() const override {
        return Source->IsStream();
    }

    EOrderKind GetOrderKind() const override {
        if (OrderBy.empty()) {
            return EOrderKind::None;
        }
        return AssumeSorted ? EOrderKind::Assume : EOrderKind::Sort;
    }

    TWriteSettings GetWriteSettings() const override {
        return Settings;
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        const bool aggregated = Source->HasAggregations() || Distinct;
        if (OrderByInit && (Source->GetJoin() || !aggregated)) {
            // ORDER BY will try to find column not only in projection items, but also in Source.
            // ```SELECT a, b FROM T ORDER BY c``` should work if c is present in T
            const bool reliable = column.IsReliable();
            column.SetAsNotReliable();
            auto maybeExist = IRealSource::AddColumn(ctx, column);
            if (reliable && !Source->GetJoin()) {
                column.ResetAsReliable();
            }
            if (!maybeExist || !maybeExist.GetRef()) {
                maybeExist = Source->AddColumn(ctx, column);
            }
            if (!maybeExist.Defined()) {
                return maybeExist;
            }
            if (!aggregated && column.GetColumnName() && IsMissingInProjection(ctx, column)) {
                ExtraSortColumns[FullColumnName(column)] = &column;
            }
            return maybeExist;
        }

        return IRealSource::AddColumn(ctx, column);
    }

    bool IsMissingInProjection(TContext& ctx, const TColumnNode& column) const {
        TString columnName = FullColumnName(column);
        if (Columns.Real.contains(columnName) || Columns.Artificial.contains(columnName)) {
            return false;
        }

        if (!Columns.IsColumnPossible(ctx, columnName)) {
            return true;
        }

        for (auto without: Without) {
            auto name = *without->GetColumnName();
            if (Source && Source->GetJoin()) {
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
        if (Without) {
            for (auto without: Without) {
                auto name = *without->GetColumnName();
                if (Source && Source->GetJoin()) {
                    name = DotJoin(*without->GetSourceName(), name);
                }
                terms = L(terms, Y("let", "row", Y("RemoveMember", "row", Q(name))));
            }
        }

        if (Source) {
            for (auto column : Source->GetTmpWindowColumns()) {
                terms = L(terms, Y("let", "row", Y("RemoveMember", "row", Q(column))));
            }
        }

        return terms;
    }

    TNodePtr DoClone() const final {
        return new TSelectCore(Pos, Source->CloneSource(), CloneContainer(GroupByExpr),
                CloneContainer(GroupBy), CompactGroupBy, GroupBySuffix, AssumeSorted, CloneContainer(OrderBy),
                SafeClone(Having), CloneContainer(WinSpecs), SafeClone(LegacyHoppingWindowSpec),
                CloneContainer(Terms), Distinct, Without, SelectStream, Settings, TColumnsSets(UniqueSets), TColumnsSets(DistinctSets));
    }

private:
    bool InitSelect(TContext& ctx, ISource* src, bool isJoin, bool& hasError) {
        for (auto& [name, winSpec] : WinSpecs) {
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

        if (LegacyHoppingWindowSpec) {
            if (!LegacyHoppingWindowSpec->TimeExtractor->Init(ctx, src)) {
                hasError = true;
            }
            src->SetLegacyHoppingWindowSpec(LegacyHoppingWindowSpec);
        }

        for (auto& term: Terms) {
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
                    Columns.QualifiedAll = true;
                    label = DotJoin(*source, "*");
                } else if (column) {
                    label = isJoin && source && *source ? DotJoin(*source, *column) : *column;
                } else {
                    label = Columns.AddUnnamed();
                    hasName = false;
                    if (ctx.WarnUnnamedColumns) {
                        ctx.Warning(term->GetPos(), TIssuesIds::YQL_UNNAMED_COLUMN)
                            << "Autogenerated column name " << label << " will be used for expression";
                    }
                }
            }
            if (hasName && !Columns.Add(&label, false, false, true)) {
                ctx.Error(Pos) << "Duplicate column: " << label;
                hasError = true;
            }
        }

        CompositeTerms = Y();
        if (!hasError && Source->IsCompositeSource() && !Columns.All && !Columns.QualifiedAll && !Columns.List.empty()) {
            auto compositeSrcPtr = static_cast<TCompositeSelect*>(Source->GetCompositeSource());
            if (compositeSrcPtr) {
                const auto& groupings = compositeSrcPtr->GetGroupingCols();
                for (const auto& column: groupings) {
                    if (Source->IsGroupByColumn(column)) {
                        continue;
                    }
                    const TString tableName = (GroupByExpr || DistinctAggrExpr) ? "preaggregated" : "origcore";
                    CompositeTerms = L(CompositeTerms, Y("let", "row", Y("AddMember", "row", BuildQuotedAtom(Pos, column), Y("Nothing", Y("MatchType",
                        Y("StructMemberType", Y("ListItemType", Y("TypeOf", tableName)), Q(column)),
                        Q("Optional"), Y("lambda", Q(Y("item")), "item"), Y("lambda", Q(Y("item")), Y("OptionalType", "item")))))));
                }
            }
        }

        for (auto iter: WinSpecs) {
            auto winSpec = *iter.second;
            for (auto orderSpec: winSpec.OrderBy) {
                if (!orderSpec->OrderExpr->Init(ctx, src)) {
                    hasError = true;
                }
            }
        }

        if (Columns.All || Columns.QualifiedAll) {
            Source->AllColumns();
        }
        for (const auto& without: Without) {
            auto namePtr = without->GetColumnName();
            auto sourcePtr = without->GetSourceName();
            YQL_ENSURE(namePtr && *namePtr);
            if (isJoin && !(sourcePtr && *sourcePtr)) {
                ctx.Error(without->GetPos()) << "Expected correlation name for WITHOUT in JOIN";
                hasError = true;
                continue;
            }
        }
        if (Having && !Having->Init(ctx, src)) {
            hasError = true;
        }
        if (!src->IsCompositeSource() && !Columns.All && src->HasAggregations()) {
            WarnIfAliasFromSelectIsUsedInGroupBy(ctx, Terms, GroupBy, GroupByExpr);

            /// verify select aggregation compatibility
            TVector<TNodePtr> exprs(Terms);
            if (Having) {
                exprs.push_back(Having);
            }
            for (const auto& iter: WinSpecs) {
                for (const auto& sortSpec: iter.second->OrderBy) {
                    exprs.push_back(sortSpec->OrderExpr);
                }
            }
            if (!ValidateAllNodesForAggregation(ctx, exprs)) {
                hasError = true;
            }
        }
        const auto label = GetLabel();
        for (const auto& sortSpec: OrderBy) {
            auto& expr = sortSpec->OrderExpr;
            SetLabel(Source->GetLabel());
            OrderByInit = true;
            if (!expr->Init(ctx, this)) {
                hasError = true;
                continue;
            }
            OrderByInit = false;
            if (!IsComparableExpression(ctx, expr, AssumeSorted, AssumeSorted ? "ASSUME ORDER BY" : "ORDER BY")) {
                hasError = true;
                continue;
            }
        }
        SetLabel(label);

        return !hasError;
    }

    TNodePtr PrepareJoinCoalesce(TContext& ctx, const TNodePtr& base, bool multipleQualifiedAll, const TVector<TString>& coalesceLabels) {
        const bool isJoin = Source->GetJoin();
        const bool needCoalesce = isJoin && ctx.SimpleColumns &&
            (Columns.All || multipleQualifiedAll || ctx.CoalesceJoinKeysOnQualifiedAll);

        if (!needCoalesce) {
            return base;
        }

        auto terms = base;
        const auto& sameKeyMap = Source->GetJoin()->GetSameKeysMap();
        if (sameKeyMap) {
            terms = L(terms, Y("let", "flatSameKeys", "row"));
            for (const auto& [key, sources]: sameKeyMap) {
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
        const bool isJoin = Source->GetJoin();

        if (Columns.All) {
            YQL_ENSURE(Columns.List.empty());
            auto terms = PrepareWithout(Y());
            auto options = Y();
            if (isJoin && ctx.SimpleColumns) {
                terms = PrepareJoinCoalesce(ctx, terms, false, Source->GetJoin()->GetJoinLabels());

                auto members = Y();
                for (auto& source : Source->GetJoin()->GetJoinLabels()) {
                    YQL_ENSURE(!source.empty());
                    members = L(members, BuildQuotedAtom(Pos, source + "."));
                }
                if (GroupByExpr.empty() || ctx.BogousStarInGroupByOverJoin) {
                    terms = L(terms, Y("let", "res", Y("DivePrefixMembers", "row", Q(members))));
                } else {
                    auto groupExprStruct = Y("AsStruct");
                    for (auto node : GroupByExpr) {
                        auto label = node->GetLabel();
                        YQL_ENSURE(label);
                        if (Source->IsGroupByColumn(label)) {
                            auto name = BuildQuotedAtom(Pos, label);
                            groupExprStruct = L(groupExprStruct, Q(Y(name, Y("Member", "row", name))));
                        }
                    }
                    auto groupColumnsStruct = Y("DivePrefixMembers", "row", Q(members));

                    terms = L(terms, Y("let", "res", Y("FlattenMembers", Q(Y(BuildQuotedAtom(Pos, ""), groupExprStruct)),
                        Q(Y(BuildQuotedAtom(Pos, ""), groupColumnsStruct)))));
                }
                options = L(options, Q(Y(Q("divePrefix"), Q(members))));
            } else {
                terms = L(terms, Y("let", "res", "row"));
            }
            sqlProjectArgs = L(sqlProjectArgs, Y("SqlProjectStarItem", "projectCoreType", BuildQuotedAtom(Pos, ""),  BuildLambda(Pos, Y("row"), terms, "res"), Q(options)));
        } else {
            YQL_ENSURE(!Columns.List.empty());
            YQL_ENSURE(Columns.List.size() == Terms.size());

            TVector<TString> coalesceLabels;
            bool multipleQualifiedAll = false;

            if (isJoin && ctx.SimpleColumns) {
                THashSet<TString> starTerms;
                for (auto& term: Terms) {
                    if (term->IsAsterisk()) {
                        auto sourceName = term->GetSourceName();
                        YQL_ENSURE(*sourceName && !sourceName->empty());
                        YQL_ENSURE(Columns.QualifiedAll);
                        starTerms.insert(*sourceName);
                    }
                }

                TVector<TString> matched;
                TVector<TString> unmatched;
                for (auto& label : Source->GetJoin()->GetJoinLabels()) {
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

            auto column = Columns.List.begin();
            auto isNamedColumn = Columns.NamedColumns.begin();
            for (auto& term: Terms) {
                auto sourceName = term->GetSourceName();
                if (!term->IsAsterisk()) {
                    auto body = Y();
                    body = L(body, Y("let", "res", term));
                    TPosition lambdaPos = Pos;
                    TPosition aliasPos = Pos;
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
                            members = L(members, BuildQuotedAtom(Pos, *sourceName + "."));
                            if (ctx.SimpleColumns) {
                                options = L(options, Q(Y(Q("divePrefix"), Q(members))));
                            }
                            members = Y(ctx.SimpleColumns ? "DivePrefixMembers" : "SelectMembers", "row", Q(members));
                        } else {
                            auto prefix = BuildQuotedAtom(Pos, ctx.SimpleColumns ? "" : *sourceName + ".");
                            members = L(members, Q(Y(prefix, "row")));
                            if (!ctx.SimpleColumns) {
                                options = L(options, Q(Y(Q("addPrefix"), prefix)));
                            }
                        }

                        terms = L(terms, Y("let", "res", members));
                    }
                    sqlProjectArgs = L(sqlProjectArgs, Y("SqlProjectStarItem", "projectCoreType", BuildQuotedAtom(Pos, *sourceName), BuildLambda(Pos, Y("row"), terms, "res"), Q(options)));
                }
                ++column;
                ++isNamedColumn;
            }
        }

        for (const auto& [columnName, column]: ExtraSortColumns) {
            auto body = Y();
            body = L(body, Y("let", "res", column));
            TPosition pos = column->GetPos();
            auto projectItem = Y("SqlProjectItem", "projectCoreType", BuildQuotedAtom(pos, columnName), BuildLambda(pos, Y("row"), body, "res"));
            sqlProjectArgs = L(sqlProjectArgs, projectItem);
        }

        auto block(Y(Y("let", "projectCoreType", Y("TypeOf", "core"))));
        block = L(block, Y("let", "core", Y(ordered ? "OrderedSqlProject" : "SqlProject", "core", Q(sqlProjectArgs))));
        if (!(UniqueSets.empty() && DistinctSets.empty())) {
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

            MakeUniqueHint(block, DistinctSets, true);
            MakeUniqueHint(block, UniqueSets, false);
        }

        return Y("block", Q(L(block, Y("return", "core"))));
    }

private:
    TSourcePtr Source;
    TVector<TNodePtr> GroupByExpr;
    TVector<TNodePtr> DistinctAggrExpr;
    TVector<TNodePtr> GroupBy;
    bool AssumeSorted = false;
    bool CompactGroupBy = false;
    TString GroupBySuffix;
    TVector<TSortSpecificationPtr> OrderBy;
    TNodePtr Having;
    TWinSpecs WinSpecs;
    TNodePtr Flatten;
    TNodePtr PreFlattenMap;
    TNodePtr PreaggregatedMap;
    TNodePtr PrewindowMap;
    TNodePtr Aggregate;
    TNodePtr CalcOverWindow;
    TNodePtr CompositeTerms;
    TVector<TNodePtr> Terms;
    TVector<TNodePtr> Without;
    const bool Distinct;
    bool OrderByInit = false;
    TLegacyHoppingWindowSpecPtr LegacyHoppingWindowSpec;
    const bool SelectStream;
    const TWriteSettings Settings;
    const TColumnsSets UniqueSets, DistinctSets;
    TMap<TString, TNodePtr> ExtraSortColumns;
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
        const TVector<TSortSpecificationPtr>& assumeOrderBy
    )
        : IRealSource(pos)
        , Source(std::move(source))
        , With(with)
        , WithExtFunction(withExtFunction)
        , Terms(std::move(terms))
        , ListCall(listCall)
        , ProcessStream(processStream)
        , Settings(settings)
        , AssumeOrderBy(assumeOrderBy)
    {
    }

    void GetInputTables(TTableList& tableList) const override {
        Source->GetInputTables(tableList);
        ISource::GetInputTables(tableList);
    }

    bool DoInit(TContext& ctx, ISource* initSrc) override {
        if (AsInner) {
            Source->UseAsInner();
        }

        if (!Source->Init(ctx, initSrc)) {
            return false;
        }

        if (ProcessStream && !Source->IsStream()) {
            ctx.Error(Pos) << "PROCESS STREAM is unsupported for non-streaming sources";
            return false;
        }

        auto src = Source.Get();
        if (!With) {
            src->AllColumns();
            Columns.SetAll();
            src->FinishColumns();
            return true;
        }

        /// grouped expressions are available in filters
        if (!Source->InitFilters(ctx)) {
            return false;
        }

        TSourcePtr fakeSource = nullptr;
        if (ListCall && !WithExtFunction) {
            fakeSource = BuildFakeSource(src->GetPos());
            src->AllColumns();
        }

        auto processSource = fakeSource != nullptr ? fakeSource.Get() : src;
        Y_DEBUG_ABORT_UNLESS(processSource != nullptr);
        if (!With->Init(ctx, processSource)) {
            return false;
        }
        if (With->GetLabel().empty()) {
            Columns.SetAll();
        } else {
            if (ListCall) {
                ctx.Error(With->GetPos()) << "Label is not allowed to use with TableRows()";
                return false;
            }
            Columns.Add(&With->GetLabel(), false);
        }

        bool hasError = false;

        TNodePtr produce;
        if (WithExtFunction) {
            produce = Y();
        } else {
            TString processCall = (ListCall ? "SqlProcess" : "Apply");
            produce = Y(processCall, With);
        }
        TMaybe<ui32> listPosIndex;
        ui32 termIndex = 0;
        for (auto& term: Terms) {
            if (!term->GetLabel().empty()) {
                ctx.Error(term->GetPos()) << "Labels are not allowed for PROCESS terms";
                hasError = true;
                continue;
            }

            if (!term->Init(ctx, processSource)) {
                hasError = true;
                continue;
            }

            if (ListCall) {
                if (auto atom = dynamic_cast<TTableRows*>(term.Get())) {
                    listPosIndex = termIndex;
                }
            }
            ++termIndex;

            produce = L(produce, term);
        }

        if (hasError) {
            return false;
        }

        if (ListCall && !WithExtFunction) {
            YQL_ENSURE(listPosIndex.Defined());
            produce = L(produce, Q(ToString(*listPosIndex)));
        }

        if (!produce->Init(ctx, src)) {
            hasError = true;
        }

        if (!(WithExtFunction && Terms.empty())) {
            TVector<TNodePtr>(1, produce).swap(Terms);
        }

        src->FinishColumns();

        const auto label = GetLabel();
        for (const auto& sortSpec: AssumeOrderBy) {
            auto& expr = sortSpec->OrderExpr;
            SetLabel(Source->GetLabel());
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
        auto input = Source->Build(ctx);
        if (!input) {
            return nullptr;
        }

        if (!With) {
            auto res = input;
            if (ctx.EnableSystemColumns) {
                res = Y("RemoveSystemMembers", res);
            }

            return res;
        }

        TString inputLabel = ListCall ? "inputRowsList" : "core";

        auto block(Y(Y("let", inputLabel, input)));

        auto filter = Source->BuildFilter(ctx, inputLabel);
        if (filter) {
            block = L(block, Y("let", inputLabel, filter));
        }

        if (WithExtFunction) {
            auto preTransform = Y("RemoveSystemMembers", inputLabel);
            if (Terms.size() > 0) {
                preTransform = Y("Map", preTransform, BuildLambda(Pos, Y("row"), Q(Terms[0])));
            }
            block = L(block, Y("let", inputLabel, preTransform));
            block = L(block, Y("let", "transform", With));
            block = L(block, Y("let", "core", Y("Apply", "transform", inputLabel)));
        } else if (ListCall) {
            block = L(block, Y("let", "core", Terms[0]));
        } else {
            auto terms = BuildColumnsTerms(ctx);
            block = L(block, Y("let", "core", Y(ctx.UseUnordered(*this) ? "OrderedFlatMap" : "FlatMap", "core", BuildLambda(Pos, Y("row"), terms, "res"))));
        }
        block = L(block, Y("let", "core", Y("AutoDemux", Y("PersistableRepr", "core"))));
        return Y("block", Q(L(block, Y("return", "core"))));
    }

    TNodePtr BuildSort(TContext& ctx, const TString& label) override {
        Y_UNUSED(ctx);
        if (AssumeOrderBy.empty()) {
            return nullptr;
        }

        return Y("let", label, BuildSortSpec(AssumeOrderBy, label, false, true));
    }

    EOrderKind GetOrderKind() const override {
        if (!With) {
            return EOrderKind::Passthrough;
        }
        return AssumeOrderBy.empty() ? EOrderKind::None : EOrderKind::Assume;
    }

    bool IsSelect() const override {
        return false;
    }

    bool HasSelectResult() const override {
        return !Settings.Discard;
    }

    bool IsStream() const override {
        return Source->IsStream();
    }

    TWriteSettings GetWriteSettings() const override {
        return Settings;
    }

    TNodePtr DoClone() const final {
        return new TProcessSource(Pos, Source->CloneSource(), SafeClone(With), WithExtFunction,
            CloneContainer(Terms), ListCall, ProcessStream, Settings, CloneContainer(AssumeOrderBy));
    }

private:
    TNodePtr BuildColumnsTerms(TContext& ctx) {
        Y_UNUSED(ctx);
        TNodePtr terms;
        Y_DEBUG_ABORT_UNLESS(Terms.size() == 1);
        if (Columns.All) {
            terms = Y(Y("let", "res", Y("ToSequence", Terms.front())));
        } else {
            Y_DEBUG_ABORT_UNLESS(Columns.List.size() == Terms.size());
            terms = L(Y(), Y("let", "res",
                L(Y("AsStructUnordered"), Q(Y(BuildQuotedAtom(Pos, Columns.List.front()), Terms.front())))));
            terms = L(terms, Y("let", "res", Y("Just", "res")));
        }
        return terms;
    }

private:
    TSourcePtr Source;
    TNodePtr With;
    const bool WithExtFunction;
    TVector<TNodePtr> Terms;
    const bool ListCall;
    const bool ProcessStream;
    const TWriteSettings Settings;
    TVector<TSortSpecificationPtr> AssumeOrderBy;
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
    const TVector<TSortSpecificationPtr>& assumeOrderBy
) {
    return new TProcessSource(pos, std::move(source), with, withExtFunction, std::move(terms), listCall, processStream, settings, assumeOrderBy);
}

class TNestedProxySource: public IProxySource {
public:
    TNestedProxySource(TPosition pos, const TVector<TNodePtr>& groupBy, TSourcePtr source)
        : IProxySource(pos, source.Get())
        , CompositeSelect(nullptr)
        , Holder(std::move(source))
        , GroupBy(groupBy)
    {}

    TNestedProxySource(TCompositeSelect* compositeSelect, const TVector<TNodePtr>& groupBy)
        : IProxySource(compositeSelect->GetPos(), compositeSelect->RealSource())
        , CompositeSelect(compositeSelect)
        , GroupBy(groupBy)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        return Source->Init(ctx, src);
    }

    TNodePtr Build(TContext& ctx) override  {
        return CompositeSelect ? BuildAtom(Pos, "composite", TNodeFlags::Default) : Source->Build(ctx);
    }

    bool InitFilters(TContext& ctx) override {
        return CompositeSelect ? true : Source->InitFilters(ctx);
    }

    TNodePtr BuildFilter(TContext& ctx, const TString& label) override {
        return CompositeSelect ? nullptr : Source->BuildFilter(ctx, label);
    }

    IJoin* GetJoin() override {
        return Source->GetJoin();
    }

    bool IsCompositeSource() const override {
        return true;
    }

    ISource* GetCompositeSource() override {
        return CompositeSelect;
    }

    bool AddGrouping(TContext& ctx, const TVector<TString>& columns, TString& hintColumn) override {
        Y_UNUSED(ctx);
        hintColumn = TStringBuilder() << "GroupingHint" << Hints.size();
        ui64 hint = 0;
        if (GroupByColumns.empty()) {
            const bool isJoin = GetJoin();
            for (const auto& groupByNode: GroupBy) {
                auto namePtr = groupByNode->GetColumnName();
                YQL_ENSURE(namePtr);
                TString column = *namePtr;
                if (isJoin) {
                    auto sourceNamePtr = groupByNode->GetSourceName();
                    if (sourceNamePtr && !sourceNamePtr->empty()) {
                        column = DotJoin(*sourceNamePtr, column);
                    }
                }
                GroupByColumns.insert(column);
            }
        }
        for (const auto& column: columns) {
            hint <<= 1;
            if (!GroupByColumns.contains(column)) {
                hint += 1;
            }
        }
        Hints.push_back(hint);
        return true;
    }

    size_t GetGroupingColumnsCount() const override {
        return Hints.size();
    }

    TNodePtr BuildGroupingColumns(const TString& label) override {
        if (Hints.empty()) {
            return nullptr;
        }

        auto body = Y();
        for (size_t i = 0; i < Hints.size(); ++i) {
            TString hintColumn = TStringBuilder() << "GroupingHint" << i;
            TString hintValue = ToString(Hints[i]);
            body = L(body, Y("let", "row", Y("AddMember", "row", Q(hintColumn), Y("Uint64", Q(hintValue)))));
        }
        return Y("Map", label, BuildLambda(Pos, Y("row"), body, "row"));
    }


    void FinishColumns() override {
        Source->FinishColumns();
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        if (const TString* columnName = column.GetColumnName()) {
            if (columnName && IsExprAlias(*columnName)) {
                return true;
            }
        }
        return Source->AddColumn(ctx, column);
    }

    TPtr DoClone() const final {
        YQL_ENSURE(Hints.empty());
        return Holder.Get() ? new TNestedProxySource(Pos, CloneContainer(GroupBy), Holder->CloneSource()) :
            new TNestedProxySource(CompositeSelect, CloneContainer(GroupBy));
    }

private:
    TCompositeSelect* CompositeSelect;
    TSourcePtr Holder;
    TVector<TNodePtr> GroupBy;
    mutable TSet<TString> GroupByColumns;
    mutable TVector<ui64> Hints;
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
    bool selectStream,
    const TWriteSettings& settings,
    TColumnsSets&& uniqueSets,
    TColumnsSets&& distinctSets
) {
    if (groupBy.empty() || !groupBy.front()->ContentListPtr()) {
        return new TSelectCore(pos, std::move(source), groupByExpr, groupBy, compactGroupBy, groupBySuffix, assumeSorted,
            orderBy, having, winSpecs, legacyHoppingWindowSpec, terms, distinct, without, selectStream, settings, std::move(uniqueSets), std::move(distinctSets));
    }
    if (groupBy.size() == 1) {
        /// actualy no big idea to use grouping function in this case (result allways 0)
        auto contentPtr = groupBy.front()->ContentListPtr();
        source = new TNestedProxySource(pos, *contentPtr, source);
        return DoBuildSelectCore(ctx, pos, originalSource, source, groupByExpr, *contentPtr, compactGroupBy, groupBySuffix,
            assumeSorted, orderBy, having, std::move(winSpecs),
            legacyHoppingWindowSpec, std::move(terms), distinct, std::move(without), selectStream, settings, std::move(uniqueSets), std::move(distinctSets));
    }
    /// \todo some smart merge logic, generalize common part of grouping (expr, flatten, etc)?
    TIntrusivePtr<TCompositeSelect> compositeSelect = new TCompositeSelect(pos, std::move(source), originalSource->CloneSource(), settings);
    size_t totalGroups = 0;
    TVector<TSourcePtr> subselects;
    TVector<TNodePtr> groupingCols;
    for (auto& grouping: groupBy) {
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
            for (const auto& term: terms) {
                termsCopy.emplace_back(term->Clone());
            }
            std::swap(terms, termsCopy);
        }
        totalGroups += contentPtr->size();
        TSelectCore* selectCore = new TSelectCore(pos, std::move(proxySource), CloneContainer(groupByExpr),
            CloneContainer(*contentPtr), compactGroupBy, groupBySuffix, assumeSorted, orderBy, SafeClone(having), CloneContainer(winSpecs),
            legacyHoppingWindowSpec, terms, distinct, without, selectStream, settings, TColumnsSets(uniqueSets), TColumnsSets(distinctSets));
        subselects.emplace_back(selectCore);
    }
    if (totalGroups > ctx.PragmaGroupByLimit) {
        ctx.Error(pos) << "Unable to GROUP BY more than " << ctx.PragmaGroupByLimit << " groups, you try use " << totalGroups << " groups";
        return nullptr;
    }
    compositeSelect->SetSubselects(std::move(subselects), std::move(groupingCols), CloneContainer(groupByExpr));
    return compositeSelect;
}

}

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
    bool selectStream,
    const TWriteSettings& settings,
    TColumnsSets&& uniqueSets,
    TColumnsSets&& distinctSets
)
{
    return DoBuildSelectCore(ctx, pos, source, source, groupByExpr, groupBy, compactGroupBy, groupBySuffix, assumeSorted, orderBy,
        having, std::move(winSpecs), legacyHoppingWindowSpec, std::move(terms), distinct, std::move(without), selectStream, settings, std::move(uniqueSets), std::move(distinctSets));
}

class TUnion: public IRealSource {
public:
    TUnion(TPosition pos, TVector<TSourcePtr>&& sources, bool quantifierAll, const TWriteSettings& settings)
        : IRealSource(pos)
        , Sources(std::move(sources))
        , QuantifierAll(quantifierAll)
        , Settings(settings)
    {
    }

    const TColumns* GetColumns() const override {
        return IRealSource::GetColumns();
    }

    void GetInputTables(TTableList& tableList) const override {
        for (auto& x : Sources) {
            x->GetInputTables(tableList);
        }

        ISource::GetInputTables(tableList);
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        bool first = true;
        for (auto& s: Sources) {
            s->UseAsInner();
            if (!s->Init(ctx, src)) {
                return false;
            }
            if (!ctx.PositionalUnionAll || first) {
                auto c = s->GetColumns();
                Y_DEBUG_ABORT_UNLESS(c);
                Columns.Merge(*c);
                first = false;
            }
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        TPtr res;
        if (QuantifierAll) {
            res = ctx.PositionalUnionAll ? Y("UnionAllPositional") : Y("UnionAll");
        } else {
            res = ctx.PositionalUnionAll ? Y("UnionPositional") : Y("Union");
        }

        for (auto& s: Sources) {
            auto input = s->Build(ctx);
            if (!input) {
                return nullptr;
            }
            res->Add(input);
        }
        return res;
    }


    bool IsStream() const override {
        for (auto& s: Sources) {
            if (!s->IsStream()) {
                return false;
            }
        }
        return true;
    }

    TNodePtr DoClone() const final {
        return MakeIntrusive<TUnion>(Pos, CloneContainer(Sources), QuantifierAll, Settings);
    }

    bool IsSelect() const override {
        return true;
    }

    bool HasSelectResult() const override {
        return !Settings.Discard;
    }

    TWriteSettings GetWriteSettings() const override {
        return Settings;
    }

private:
    TVector<TSourcePtr> Sources;
    bool QuantifierAll;
    const TWriteSettings Settings;
};

TSourcePtr BuildUnion(
    TPosition pos, 
    TVector<TSourcePtr>&& sources, 
    bool quantifierAll,
    const TWriteSettings& settings
) {
    return new TUnion(pos, std::move(sources), quantifierAll, settings);
}

class TOverWindowSource: public IProxySource {
public:
    TOverWindowSource(TPosition pos, const TString& windowName, ISource* origSource)
        : IProxySource(pos, origSource)
        , WindowName(windowName)
    {
        Source->SetLabel(origSource->GetLabel());
    }

    TString MakeLocalName(const TString& name) override {
        return Source->MakeLocalName(name);
    }

    void AddTmpWindowColumn(const TString& column) override {
        return Source->AddTmpWindowColumn(column);
    }

    bool AddAggregation(TContext& ctx, TAggregationPtr aggr) override {
        if (aggr->IsOverWindow()) {
            return Source->AddAggregationOverWindow(ctx, WindowName, aggr);
        }
        return Source->AddAggregation(ctx, aggr);
    }

    bool AddFuncOverWindow(TContext& ctx, TNodePtr expr) override {
        return Source->AddFuncOverWindow(ctx, WindowName, expr);
    }

    bool IsOverWindowSource() const override {
        return true;
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        return Source->AddColumn(ctx, column);
    }

    TNodePtr Build(TContext& ctx) override {
        Y_UNUSED(ctx);
        Y_ABORT("Unexpected call");
    }

    const TString* GetWindowName() const override {
        return &WindowName;
    }

    TWindowSpecificationPtr FindWindowSpecification(TContext& ctx, const TString& windowName) const override {
        return Source->FindWindowSpecification(ctx, windowName);
    }

    TNodePtr GetSessionWindowSpec() const override {
        return Source->GetSessionWindowSpec();
    }

    TNodePtr DoClone() const final {
        return {};
    }

private:
    const TString WindowName;
};

TSourcePtr BuildOverWindowSource(TPosition pos, const TString& windowName, ISource* origSource) {
    return new TOverWindowSource(pos, windowName, origSource);
}

class TSkipTakeNode final: public TAstListNode {
public:
    TSkipTakeNode(TPosition pos, const TNodePtr& skip, const TNodePtr& take)
        : TAstListNode(pos), IsSkipProvided_(!!skip)
    {
        TNodePtr select(AstNode("select"));
        if (skip) {
            select = Y("Skip", select, Y("Coalesce", skip, Y("Uint64", Q("0"))));
        }
        static const TString uiMax = ::ToString(std::numeric_limits<ui64>::max());
        Add("let", "select", Y("Take", select, Y("Coalesce", take, Y("Uint64", Q(uiMax)))));
    }

    TPtr DoClone() const final {
        return {};
    }

    bool HasSkip() const {
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
        , Source(std::move(source))
        , SkipTake(skipTake)
    {}

    bool DoInit(TContext& ctx, ISource* src) override {
        Source->SetLabel(Label);
        if (AsInner) {
            Source->UseAsInner();
        }

        if (IgnoreSort()) {
            ctx.Warning(Source->GetPos(), TIssuesIds::YQL_ORDER_BY_WITHOUT_LIMIT_IN_SUBQUERY) << "ORDER BY without LIMIT in subquery will be ignored";
        }

        if (!Source->Init(ctx, src)) {
            return false;
        }
        src = Source.Get();
        if (SkipTake) {
            FakeSource = BuildFakeSource(SkipTake->GetPos());
            if (!SkipTake->Init(ctx, FakeSource.Get())) {
                return false;
            }
            if (SkipTake->HasSkip() && EOrderKind::Sort != Source->GetOrderKind()) {
                ctx.Warning(Source->GetPos(), TIssuesIds::YQL_OFFSET_WITHOUT_SORT) << "LIMIT with OFFSET without ORDER BY may provide different results from run to run";
            }
        }

        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        auto input = Source->Build(ctx);
        if (!input) {
            return nullptr;
        }
        const auto label = "select";
        auto block(Y(Y("let", label, input)));

        auto sortNode = Source->BuildSort(ctx, label);
        if (sortNode && !IgnoreSort()) {
            block = L(block, sortNode);
        }

        if (SkipTake) {
            block = L(block, SkipTake);
        }

        TNodePtr sample;
        if (!BuildSamplingLambda(sample)) {
            return nullptr;
        } else if (sample) {
            block = L(block, Y("let", "select", Y("OrderedFlatMap", "select", sample)));
        }

        if (auto removeNode = Source->BuildCleanupColumns(ctx, label)) {
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
        return Source->IsSelect();
    }

    bool HasSelectResult() const override {
        return Source->HasSelectResult();
    }

    TPtr DoClone() const final {
        return MakeIntrusive<TSelect>(Pos, Source->CloneSource(), SafeClone(SkipTake));
    }
protected:
    bool IgnoreSort() const {
        return AsInner && !SkipTake && EOrderKind::Sort == Source->GetOrderKind();
    }

    TSourcePtr Source;
    TNodePtr SkipTake;
    TSourcePtr FakeSource;
};

TSourcePtr BuildSelect(TPosition pos, TSourcePtr source, TNodePtr skipTake) {
    return new TSelect(pos, std::move(source), skipTake);
}

class TSelectResultNode final: public TAstListNode {
public:
    TSelectResultNode(TPosition pos, TSourcePtr source, bool writeResult, bool inSubquery,
        TScopedStatePtr scoped)
        : TAstListNode(pos)
        , Source(std::move(source))
        , WriteResult(writeResult)
        , InSubquery(inSubquery)
        , Scoped(scoped)
    {
        YQL_ENSURE(Source, "Invalid source node");
        FakeSource = BuildFakeSource(pos);
    }

    bool IsSelect() const override {
        return true;
    }

    bool HasSelectResult() const override {
        return Source->HasSelectResult();
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Source->Init(ctx, src)) {
            return false;
        }

        src = Source.Get();
        TTableList tableList;
        Source->GetInputTables(tableList);

        TNodePtr node(BuildInputTables(Pos, tableList, InSubquery, Scoped));
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

            if (!labelNode->Init(ctx, FakeSource.Get())) {
                return false;
            }

            settings = L(settings, Q(Y(Q("label"), labelNode)));
        }

        if (asRef) {
            settings = L(settings, Q(Y(Q("ref"))));
        } else if (asAutoRef) {
            settings = L(settings, Q(Y(Q("autoref"))));
        }

        auto columns = Source->GetColumns();
        if (columns && !columns->All && !(columns->QualifiedAll && ctx.SimpleColumns)) {
            auto list = Y();
            YQL_ENSURE(columns->List.size() == columns->NamedColumns.size());
            for (size_t i = 0; i < columns->List.size(); ++i) {
                auto& c = columns->List[i];
                if (c.EndsWith('*')) {
                    list = L(list, Q(Y(Q("prefix"), BuildQuotedAtom(Pos, c.substr(0, c.size() - 1)))));
                } else if (columns->NamedColumns[i]) {
                    list = L(list, BuildQuotedAtom(Pos, c));
                } else {
                    list = L(list, Q(Y(Q("auto"))));
                }
            }
            settings = L(settings, Q(Y(Q("columns"), Q(list))));
        }

        if (ctx.ResultRowsLimit > 0) {
            settings = L(settings, Q(Y(Q("take"), Q(ToString(ctx.ResultRowsLimit)))));
        }

        auto output = Source->Build(ctx);
        if (!output) {
            return false;
        }
        node = L(node, Y("let", "output", output));
        if (WriteResult || writeSettings.Discard) {
            if (EOrderKind::None == Source->GetOrderKind() && ctx.UseUnordered(*Source)) {
                node = L(node, Y("let", "output", Y("Unordered", "output")));
                if (ctx.UnorderedResult) {
                    settings = L(settings, Q(Y(Q("unordered"))));
                }
            }
            auto writeResult(BuildWriteResult(Pos, "output", settings));
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
    TSourcePtr Source;

    const bool WriteResult;
    const bool InSubquery;
    TScopedStatePtr Scoped;
    TSourcePtr FakeSource;
};

TNodePtr BuildSelectResult(TPosition pos, TSourcePtr source, bool writeResult, bool inSubquery,
    TScopedStatePtr scoped) {
    return new TSelectResultNode(pos, std::move(source), writeResult, inSubquery, scoped);
}

} // namespace NSQLTranslationV1
