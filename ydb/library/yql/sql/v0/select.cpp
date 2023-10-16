#include "sql.h"
#include "node.h"

#include "context.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/charset/ci_string.h>

using namespace NYql;

namespace NSQLTranslationV0 {

class TSubqueryNode: public INode {
public:
    TSubqueryNode(TSourcePtr&& source, const TString& alias, bool inSubquery, int ensureTupleSize)
        : INode(source->GetPos())
        , Source(std::move(source))
        , Alias(alias)
        , InSubquery(inSubquery)
        , EnsureTupleSize(ensureTupleSize)
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

        auto tables = BuildInputTables(Pos, tableList, InSubquery);
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
        return {};
    }

protected:
    TSourcePtr Source;
    TNodePtr Node;
    const TString Alias;
    const bool InSubquery;
    const int EnsureTupleSize;
    bool IsUsed = false;
};

TNodePtr BuildSubquery(TSourcePtr source, const TString& alias, bool inSubquery, int ensureTupleSize) {
    return new TSubqueryNode(std::move(source), alias, inSubquery, ensureTupleSize);
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
                if (!columnsPtr || columnsPtr->All || columnsPtr->QualifiedAll || columnsPtr->List.size() != 1) {
                    ctx.Error(Pos) << "Source used in expression should contain one concrete column";
                    return false;
                }
                Node = Y("Member", Y("SqlAccess", Q("dict"), Y("Take", Node, Y("Uint64", Q("1"))), Y("Uint64", Q("0"))), Q(columnsPtr->List.front()));
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
    TFakeSource(TPosition pos)
        : ISource(pos)
    {}

    bool IsFake() const override {
        return true;
    }

    bool AddFilter(TContext& ctx, TNodePtr filter) override  {
        Y_UNUSED(filter);
        ctx.Error(Pos) << "Source does not allow filtering";
        return false;
    }

    TNodePtr Build(TContext& ctx) override  {
        Y_UNUSED(ctx);
        return Y("AsList", Y("Uint32", Q("0")));
    }

    bool AddGroupKey(TContext& ctx, const TString& column) override {
        Y_UNUSED(column);
        ctx.Error(Pos) << "Source does not allow grouping";
        return false;
    }

    bool AddAggregation(TContext& ctx, TAggregationPtr aggr) override {
        Y_UNUSED(aggr);
        ctx.Error(Pos) << "Source does not allow aggregation";
        return false;
    }

    bool IsGroupByColumn(const TString& column) const override {
        Y_UNUSED(column);
        return false;
    }

    TNodePtr BuildFilter(TContext& ctx, const TString& label, const TNodePtr& groundNode) override {
        Y_UNUSED(ctx);
        Y_UNUSED(label);
        Y_UNUSED(groundNode);
        return nullptr;
    }

    TNodePtr BuildAggregation(const TString& label) override {
        Y_UNUSED(label);
        return nullptr;
    }

    TPtr DoClone() const final {
        return new TFakeSource(Pos);
    }
};

TSourcePtr BuildFakeSource(TPosition pos) {
    return new TFakeSource(pos);
}

class TNodeSource: public ISource {
public:
    TNodeSource(TPosition pos, const TNodePtr& node)
        : ISource(pos)
        , Node(node)
    {
        YQL_ENSURE(Node);
        FakeSource = BuildFakeSource(pos);
    }

    void AllColumns() final {
        UseAllColumns = true;
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) final {
        Y_UNUSED(ctx);
        if (UseAllColumns) {
            return true;
        }

        if (column.IsAsterisk()) {
            AllColumns();
        } else {
            Columns.push_back(*column.GetColumnName());
        }

        return true;
    }

    TNodePtr Build(TContext& ctx) final  {
        ctx.PushBlockShortcuts();
        if (!Node->Init(ctx, FakeSource.Get())) {
            return {};
        }

        Node = ctx.GroundBlockShortcutsForExpr(Node);
        auto nodeAst = AstNode(Node);

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
        return new TNodeSource(Pos, Node);
    }

private:
    TNodePtr Node;
    TSourcePtr FakeSource;
    TVector<TString> Columns;
    bool UseAllColumns = false;
};

TSourcePtr BuildNodeSource(TPosition pos, const TNodePtr& node) {
    return new TNodeSource(pos, node);
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

    bool ShouldUseSourceAsColumn(const TString& source) override {
        return Source->ShouldUseSourceAsColumn(source);
    }

    bool IsStream() const override {
        Y_DEBUG_ABORT_UNLESS(Source);
        return Source->IsStream();
    }

    bool IsOrdered() const override {
        Y_DEBUG_ABORT_UNLESS(Source);
        return Source->IsOrdered();
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
        auto& label = *column.GetSourceName();
        if (!label.empty() && label != GetLabel()) {
            if (column.IsReliable()) {
                ctx.Error(column.GetPos()) << "Unknown correlation name: " << label;
            }
            return {};
        }
        if (column.IsAsterisk()) {
            return true;
        }
        const auto* name = column.GetColumnName();
        if (name && !Columns.IsColumnPossible(ctx, *name) && !IsAlias(EExprSeat::GroupBy, *name)) {
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

            ctx.PushBlockShortcuts();
            if (src) {
                src->AddDependentSource(source.Get());
            }
            if (!source->Init(ctx, src)) {
                return false;
            }
            if (!source->InitFilters(ctx)) {
                return false;
            }
            FiltersGrounds.push_back(ctx.GroundBlockShortcuts(Pos));
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
            auto filter = source->BuildFilter(ctx, ref, FiltersGrounds[i]);
            if (filter) {
                block = L(block, Y("let", ref, filter));
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
        // Don't clone FiltersGrounds container because it will be initialized in DoInit of cloned object
        return new TMuxSource(Pos, CloneContainer(Sources));
    }

protected:
    TVector<TSourcePtr> Sources;
    TVector<TNodePtr> FiltersGrounds;
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
            if (!columnsPtr || columnsPtr->All || columnsPtr->QualifiedAll || columnsPtr->List.size() != 1) {
                ctx.Error(Pos) << "Source used in expression should contain one concrete column";
                return false;
            }
            Node = Y("Member", Y("SqlAccess", Q("dict"), Y("Take", Node, Y("Uint64", Q("1"))), Y("Uint64", Q("0"))), Q(columnsPtr->List.front()));
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override  {
        Y_UNUSED(ctx);
        return Node;
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

class TTableSource: public IRealSource {
public:
    TTableSource(TPosition pos, const TTableRef& table, bool stream, const TString& label)
        : IRealSource(pos)
        , Table(table)
        , Stream(stream)
    {
        SetLabel(label.empty() ? Table.ShortName() : label);
    }

    void GetInputTables(TTableList& tableList) const override {
        tableList.push_back(Table);
        ISource::GetInputTables(tableList);
    }

    bool ShouldUseSourceAsColumn(const TString& source) override {
        return source && source != GetLabel();
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
            ESampleMode mode,
            TNodePtr samplingRate,
            TNodePtr samplingSeed) override {
        Y_UNUSED(pos);
        TString modeName;
        if (!samplingSeed) {
            samplingSeed = Y("Int32", Q("0"));
        }

        switch (mode) {
        case ESampleMode::Auto:
            modeName = "bernoulli";
            samplingRate = Y("*", samplingRate, Y("Double", Q("100")));
            break;
        case ESampleMode::Bernoulli:
            modeName = "bernoulli";
            break;
        case ESampleMode::System:
            modeName = "system";
            break;
        }

        samplingRate = Y("Ensure", samplingRate, Y(">", samplingRate, Y("Double", Q("0"))), Y("String", Q("Expected sampling rate to be positive")));
        samplingRate = Y("Ensure", samplingRate, Y("<=", samplingRate, Y("Double", Q("100"))), Y("String", Q("Sampling rate is over 100%")));
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

    TNodePtr Build(TContext& ctx) override {
        if (!Table.Keys->Init(ctx, nullptr)) {
            return nullptr;
        }
        return AstNode(Table.RefName);
    }

    bool IsStream() const override {
        return Stream;
    }

    TPtr DoClone() const final {
        return new TTableSource(Pos, Table, Stream, GetLabel());
    }

    bool IsTableSource() const override {
        return true;
    }
protected:
    TTableRef Table;
    const bool Stream;
};

TSourcePtr BuildTableSource(TPosition pos, const TTableRef& table, bool stream, const TString& label) {
    return new TTableSource(pos, table, stream, label);
}

class TInnerSource: public IProxySource {
public:
    TInnerSource(TPosition pos, TNodePtr node, const TString& label)
        : IProxySource(pos, nullptr)
        , Node(node)
    {
        SetLabel(label);
    }

    bool ShouldUseSourceAsColumn(const TString& source) override {
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

    bool DoInit(TContext& ctx, ISource* src) override {
        auto source = Node->GetSource();
        if (!source) {
            NewSource = TryMakeSourceFromExpression(ctx, Node);
            source = NewSource.Get();
        }

        if (!source) {
            ctx.Error(Pos) << "Invalid inner source node";
            return false;
        }
        source->SetLabel(Label);
        if (!NewSource) {
            Node->UseAsInner();
            if (!Node->Init(ctx, src)) {
                return false;
            }
        }

        SetSource(source);
        if (NewSource && !NewSource->Init(ctx, src)) {
            return false;
        }

        return ISource::DoInit(ctx, source);
    }

    TNodePtr Build(TContext& ctx) override {
        Y_UNUSED(ctx);
        return NewSource ? NewSource->Build(ctx) : Node;
    }

    TPtr DoClone() const final {
        return new TInnerSource(Pos, SafeClone(Node), GetLabel());
    }
protected:
    TNodePtr Node;
    TSourcePtr NewSource;
};

TSourcePtr BuildInnerSource(TPosition pos, TNodePtr node, const TString& label) {
    return new TInnerSource(pos, node, label);
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
        const TWriteSettings& settings)
        : IRealSource(pos)
        , Mode(mode)
        , Source(std::move(source))
        , OrderBy(std::move(orderBy))
        , Keys(std::move(keys))
        , Args(std::move(args))
        , Udf(udf)
        , Having(having)
        , Settings(settings)
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

        ctx.PushBlockShortcuts();
        YQL_ENSURE(!src);
        if (!Source->Init(ctx, src)) {
            return false;
        }
        if (!Source->InitFilters(ctx)) {
            return false;
        }
        FiltersGround = ctx.GroundBlockShortcuts(Pos);
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
        ctx.PushBlockShortcuts();
        if (Having && !Having->Init(ctx, nullptr)) {
            return false;
        }
        HavingGround = ctx.GroundBlockShortcuts(Pos);

        /// SIN: verify reduce one argument
        if (Args.size() != 1) {
            ctx.Error(Pos) << "REDUCE requires exactly one UDF argument";
            return false;
        }
        ctx.PushBlockShortcuts();
        if (!Args[0]->Init(ctx, src)) {
            return false;
        }
        ExprGround = ctx.GroundBlockShortcuts(Pos);

        ctx.PushBlockShortcuts();
        for (auto orderSpec: OrderBy) {
            if (!orderSpec->OrderExpr->Init(ctx, src)) {
                return false;
            }
        }
        OrderByGround = ctx.GroundBlockShortcuts(Pos);

        if (!Udf->Init(ctx, src)) {
            return false;
        }
        if (Udf->GetLabel().empty()) {
            Columns.SetAll();
        } else {
            Columns.Add(&Udf->GetLabel(), false);
        }
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
        switch (Mode) {
            case ReduceMode::ByAll: {
                auto columnPtr = Args[0]->GetColumnName();
                TNodePtr expr = BuildAtom(Pos, "partitionStream");
                if (!columnPtr || *columnPtr != "*") {
                    expr = Y("Map", "partitionStream", BuildLambda(Pos, Y("keyPair"), Q(L(Y(),\
                        Y("Nth", "keyPair", Q(ToString("0"))),\
                        Y("Map", Y("Nth", "keyPair", Q(ToString("1"))), BuildLambda(Pos, Y("row"),
                                GroundWithExpr(ExprGround, Args[0])))))));
                }
                processPartitions = Y("ToSequence", Y("Apply", Udf, expr));
                break;
            }
            case ReduceMode::ByPartition: {
                processPartitions = Y("SqlReduce", "partitionStream", extractKeyLambda, Udf,
                    BuildLambda(Pos, Y("row"), GroundWithExpr(ExprGround, Args[0])));
                break;
            }
            default:
                YQL_ENSURE(false, "Unexpected REDUCE mode");
        }

        TNodePtr sortDirection;
        auto sortKeySelector = OrderByGround;
        FillSortParts(OrderBy, sortDirection, sortKeySelector);
        if (!OrderBy.empty()) {
            sortKeySelector = BuildLambda(Pos, Y("row"), Y("SqlExtractKey", "row", sortKeySelector));
        }

        auto partitionByKey = Y(Mode == ReduceMode::ByAll ? "PartitionByKey" : "PartitionsByKeys", "core", extractKeyLambda,
            sortDirection, sortKeySelector, BuildLambda(Pos, Y("partitionStream"), processPartitions));

        auto block(Y(Y("let", "core", input)));
        auto filter = Source->BuildFilter(ctx, "core", FiltersGround);
        if (filter) {
            block = L(block, Y("let", "core", filter));
        }
        block = L(block, Y("let", "core", Y("AutoDemux", partitionByKey)));
        if (Having) {
            block = L(block, Y("let", "core",
                Y("Filter", "core", BuildLambda(Pos, Y("row"), GroundWithExpr(HavingGround, Y("Coalesce", Having, Y("Bool", Q("false"))))))
            ));
        }
        return Y("block", Q(L(block, Y("return", "core"))));
    }

    TWriteSettings GetWriteSettings() const final {
        return Settings;
    }

    TPtr DoClone() const final {
        return new TReduceSource(Pos, Mode, Source->CloneSource(), CloneContainer(OrderBy),
                CloneContainer(Keys), CloneContainer(Args), SafeClone(Udf), SafeClone(Having), Settings);
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
    TNodePtr ExprGround;
    TNodePtr FiltersGround;
    TNodePtr OrderByGround;
    TNodePtr HavingGround;
};

TSourcePtr BuildReduce(TPosition pos,
    ReduceMode mode,
    TSourcePtr source,
    TVector<TSortSpecificationPtr>&& orderBy,
    TVector<TNodePtr>&& keys,
    TVector<TNodePtr>&& args,
    TNodePtr udf,
    TNodePtr having,
    const TWriteSettings& settings) {
    return new TReduceSource(pos, mode, std::move(source), std::move(orderBy), std::move(keys), std::move(args), udf, having, settings);
}

class TCompositeSelect: public IRealSource {
public:
    TCompositeSelect(TPosition pos, TSourcePtr source, const TWriteSettings& settings)
        : IRealSource(pos)
        , Source(std::move(source))
        , Settings(settings)
    {
        YQL_ENSURE(Source);
    }

    void SetSubselects(TVector<TSourcePtr>&& subselects, TSet<TString>&& groupingCols) {
        Subselects = std::move(subselects);
        GroupingCols = std::move(groupingCols);
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

        ctx.PushBlockShortcuts();
        if (src) {
            src->AddDependentSource(Source.Get());
        }
        if (!Source->Init(ctx, src)) {
            return false;
        }
        if (!Source->InitFilters(ctx)) {
            return false;
        }
        FiltersGround = ctx.GroundBlockShortcuts(Pos);
        for (const auto& select: Subselects) {
            select->SetLabel(Label);
            if (AsInner) {
                select->UseAsInner();
            }

            if (!select->Init(ctx, Source.Get())) {
                return false;
            }
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
        auto filter = Source->BuildFilter(ctx, "composite", FiltersGround);
        if (filter) {
            block = L(block, Y("let", "composite", filter));
        }

        TNodePtr compositeNode = Y("UnionAll");
        for (const auto& select: Subselects) {
            auto addNode = select->Build(ctx);
            if (!addNode) {
                return nullptr;
            }
            compositeNode->Add(addNode);
        }

        return GroundWithExpr(block, compositeNode);
    }

    bool IsGroupByColumn(const TString& column) const override {
        return GroupingCols.contains(column);
    }

    const TSet<TString>& GetGroupingCols() const {
        return GroupingCols;
    }

    TNodePtr BuildSort(TContext& ctx, const TString& label) override {
        return Subselects.front()->BuildSort(ctx, label);
    }

    bool IsOrdered() const override {
        return Subselects.front()->IsOrdered();
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

    TNodePtr DoClone() const final {
        auto newSource = MakeIntrusive<TCompositeSelect>(Pos, Source->CloneSource(), Settings);
        newSource->SetSubselects(CloneContainer(Subselects), TSet<TString>(GroupingCols));
        return newSource;
    }
private:
    TSourcePtr Source;
    const TWriteSettings Settings;
    TVector<TSourcePtr> Subselects;
    TSet<TString> GroupingCols;
    TNodePtr FiltersGround;
};

/// \todo simplify class
class TSelectCore: public IRealSource {
public:
    TSelectCore(
        TPosition pos,
        TSourcePtr source,
        const TVector<TNodePtr>& groupByExpr,
        const TVector<TNodePtr>& groupBy,
        const TVector<TSortSpecificationPtr>& orderBy,
        TNodePtr having,
        TWinSpecs& winSpecs,
        THoppingWindowSpecPtr hoppingWindowSpec,
        const TVector<TNodePtr>& terms,
        bool distinct,
        const TVector<TNodePtr>& without,
        bool stream,
        const TWriteSettings& settings
    )
        : IRealSource(pos)
        , Source(std::move(source))
        , GroupByExpr(groupByExpr)
        , GroupBy(groupBy)
        , OrderBy(orderBy)
        , Having(having)
        , WinSpecs(winSpecs)
        , Terms(terms)
        , Without(without)
        , Distinct(distinct)
        , HoppingWindowSpec(hoppingWindowSpec)
        , Stream(stream)
        , Settings(settings)
    {
    }

    void GetInputTables(TTableList& tableList) const override {
        Source->GetInputTables(tableList);
        ISource::GetInputTables(tableList);
    }

    bool IsComparableExpression(TContext& ctx, const TNodePtr& expr, const char* sqlConstruction) {
        if (expr->IsConstant()) {
            ctx.Error(expr->GetPos()) << "Unable to " << sqlConstruction << " constant expression";
            return false;
        }
        if (expr->IsAggregated() && !expr->HasState(ENodeState::AggregationKey)) {
            ctx.Error(expr->GetPos()) << "Unable to " << sqlConstruction << " aggregated values";
            return false;
        }
        if (expr->GetSourceName()) {
            return true;
        }
        if (expr->GetOpName().empty()) {
            ctx.Error(expr->GetPos()) << "You should use in " << sqlConstruction << " column name, qualified field, callable function or expression";
            return false;
        }
        return true;
    }

    bool DoInit(TContext& ctx, ISource* initSrc) override {
        if (AsInner) {
            Source->UseAsInner();
        }

        if (!Source->Init(ctx, initSrc)) {
            return false;
        }
        if (Stream && !Source->IsStream()) {
            ctx.Error(Pos) << "SELECT STREAM is unsupported for non-streaming sources";
            return false;
        }
        if (!Stream && Source->IsStream() && !ctx.PragmaDirectRead) {
            ctx.Error(Pos) << "SELECT STREAM must be used for streaming sources";
            return false;
        }

        ctx.PushBlockShortcuts();
        auto src = Source.Get();
        bool hasError = false;
        for (auto& expr: GroupByExpr) {
            if (!expr->Init(ctx, src) || !IsComparableExpression(ctx, expr, "GROUP BY")) {
                hasError = true;
                continue;
            }
        }
        if (!src->AddExpressions(ctx, GroupByExpr, EExprSeat::GroupBy)) {
            hasError = true;
        }
        GroupByExprGround = ctx.GroundBlockShortcuts(Pos);
        /// grouped expressions are available in filters
        ctx.PushBlockShortcuts();
        if (!Source->InitFilters(ctx)) {
            hasError = true;
        }
        FiltersGround = ctx.GroundBlockShortcuts(Pos);
        const bool isJoin = Source->GetJoin();
        for (auto& expr: GroupBy) {
            if (!expr->Init(ctx, src)) {
                hasError = true;
                continue;
            }
            auto keyNamePtr = expr->GetColumnName();
            if (keyNamePtr && expr->GetLabel().empty()) {
                auto usedColumn = *keyNamePtr;
                auto sourceNamePtr = expr->GetSourceName();
                auto columnNode = dynamic_cast<TColumnNode*>(expr.Get());
                if (isJoin && (!columnNode || !columnNode->IsArtificial())) {
                    if (!sourceNamePtr || sourceNamePtr->empty()) {
                        ctx.Error(expr->GetPos()) << "Columns in GROUP BY should have correlation name, error in key: " << usedColumn;
                        hasError = true;
                        continue;
                    }
                    usedColumn = DotJoin(*sourceNamePtr, usedColumn);
                }
                if (!src->AddGroupKey(ctx, usedColumn)) {
                    hasError = true;
                    continue;
                }
            }
        }
        ctx.PushBlockShortcuts();
        if (Having && !Having->Init(ctx, src)) {
            hasError = true;
        }
        HavingGround = ctx.GroundBlockShortcuts(Pos);
        src->AddWindowSpecs(WinSpecs);

        if (!InitSelect(ctx, src, isJoin, hasError)) {
            return false;
        }

        src->FinishColumns();
        Aggregate = src->BuildAggregation("core");
        if (src->IsFlattenByColumns() || src->IsFlattenColumns()) {
            Flatten = src->IsFlattenByColumns() ?
                src->BuildFlattenByColumns("row") :
                src->BuildFlattenColumns("row");
            if (!Flatten || !Flatten->Init(ctx, src)) {
                hasError = true;
            }
        }
        if (GroupByExpr) {
            auto sourcePreaggregate = src->BuildPreaggregatedMap(ctx);
            if (!sourcePreaggregate) {
                hasError = true;
            } else {
                PreaggregatedMap = !GroupByExprGround ? sourcePreaggregate :
                    Y("block", Q(L(GroupByExprGround, Y("return", sourcePreaggregate))));
            }
        }
        if (Aggregate) {
            if (!Aggregate->Init(ctx, src)) {
                hasError = true;
            }
            if (Having) {
                Aggregate = Y(
                    "Filter",
                    Aggregate,
                    BuildLambda(Pos, Y("row"), GroundWithExpr(HavingGround, Y("Coalesce", Having, Y("Bool", Q("false")))))
                );
            }
        } else if (Having) {
            ctx.Error(Having->GetPos()) << "HAVING with meaning GROUP BY () should be with aggregation function.";
            hasError = true;
        } else if (!Distinct && !GroupBy.empty()) {
            ctx.Error(Pos) << "No aggregations were specified";
            hasError = true;
        }
        if (hasError) {
            return false;
        }

        if (src->IsCalcOverWindow()) {
            if (src->IsExprSeat(EExprSeat::WindowPartitionBy, EExprType::WithExpression)) {
                PrewindowMap = src->BuildPrewindowMap(ctx, WinSpecsPartitionByGround);
                if (!PrewindowMap) {
                    hasError = true;
                }
            }
            CalcOverWindow = src->BuildCalcOverWindow(ctx, "core", WinSpecsOrderByGround);
            if (!CalcOverWindow) {
                hasError = true;
            }
        }
        if (hasError) {
            return false;
        }

        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        auto input = Source->Build(ctx);
        if (!input) {
            return nullptr;
        }

        TNodePtr terms = BuildColumnsTerms(ctx);

        bool ordered = ctx.UseUnordered(*this);
        auto block(Y(Y("let", "core", input)));
        if (Flatten) {
            block = L(block, Y("let", "core", Y(ordered ? "OrderedFlatMap" : "FlatMap", "core", BuildLambda(Pos, Y("row"), Flatten, "res"))));
        }
        if (PreaggregatedMap) {
            block = L(block, Y("let", "core", Y("FlatMap", "core", BuildLambda(Pos, Y("row"), PreaggregatedMap))));
            if (Source->IsCompositeSource() && !Columns.QualifiedAll) {
                block = L(block, Y("let", "preaggregated", "core"));
            }
        } else if (Source->IsCompositeSource() && !Columns.QualifiedAll) {
            block = L(block, Y("let", "origcore", "core"));
        }
        auto filter = Source->BuildFilter(ctx, "core", FiltersGround);
        if (filter) {
            block = L(block, Y("let", "core", filter));
        }
        if (Aggregate) {
            block = L(block, Y("let", "core", Aggregate));
            ordered = false;
        }
        if (PrewindowMap) {
            block = L(block, Y("let", "core", PrewindowMap));
        }
        if (CalcOverWindow) {
            block = L(block, Y("let", "core", CalcOverWindow));
        }
        block = L(block, Y("let", "core", Y("EnsurePersistable", Y(ordered ? "OrderedFlatMap" : "FlatMap", "core", BuildLambda(Pos, Y("row"), terms, "res")))));
        return Y("block", Q(L(block, Y("return", "core"))));
    }

    TNodePtr BuildSort(TContext& ctx, const TString& label) override {
        Y_UNUSED(ctx);
        if (OrderBy.empty()) {
            return nullptr;
        }

        return Y("let", label, BuildSortSpec(OrderBy, label, OrderByGround));
    }

    bool IsSelect() const override {
        return true;
    }

    bool IsStream() const override {
        return Stream;
    }

    bool IsOrdered() const override {
        return !OrderBy.empty();
    }

    TWriteSettings GetWriteSettings() const override {
        return Settings;
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        if (OrderByInit && Source->GetJoin()) {
            column.SetAsNotReliable();
            auto maybeExist = IRealSource::AddColumn(ctx, column);
            if (maybeExist && maybeExist.GetRef()) {
                return true;
            }
            return Source->AddColumn(ctx, column);
        }
        return IRealSource::AddColumn(ctx, column);
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
        TWinSpecs newSpecs;
        for (auto cur: WinSpecs) {
            newSpecs.emplace(cur.first, cur.second->Clone());
        }
        return new TSelectCore(Pos, Source->CloneSource(), CloneContainer(GroupByExpr),
                CloneContainer(GroupBy), CloneContainer(OrderBy), SafeClone(Having), newSpecs, SafeClone(HoppingWindowSpec),
                CloneContainer(Terms), Distinct, Without, Stream, Settings);
    }

private:
    bool InitSelect(TContext& ctx, ISource* src, bool isJoin, bool& hasError) {
        for (auto iter: WinSpecs) {
            auto winSpec = *iter.second;
            ctx.PushBlockShortcuts();
            for (auto& partitionNode: winSpec.Partitions) {
                auto invalidPartitionNodeFunc = [&]() {
                    ctx.Error(partitionNode->GetPos()) << "Expected either column name, either alias" <<
                        " or expression with alias for PARTITION BY expression in WINDOWS clause";
                    hasError = true;
                };
                if (!partitionNode->GetLabel() && !partitionNode->GetColumnName()) {
                    invalidPartitionNodeFunc();
                    continue;
                }
                if (!partitionNode->Init(ctx, src)) {
                    hasError = true;
                    continue;
                }
                if (!partitionNode->GetLabel() && !partitionNode->GetColumnName()) {
                    invalidPartitionNodeFunc();
                    continue;
                }
            }
            WinSpecsPartitionByGround = ctx.GroundBlockShortcuts(Pos, WinSpecsPartitionByGround);
            if (!src->AddExpressions(ctx, winSpec.Partitions, EExprSeat::WindowPartitionBy)) {
                hasError = true;
            }

            ctx.PushBlockShortcuts();
            for (auto orderSpec: winSpec.OrderBy) {
                if (!orderSpec->OrderExpr->Init(ctx, src)) {
                    hasError = true;
                }
            }
            WinSpecsOrderByGround = ctx.GroundBlockShortcuts(Pos, WinSpecsOrderByGround);
        }

        if (HoppingWindowSpec) {
            ctx.PushBlockShortcuts();
            if (!HoppingWindowSpec->TimeExtractor->Init(ctx, src)) {
                hasError = true;
            }
            HoppingWindowSpec->TimeExtractor = ctx.GroundBlockShortcutsForExpr(HoppingWindowSpec->TimeExtractor);
            src->SetHoppingWindowSpec(HoppingWindowSpec);
        }

        ctx.PushBlockShortcuts();
        for (auto& term: Terms) {
            if (!term->Init(ctx, src)) {
                hasError = true;
                continue;
            }
            auto column = term->GetColumnName();
            if (Distinct) {
                if (!column) {
                    ctx.Error(Pos) << "SELECT DISTINCT requires a list of column references";
                    hasError = true;
                    continue;
                }
                if (term->IsAsterisk()) {
                    ctx.Error(Pos) << "SELECT DISTINCT * is not implemented yet";
                    hasError = true;
                    continue;
                }
                auto columnName = *column;
                if (isJoin) {
                    auto sourceNamePtr = term->GetSourceName();
                    if (!sourceNamePtr || sourceNamePtr->empty()) {
                        if (src->IsGroupByColumn(columnName)) {
                            ctx.Error(term->GetPos()) << ErrorDistinctByGroupKey(columnName);
                            hasError = true;
                            continue;
                        } else {
                            ctx.Error(term->GetPos()) << ErrorDistinctWithoutCorrelation(columnName);
                            hasError = true;
                            continue;
                        }
                    }
                    columnName = DotJoin(*sourceNamePtr, columnName);
                }
                if (src->IsGroupByColumn(columnName)) {
                    ctx.Error(term->GetPos()) << ErrorDistinctByGroupKey(columnName);
                    hasError = true;
                    continue;
                }
                if (!src->AddGroupKey(ctx, columnName)) {
                    hasError = true;
                    continue;
                }
                GroupBy.push_back(BuildColumn(Pos, columnName));
            }
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
                    label = TStringBuilder() << "column" << Columns.List.size();
                    hasName = false;
                }
            }
            if (!Columns.Add(&label, false, false, true, hasName)) {
                ctx.Error(Pos) << "Duplicate column: " << label;
                hasError = true;
                continue;
            }
        }
        TermsGround = ctx.GroundBlockShortcuts(Pos);

        if (Columns.All || Columns.QualifiedAll) {
            Source->AllColumns();
            if (Columns.All && isJoin && ctx.SimpleColumns) {
                Columns.All = false;
                Columns.QualifiedAll = true;
                const auto pos = Terms.front()->GetPos();
                Terms.clear();
                for (const auto& source: Source->GetJoin()->GetJoinLabels()) {
                    auto withDot = DotJoin(source, "*");
                    Columns.Add(&withDot, false);
                    Terms.push_back(BuildColumn(pos, "*", source));
                }
            }
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
        if (!src->IsCompositeSource() && !Distinct && !Columns.All && src->HasAggregations()) {
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
        ctx.PushBlockShortcuts();
        for (const auto& sortSpec: OrderBy) {
            auto& expr = sortSpec->OrderExpr;
            SetLabel(Source->GetLabel());
            OrderByInit = true;
            if (!expr->Init(ctx, this)) {
                hasError = true;
                continue;
            }
            OrderByInit = false;
            if (!IsComparableExpression(ctx, expr, "ORDER BY")) {
                hasError = true;
                continue;
            }
        }
        OrderByGround = ctx.GroundBlockShortcuts(Pos);
        SetLabel(label);

        return true;
    }

    TNodePtr BuildColumnsTerms(TContext& ctx) {
        TNodePtr terms;
        if (Columns.All) {
            Y_DEBUG_ABORT_UNLESS(Columns.List.empty());
            terms = PrepareWithout(Y());
            if (ctx.EnableSystemColumns) {
                terms = L(terms, Y("let", "res", Y("AsList", Y("RemoveSystemMembers", "row"))));
            } else {
                terms = L(terms, (Y("let", "res", Y("AsList", "row"))));
            }
        } else if (!Columns.List.empty()) {
            Y_DEBUG_ABORT_UNLESS(Columns.List.size() == Terms.size());
            const bool isJoin = Source->GetJoin();

            terms = TermsGround ? TermsGround : Y();
            if (Source->IsCompositeSource() && !Columns.QualifiedAll) {
                auto compositeSrcPtr = static_cast<TCompositeSelect*>(Source->GetCompositeSource());
                if (compositeSrcPtr) {
                    const auto& groupings = compositeSrcPtr->GetGroupingCols();
                    for (const auto& column: groupings) {
                        bool isAggregated = false;
                        for (const auto& group: GroupBy) {
                            const auto columnName = group->GetColumnName();
                            if (columnName && *columnName == column) {
                                isAggregated = true;
                                break;
                            }
                        }
                        if (isAggregated) {
                            continue;
                        }
                        const TString tableName = PreaggregatedMap ? "preaggregated" : "origcore";
                        terms = L(terms, Y("let", "row", Y("AddMember", "row", BuildQuotedAtom(Pos, column), Y("Nothing", Y("MatchType",
                            Y("StructMemberType", Y("ListItemType", Y("TypeOf", tableName)), Q(column)),
                            Q("Optional"), Y("lambda", Q(Y("item")), "item"), Y("lambda", Q(Y("item")), Y("OptionalType", "item")))))));
                    }
                }
            }

            TNodePtr structObj = nullptr;
            auto column = Columns.List.begin();
            for (auto& term: Terms) {
                if (!term->IsAsterisk()) {
                    if (!structObj) {
                        structObj = Y("AsStruct");
                    }
                    structObj = L(structObj, Q(Y(BuildQuotedAtom(Pos, *column), term)));
                }
                ++column;
            }
            terms = structObj ? L(terms, Y("let", "res", structObj)) : Y(Y("let", "res", Y("AsStruct")));
            terms = PrepareWithout(terms);
            if (Columns.QualifiedAll) {
                if (ctx.SimpleColumns && !isJoin) {
                    terms = L(terms, Y("let", "res", Y("FlattenMembers", Q(Y(BuildQuotedAtom(Pos, ""), "res")),
                        Q(Y(BuildQuotedAtom(Pos, ""), "row")))));
                } else {
                    if (isJoin && ctx.SimpleColumns) {
                        const auto& sameKeyMap = Source->GetJoin()->GetSameKeysMap();
                        if (sameKeyMap) {
                            terms = L(terms, Y("let", "flatSameKeys", "row"));
                            for (const auto& sameKeysPair: sameKeyMap) {
                                const auto& column = sameKeysPair.first;
                                auto keys = Y("Coalesce");
                                auto sameSourceIter = sameKeysPair.second.begin();
                                for (auto end = sameKeysPair.second.end(); sameSourceIter != end; ++sameSourceIter) {
                                    auto addKeyNode = Q(DotJoin(*sameSourceIter, column));
                                    keys = L(keys, Y("TryMember", "row", addKeyNode, Y("Null")));
                                }

                                terms = L(terms, Y("let", "flatSameKeys", Y("AddMember", "flatSameKeys", Q(column), keys)));
                                sameSourceIter = sameKeysPair.second.begin();
                                for (auto end = sameKeysPair.second.end(); sameSourceIter != end; ++sameSourceIter) {
                                    auto removeKeyNode = Q(DotJoin(*sameSourceIter, column));
                                    terms = L(terms, Y("let", "flatSameKeys", Y("ForceRemoveMember", "flatSameKeys", removeKeyNode)));
                                }
                            }
                            terms = L(terms, Y("let", "row", "flatSameKeys"));
                        }
                    }

                    auto members = isJoin ? Y() : Y("FlattenMembers");
                    for (auto& term: Terms) {
                        if (term->IsAsterisk()) {
                            auto sourceName = term->GetSourceName();
                            YQL_ENSURE(*sourceName && !sourceName->empty());
                            if (isJoin) {
                                members = L(members, BuildQuotedAtom(Pos, *sourceName + "."));
                            } else {
                                auto prefix = ctx.SimpleColumns ? "" : *sourceName + ".";
                                members = L(members, Q(Y(Q(prefix), "row")));
                            }
                        }
                    }
                    if (isJoin) {
                        members = Y(ctx.SimpleColumns ? "DivePrefixMembers" : "SelectMembers", "row", Q(members));
                    }
                    terms = L(terms, Y("let", "res", Y("FlattenMembers", Q(Y(BuildQuotedAtom(Pos, ""), "res")),
                        Q(Y(BuildQuotedAtom(Pos, ""), members)))));
                    if (isJoin && ctx.SimpleColumns) {
                        for (const auto& sameKeysPair: Source->GetJoin()->GetSameKeysMap()) {
                            const auto& column = sameKeysPair.first;
                            auto addMemberKeyNode = Y("Member", "row", Q(column));
                            terms = L(terms, Y("let", "res", Y("AddMember", "res", Q(column), addMemberKeyNode)));
                        }
                    }
                }
            }
            terms = L(terms, Y("let", "res", Y("AsList", "res")));
        }
        return terms;
    }

private:
    TSourcePtr Source;
    TVector<TNodePtr> GroupByExpr;
    TVector<TNodePtr> GroupBy;
    TVector<TSortSpecificationPtr> OrderBy;
    TNodePtr Having;
    TWinSpecs WinSpecs;
    TNodePtr Flatten;
    TNodePtr PreaggregatedMap;
    TNodePtr PrewindowMap;
    TNodePtr Aggregate;
    TNodePtr CalcOverWindow;
    TNodePtr FiltersGround;
    TNodePtr TermsGround;
    TNodePtr GroupByExprGround;
    TNodePtr HavingGround;
    TNodePtr OrderByGround;
    TNodePtr WinSpecsPartitionByGround;
    TNodePtr WinSpecsOrderByGround;
    TVector<TNodePtr> Terms;
    TVector<TNodePtr> Without;
    const bool Distinct;
    bool OrderByInit = false;
    THoppingWindowSpecPtr HoppingWindowSpec;
    const bool Stream;
    const TWriteSettings Settings;
};

class TProcessSource: public IRealSource {
public:
    TProcessSource(
        TPosition pos,
        TSourcePtr source,
        TNodePtr with,
        TVector<TNodePtr>&& terms,
        bool listCall,
        bool stream,
        const TWriteSettings& settings
    )
        : IRealSource(pos)
        , Source(std::move(source))
        , With(with)
        , Terms(std::move(terms))
        , ListCall(listCall)
        , Stream(stream)
        , Settings(settings)
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

        if (Stream && !Source->IsStream()) {
            ctx.Error(Pos) << "PROCESS STREAM is unsupported for non-streaming sources";
            return false;
        }

        if (!Stream && Source->IsStream() && !ctx.PragmaDirectRead) {
            ctx.Error(Pos) << "PROCESS STREAM must be used for streaming sources";
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
        ctx.PushBlockShortcuts();
        if (!Source->InitFilters(ctx)) {
            return false;
        }
        FiltersGround = ctx.GroundBlockShortcuts(Pos);

        // Use fake source in case of list process to restrict column access.
        TSourcePtr fakeSource;
        if (ListCall) {
            fakeSource = BuildFakeSource(src->GetPos());
            src->AllColumns();
        }

        auto processSource = ListCall ? fakeSource.Get() : src;
        Y_DEBUG_ABORT_UNLESS(processSource != nullptr);

        ctx.PushBlockShortcuts();
        if (!With->Init(ctx, processSource)) {
            return false;
        }
        if (With->GetLabel().empty()) {
            Columns.SetAll();
        } else {
            if (ListCall) {
                ctx.Error(With->GetPos()) << "Label is not allowed to use with $ROWS";
                return false;
            }

            Columns.Add(&With->GetLabel(), false);
        }

        bool hasError = false;
        auto produce = Y(ListCall ? "SqlProcess" : "Apply", With);
        TMaybe<ui32> listPosIndex;
        ui32 termIndex = 0;
        for (auto& term: Terms) {
            if (ListCall) {
                if (auto atom = dynamic_cast<TAstAtomNode*>(term.Get())) {
                    if (atom->GetContent() == "inputRowsList") {
                        listPosIndex = termIndex;
                    }
                }
            }
            ++termIndex;

            if (!term->GetLabel().empty()) {
                ctx.Error(term->GetPos()) << "Labels are not allowed for PROCESS terms";
                hasError = true;
                continue;
            }

            if (!term->Init(ctx, processSource)) {
                hasError = true;
                continue;
            }

            produce = L(produce, term);
        }

        if (ListCall) {
            produce = L(produce, Q(ToString(*listPosIndex)));
        }

        if (!produce->Init(ctx, src)) {
            hasError = true;
        }
        produce = ctx.GroundBlockShortcutsForExpr(produce);

        TVector<TNodePtr>(1, produce).swap(Terms);

        src->FinishColumns();

        if (hasError) {
            return false;
        }

        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        auto input = Source->Build(ctx);
        if (!input) {
            return nullptr;
        }

        if (!With) {
            return input;
        }

        TString inputLabel = ListCall ? "inputRowsList" : "core";

        auto block(Y(Y("let", inputLabel, input)));

        auto filter = Source->BuildFilter(ctx, inputLabel, FiltersGround);
        if (filter) {
            block = L(block, Y("let", inputLabel, filter));
        }

        if (ListCall) {
            block = L(block, Y("let", "core", Terms[0]));
        } else {
            auto terms = BuildColumnsTerms(ctx);
            block = L(block, Y("let", "core", Y(ctx.UseUnordered(*this) ? "OrderedFlatMap" : "FlatMap", "core", BuildLambda(Pos, Y("row"), terms, "res"))));
        }
        block = L(block, Y("let", "core", Y("AutoDemux", Y("EnsurePersistable", "core"))));
        return Y("block", Q(L(block, Y("return", "core"))));
    }

    bool IsSelect() const override {
        return false;
    }

    bool IsStream() const override {
        return Stream;
    }

    TWriteSettings GetWriteSettings() const override {
        return Settings;
    }

    TNodePtr DoClone() const final {
        return new TProcessSource(Pos, Source->CloneSource(), SafeClone(With),
            CloneContainer(Terms), ListCall, Stream, Settings);
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
            terms = TermsGround ? TermsGround : Y();
            terms = L(terms, Y("let", "res",
                L(Y("AsStruct"), Q(Y(BuildQuotedAtom(Pos, Columns.List.front()), Terms.front())))));
            terms = L(terms, Y("let", "res", Y("Just", "res")));
        }
        return terms;
    }

private:
    TSourcePtr Source;
    TNodePtr With;
    TNodePtr FiltersGround;
    TNodePtr TermsGround;
    TVector<TNodePtr> Terms;
    const bool ListCall;
    const bool Stream;
    const TWriteSettings Settings;
};

TSourcePtr BuildProcess(
    TPosition pos,
    TSourcePtr source,
    TNodePtr with,
    TVector<TNodePtr>&& terms,
    bool listCall,
    bool stream,
    const TWriteSettings& settings
) {
    return new TProcessSource(pos, std::move(source), with, std::move(terms), listCall, stream, settings);
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

    TNodePtr BuildFilter(TContext& ctx, const TString& label, const TNodePtr& groundNode) override {
        return CompositeSelect ? nullptr : Source->BuildFilter(ctx, label, groundNode);
    }

    bool IsCompositeSource() const override {
        return true;
    }

    ISource* GetCompositeSource() override {
        return CompositeSelect;
    }

    bool CalculateGroupingHint(TContext& ctx, const TVector<TString>& columns, ui64& hint) const override {
        Y_UNUSED(ctx);
        hint = 0;
        if (GroupByColumns.empty()) {
            for (const auto& groupByNode: GroupBy) {
                auto namePtr = groupByNode->GetColumnName();
                YQL_ENSURE(namePtr);
                GroupByColumns.insert(*namePtr);
            }
        }
        for (const auto& column: columns) {
            hint <<= 1;
            if (!GroupByColumns.contains(column)) {
                hint += 1;
            }
        }
        return true;
    }

    void FinishColumns() override {
        Source->FinishColumns();
    }

    TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column) override {
        return Source->AddColumn(ctx, column);
    }

    TPtr DoClone() const final {
        return Holder.Get() ? new TNestedProxySource(Pos, CloneContainer(GroupBy), Holder->CloneSource()) :
            new TNestedProxySource(CompositeSelect, CloneContainer(GroupBy));
    }

private:
    TCompositeSelect* CompositeSelect;
    TSourcePtr Holder;
    TVector<TNodePtr> GroupBy;
    mutable TSet<TString> GroupByColumns;
};

TSourcePtr BuildSelectCore(
    TContext& ctx,
    TPosition pos,
    TSourcePtr source,
    const TVector<TNodePtr>& groupByExpr,
    const TVector<TNodePtr>& groupBy,
    const TVector<TSortSpecificationPtr>& orderBy,
    TNodePtr having,
    TWinSpecs&& winSpecs,
    THoppingWindowSpecPtr hoppingWindowSpec,
    TVector<TNodePtr>&& terms,
    bool distinct,
    TVector<TNodePtr>&& without,
    bool stream,
    const TWriteSettings& settings
) {
    if (groupBy.empty() || !groupBy.front()->ContentListPtr()) {
        return new TSelectCore(pos, std::move(source), groupByExpr, groupBy, orderBy, having, winSpecs, hoppingWindowSpec, terms, distinct, without, stream, settings);
    }
    if (groupBy.size() == 1) {
        /// actualy no big idea to use grouping function in this case (result allways 0)
        auto contentPtr = groupBy.front()->ContentListPtr();
        TSourcePtr proxySource = new TNestedProxySource(pos, *contentPtr, std::move(source));
        return BuildSelectCore(ctx, pos, std::move(proxySource), groupByExpr, *contentPtr, orderBy, having, std::move(winSpecs),
            hoppingWindowSpec, std::move(terms), distinct, std::move(without), stream, settings);
    }
    /// \todo some smart merge logic, generalize common part of grouping (expr, flatten, etc)?
    TIntrusivePtr<TCompositeSelect> compositeSelect = new TCompositeSelect(pos, std::move(source), settings);
    size_t totalGroups = 0;
    TVector<TSourcePtr> subselects;
    TSet<TString> groupingCols;
    for (auto& grouping: groupBy) {
        auto contentPtr = grouping->ContentListPtr();
        TVector<TNodePtr> cache(1, nullptr);
        if (!contentPtr) {
            cache[0] = grouping;
            contentPtr = &cache;
        }
        for (const auto& elem: *contentPtr) {
            auto namePtr = elem->GetColumnName();
            if (namePtr && !namePtr->empty()) {
                groupingCols.insert(*namePtr);
            }
        }
        TSourcePtr proxySource = new TNestedProxySource(compositeSelect.Get(), *contentPtr);
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
            *contentPtr, orderBy, SafeClone(having), winSpecs, hoppingWindowSpec, terms, distinct, without, stream, settings);
        subselects.emplace_back(selectCore);
    }
    if (totalGroups > ctx.PragmaGroupByLimit) {
        ctx.Error(pos) << "Unable to GROUP BY more than " << ctx.PragmaGroupByLimit << " groups, you try use " << totalGroups << " groups";
        return nullptr;
    }
    compositeSelect->SetSubselects(std::move(subselects), std::move(groupingCols));
    return compositeSelect;
}

class TUnionAll: public IRealSource {
public:
    TUnionAll(TPosition pos, TVector<TSourcePtr>&& sources)
        : IRealSource(pos)
        , Sources(std::move(sources))
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
        for (auto& s: Sources) {
            s->UseAsInner();
            if (!s->Init(ctx, src)) {
                return false;
            }
            auto c = s->GetColumns();
            Y_DEBUG_ABORT_UNLESS(c);
            Columns.Merge(*c);
        }
        return true;
    }

    TNodePtr Build(TContext& ctx) override {
        auto res = Y("UnionAll");
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
        return MakeIntrusive<TUnionAll>(Pos, CloneContainer(Sources));
    }

private:
    TVector<TSourcePtr> Sources;
};

TSourcePtr BuildUnionAll(TPosition pos, TVector<TSourcePtr>&& sources) {
    return new TUnionAll(pos, std::move(sources));
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
        : TAstListNode(pos)
    {
        TNodePtr select(AstNode("select"));
        if (skip) {
            select = Y("Skip", select, skip);
        }
        Add("let", "select", Y("Take", select, take));
    }

    TPtr DoClone() const final {
        return {};
    }
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

        if (!Source->Init(ctx, src)) {
            return false;
        }
        src = Source.Get();
        if (SkipTake) {
            ctx.PushBlockShortcuts();
            FakeSource.Reset(new TFakeSource(SkipTake->GetPos()));
            if (!SkipTake->Init(ctx, FakeSource.Get())) {
                return false;
            }

            SkipTakeGround = ctx.GroundBlockShortcuts(ctx.Pos());
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
        if (sortNode) {
            if (AsInner && !SkipTake) {
                ctx.Warning(sortNode->GetPos(), TIssuesIds::YQL_ORDER_BY_WITHOUT_LIMIT_IN_SUBQUERY) << "ORDER BY without LIMIT in subquery will be ignored";
            } else {
                block = L(block, sortNode);
            }
        }

        if (SkipTake) {
            if (SkipTakeGround) {
                block = L(block, SkipTake->Y("let", "select", SkipTake->Y("block", SkipTake->Q(
                    SkipTake->L(SkipTake->L(SkipTakeGround, SkipTake), Y("return", "select"))))));
            } else {
                block = L(block, SkipTake);
            }
        }
        block = L(block, Y("return", label));
        return Y("block", Q(block));
    }

    bool IsSelect() const override {
        return Source->IsSelect();
    }

    TPtr DoClone() const final {
        return MakeIntrusive<TSelect>(Pos, Source->CloneSource(), SafeClone(SkipTake));
    }
protected:
    TSourcePtr Source;
    TNodePtr SkipTake;
    TNodePtr SkipTakeGround;
    THolder<TFakeSource> FakeSource;
};

TSourcePtr BuildSelect(TPosition pos, TSourcePtr source, TNodePtr skipTake) {
    return new TSelect(pos, std::move(source), skipTake);
}

class TSelectResultNode final: public TAstListNode {
public:
    TSelectResultNode(TPosition pos, TSourcePtr source, bool writeResult, bool inSubquery)
        : TAstListNode(pos)
        , Source(std::move(source))
        , WriteResult(writeResult)
        , InSubquery(inSubquery)
    {
        YQL_ENSURE(Source, "Invalid source node");
        FakeSource = BuildFakeSource(pos);
    }

    bool IsSelect() const override {
        return true;
    }

    bool DoInit(TContext& ctx, ISource* src) override {
        if (!Source->Init(ctx, src)) {
            return false;
        }

        src = Source.Get();
        TTableList tableList;
        Source->GetInputTables(tableList);

        TNodePtr node(BuildInputTables(Pos, tableList, InSubquery));
        if (!node->Init(ctx, src)) {
            return false;
        }

        TSet<TString> clusters;
        for (auto& it: tableList) {
            clusters.insert(it.Cluster);
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

            ctx.PushBlockShortcuts();
            if (!labelNode->Init(ctx, FakeSource.Get())) {
                return false;
            }

            labelNode = ctx.GroundBlockShortcutsForExpr(labelNode);
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
            for (auto& c: columns->List) {
                if (c.EndsWith('*')) {
                    list = L(list, Q(Y(Q("prefix"), BuildQuotedAtom(Pos, c.substr(0, c.size() - 1)))));
                } else {
                    list = L(list, BuildQuotedAtom(Pos, c));
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
        if (WriteResult) {
            if (!Source->IsOrdered() && ctx.UseUnordered(*Source)) {
                node = L(node, Y("let", "output", Y("Unordered", "output")));
            }
            auto writeResult(BuildWriteResult(Pos, "output", settings, clusters));
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
    TSourcePtr FakeSource;
};

TNodePtr BuildSelectResult(TPosition pos, TSourcePtr source, bool writeResult, bool inSubquery) {
    return new TSelectResultNode(pos, std::move(source), writeResult, inSubquery);
}

} // namespace NSQLTranslationV0
