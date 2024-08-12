#pragma once
#include "node.h"
#include "match_recognize.h"
#include <library/cpp/containers/sorted_vector/sorted_vector.h>

namespace NSQLTranslationV1 {
    using TColumnsSets = NSorted::TSimpleSet<NSorted::TSimpleSet<TString>>;

    class ISource;
    typedef TIntrusivePtr<ISource> TSourcePtr;

    struct TTableRef {
        TString RefName;
        TString Service;
        TDeferredAtom Cluster;
        TNodePtr Keys;
        TNodePtr Options;
        TSourcePtr Source;

        TTableRef() = default;
        TTableRef(const TString& refName, const TString& service, const TDeferredAtom& cluster, TNodePtr keys);
        TTableRef(const TTableRef&) = default;
        TTableRef& operator=(const TTableRef&) = default;

        TString ShortName() const;
    };

    typedef TVector<TTableRef> TTableList;


    class IJoin;
    class ISource: public INode {
    public:
        virtual ~ISource();

        virtual bool IsFake() const;
        virtual void AllColumns();
        virtual const TColumns* GetColumns() const;
        virtual void GetInputTables(TTableList& tableList) const;
        /// in case of error unfilled, flag show if ensure column name
        virtual TMaybe<bool> AddColumn(TContext& ctx, TColumnNode& column);
        virtual void FinishColumns();
        virtual bool AddExpressions(TContext& ctx, const TVector<TNodePtr>& columns, EExprSeat exprSeat);
        virtual void SetFlattenByMode(const TString& mode);
        virtual void MarkFlattenColumns();
        virtual bool IsFlattenColumns() const;
        virtual bool AddFilter(TContext& ctx, TNodePtr filter);
        virtual bool AddGroupKey(TContext& ctx, const TString& column);
        virtual void SetCompactGroupBy(bool compactGroupBy);
        virtual void SetGroupBySuffix(const TString& suffix);
        virtual TString MakeLocalName(const TString& name);
        virtual bool AddAggregation(TContext& ctx, TAggregationPtr aggr);
        virtual bool AddFuncOverWindow(TContext& ctx, TNodePtr expr);
        virtual void AddTmpWindowColumn(const TString& column);
        virtual void SetMatchRecognize(TMatchRecognizeBuilderPtr matchRecognize);
        virtual const TVector<TString>& GetTmpWindowColumns() const;
        virtual bool HasAggregations() const;
        virtual void AddWindowSpecs(TWinSpecs winSpecs);
        virtual bool AddAggregationOverWindow(TContext& ctx, const TString& windowName, TAggregationPtr func);
        virtual bool AddFuncOverWindow(TContext& ctx, const TString& windowName, TNodePtr func);
        virtual void SetLegacyHoppingWindowSpec(TLegacyHoppingWindowSpecPtr spec);
        virtual TLegacyHoppingWindowSpecPtr GetLegacyHoppingWindowSpec() const;
        virtual TNodePtr GetSessionWindowSpec() const;
        virtual TNodePtr GetHoppingWindowSpec() const;
        virtual bool IsCompositeSource() const;
        virtual bool IsGroupByColumn(const TString& column) const;
        virtual bool IsFlattenByColumns() const;
        virtual bool IsFlattenByExprs() const;
        virtual bool IsCalcOverWindow() const;
        virtual bool IsOverWindowSource() const;
        virtual bool IsStream() const;
        virtual EOrderKind GetOrderKind() const;
        virtual TWriteSettings GetWriteSettings() const;
        TNodePtr PrepareSamplingRate(TPosition pos, ESampleClause clause, TNodePtr samplingRate);
        virtual bool SetSamplingOptions(TContext& ctx, TPosition pos, ESampleClause clause, ESampleMode mode, TNodePtr samplingRate, TNodePtr samplingSeed);
        virtual bool SetTableHints(TContext& ctx, TPosition pos, const TTableHints& hints, const TTableHints& contextHints);
        virtual bool AddGrouping(TContext& ctx, const TVector<TString>& columns, TString& groupingColumn);
        virtual size_t GetGroupingColumnsCount() const;
        virtual TNodePtr BuildFilter(TContext& ctx, const TString& label);
        virtual TNodePtr BuildFilterLambda();
        virtual TNodePtr BuildFlattenByColumns(const TString& label);
        virtual TNodePtr BuildFlattenColumns(const TString& label);
        virtual TNodePtr BuildPreaggregatedMap(TContext& ctx);
        virtual TNodePtr BuildPreFlattenMap(TContext& ctx);
        virtual TNodePtr BuildPrewindowMap(TContext& ctx);
        virtual std::pair<TNodePtr, bool> BuildAggregation(const TString& label, TContext& ctx);
        virtual TNodePtr BuildCalcOverWindow(TContext& ctx, const TString& label);
        virtual TNodePtr BuildSort(TContext& ctx, const TString& label);
        virtual TNodePtr BuildCleanupColumns(TContext& ctx, const TString& label);
        virtual TNodePtr BuildGroupingColumns(const TString& label);
        virtual bool BuildSamplingLambda(TNodePtr& node);
        virtual bool SetSamplingRate(TContext& ctx, ESampleClause clause, TNodePtr samplingRate);
        virtual IJoin* GetJoin();
        virtual ISource* GetCompositeSource();
        virtual bool IsSelect() const;
        virtual bool IsTableSource() const;
        virtual bool ShouldUseSourceAsColumn(const TString& source) const;
        virtual bool IsJoinKeysInitializing() const;
        virtual const TString* GetWindowName() const;
        virtual bool HasMatchRecognize() const;
        virtual TNodePtr BuildMatchRecognize(TContext& ctx, TString&& inputTable);
        virtual bool DoInit(TContext& ctx, ISource* src);
        virtual TNodePtr Build(TContext& ctx) = 0;

        virtual TMaybe<TString> FindColumnMistype(const TString& name) const;

        virtual bool InitFilters(TContext& ctx);
        void AddDependentSource(ISource* usedSource);
        bool IsAlias(EExprSeat exprSeat, const TString& label) const;
        bool IsExprAlias(const TString& label) const;
        bool IsExprSeat(EExprSeat exprSeat, EExprType type = EExprType::WithExpression) const;
        TString GetGroupByColumnAlias(const TString& column) const;
        const TVector<TNodePtr>& Expressions(EExprSeat exprSeat) const;

        virtual TWindowSpecificationPtr FindWindowSpecification(TContext& ctx, const TString& windowName) const;

        TIntrusivePtr<ISource> CloneSource() const;
        TNodePtr BuildSortSpec(const TVector<TSortSpecificationPtr>& orderBy, const TString& label, bool traits, bool assume);

    protected:
        ISource(TPosition pos);
        virtual TAstNode* Translate(TContext& ctx) const;

        void FillSortParts(const TVector<TSortSpecificationPtr>& orderBy, TNodePtr& sortKeySelector, TNodePtr& sortDirection);

        TVector<TNodePtr>& Expressions(EExprSeat exprSeat);
        TNodePtr AliasOrColumn(const TNodePtr& node, bool withSource);

        TNodePtr BuildWindowFrame(const TFrameSpecification& spec, bool isCompact);

        THashSet<TString> ExprAliases;
        THashSet<TString> FlattenByAliases;
        THashMap<TString, TString> GroupByColumnAliases;
        TVector<TNodePtr> Filters;
        bool CompactGroupBy = false;
        TString GroupBySuffix;
        TSet<TString> GroupKeys;
        TVector<TString> OrderedGroupKeys;
        std::array<TVector<TNodePtr>, static_cast<unsigned>(EExprSeat::Max)> NamedExprs;
        TVector<TAggregationPtr> Aggregations;
        TMap<TString, TVector<TAggregationPtr>> AggregationOverWindow;
        TMap<TString, TVector<TNodePtr>> FuncOverWindow;
        TWinSpecs WinSpecs;
        TLegacyHoppingWindowSpecPtr LegacyHoppingWindowSpec;
        TNodePtr SessionWindow;
        TNodePtr HoppingWindow;
        TVector<ISource*> UsedSources;
        TString FlattenMode;
        bool FlattenColumns = false;
        THashMap<TString, ui32> GenIndexes;
        TVector<TString> TmpWindowColumns;
        TNodePtr SamplingRate;
        TMatchRecognizeBuilderPtr MatchRecognizeBuilder;
    };

    template<>
    inline TVector<TSourcePtr> CloneContainer<TSourcePtr>(const TVector<TSourcePtr>& args) {
        TVector<TSourcePtr> cloneArgs;
        cloneArgs.reserve(args.size());
        for (const auto& arg: args) {
            cloneArgs.emplace_back(arg ? arg->CloneSource() : nullptr);
        }
        return cloneArgs;
    }

    struct TJoinLinkSettings {
        enum class EStrategy {
            Default,
            SortedMerge,
            StreamLookup,
            ForceMap,
            ForceGrace
        };
        EStrategy Strategy = EStrategy::Default;
    };

    class IJoin: public ISource {
    public:
        virtual ~IJoin();

        virtual IJoin* GetJoin();
        virtual TNodePtr BuildJoinKeys(TContext& ctx, const TVector<TDeferredAtom>& names) = 0;
        virtual void SetupJoin(const TString& joinOp, TNodePtr joinExpr, const TJoinLinkSettings& linkSettings) = 0;
        virtual const THashMap<TString, THashSet<TString>>& GetSameKeysMap() const = 0;
        virtual TVector<TString> GetJoinLabels() const = 0;

    protected:
        IJoin(TPosition pos);
    };

    class TSessionWindow final : public INode {
    public:
        TSessionWindow(TPosition pos, const TVector<TNodePtr>& args);
        void MarkValid();
        TNodePtr BuildTraits(const TString& label) const;
    private:
        bool DoInit(TContext& ctx, ISource* src) override;
        TAstNode* Translate(TContext&) const override;
        void DoUpdateState() const override;
        TNodePtr DoClone() const override;
        TString GetOpName() const override;

        TVector<TNodePtr> Args;
        TSourcePtr FakeSource;
        TNodePtr Node;
        bool Valid;
    };

    class THoppingWindow final : public INode {
    public:
        THoppingWindow(TPosition pos, const TVector<TNodePtr>& args);
        void MarkValid();
        TNodePtr BuildTraits(const TString& label) const;
    public:
        TNodePtr Hop;
        TNodePtr Interval;
    private:
        bool DoInit(TContext& ctx, ISource* src) override;
        TAstNode* Translate(TContext&) const override;
        void DoUpdateState() const override;
        TNodePtr DoClone() const override;
        TString GetOpName() const override;
        TNodePtr ProcessIntervalParam(const TNodePtr& val) const;

        TVector<TNodePtr> Args;
        TSourcePtr FakeSource;
        TNodePtr Node;
        bool Valid;
    };


    // Implemented in join.cpp
    TString NormalizeJoinOp(const TString& joinOp);
    TSourcePtr BuildEquiJoin(TPosition pos, TVector<TSourcePtr>&& sources, TVector<bool>&& anyFlags, bool strictJoinKeyTypes);

    // Implemented in select.cpp
    TNodePtr BuildSubquery(TSourcePtr source, const TString& alias, bool inSubquery, int ensureTupleSize, TScopedStatePtr scoped);
    TNodePtr BuildSubqueryRef(TNodePtr subquery, const TString& alias, int tupleIndex = -1);
    TNodePtr BuildInvalidSubqueryRef(TPosition subqueryPos);
    TNodePtr BuildSourceNode(TPosition pos, TSourcePtr source, bool checkExist = false);
    TSourcePtr BuildMuxSource(TPosition pos, TVector<TSourcePtr>&& sources);
    TSourcePtr BuildFakeSource(TPosition pos, bool missingFrom = false, bool inSubquery = false);
    TSourcePtr BuildNodeSource(TPosition pos, const TNodePtr& node, bool wrapToList = false);
    TSourcePtr BuildTableSource(TPosition pos, const TTableRef& table, const TString& label = TString());
    TSourcePtr BuildInnerSource(TPosition pos, TNodePtr node, const TString& service, const TDeferredAtom& cluster, const TString& label = TString());
    TSourcePtr BuildRefColumnSource(TPosition pos, const TString& partExpression);
    TSourcePtr BuildUnion(TPosition pos, TVector<TSourcePtr>&& sources, bool quantifierAll, const TWriteSettings& settings);
    TSourcePtr BuildOverWindowSource(TPosition pos, const TString& windowName, ISource* origSource);

    TNodePtr BuildOrderBy(TPosition pos, const TVector<TNodePtr>& keys, const TVector<bool>& order);
    TNodePtr BuildSkipTake(TPosition pos, const TNodePtr& skip, const TNodePtr& take);


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
        TWinSpecs&& windowSpec,
        TLegacyHoppingWindowSpecPtr legacyHoppingWindowSpec,
        TVector<TNodePtr>&& terms,
        bool distinct,
        TVector<TNodePtr>&& without,
        bool selectStream,
        const TWriteSettings& settings,
        TColumnsSets&& uniqueSets,
        TColumnsSets&& distinctSets
    );
    TSourcePtr BuildSelect(TPosition pos, TSourcePtr source, TNodePtr skipTake);


    enum class ReduceMode {
        ByPartition,
        ByAll,
    };
    TSourcePtr BuildReduce(TPosition pos, ReduceMode mode, TSourcePtr source, TVector<TSortSpecificationPtr>&& orderBy,
        TVector<TNodePtr>&& keys, TVector<TNodePtr>&& args, TNodePtr udf, TNodePtr having, const TWriteSettings& settings,
        const TVector<TSortSpecificationPtr>& assumeOrderBy, bool listCall);
    TSourcePtr BuildProcess(TPosition pos, TSourcePtr source, TNodePtr with, bool withExtFunction, TVector<TNodePtr>&& terms, bool listCall,
        bool prcessStream, const TWriteSettings& settings, const TVector<TSortSpecificationPtr>& assumeOrderBy);

    TNodePtr BuildSelectResult(TPosition pos, TSourcePtr source, bool writeResult, bool inSubquery, TScopedStatePtr scoped);

    // Implemented in insert.cpp
    TSourcePtr BuildWriteValues(TPosition pos, const TString& opertationHumanName, const TVector<TString>& columnsHint, const TVector<TVector<TNodePtr>>& values);
    TSourcePtr BuildWriteValues(TPosition pos, const TString& opertationHumanName, const TVector<TString>& columnsHint, TSourcePtr source);
    TSourcePtr BuildUpdateValues(TPosition pos, const TVector<TString>& columnsHint, const TVector<TNodePtr>& values);

    EWriteColumnMode ToWriteColumnsMode(ESQLWriteColumnMode sqlWriteColumnMode);
    TNodePtr BuildEraseColumns(TPosition pos, const TVector<TString>& columns);
    TNodePtr BuildIntoTableOptions(TPosition pos, const TVector<TString>& eraseColumns, const TTableHints& hints);
    TNodePtr BuildWriteColumns(TPosition pos, TScopedStatePtr scoped, const TTableRef& table, EWriteColumnMode mode, TSourcePtr values, TNodePtr options = nullptr);
    TNodePtr BuildUpdateColumns(TPosition pos, TScopedStatePtr scoped, const TTableRef& table, TSourcePtr values, TSourcePtr source, TNodePtr options = nullptr);
    TNodePtr BuildDelete(TPosition pos, TScopedStatePtr scoped, const TTableRef& table, TSourcePtr source, TNodePtr options = nullptr);

    // Implemented in query.cpp
    TNodePtr BuildTableKey(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TViewDescription& view);
    TNodePtr BuildTableKeys(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TString& func, const TVector<TTableArg>& args);
    TNodePtr BuildTopicKey(TPosition pos, const TDeferredAtom& cluster, const TDeferredAtom& name);
    TNodePtr BuildInputOptions(TPosition pos, const TTableHints& hints);
    TNodePtr BuildInputTables(TPosition pos, const TTableList& tables, bool inSubquery, TScopedStatePtr scoped);
    TNodePtr BuildCreateTable(TPosition pos, const TTableRef& tr, bool existingOk, bool replaceIfExists, const TCreateTableParameters& params, TSourcePtr source, TScopedStatePtr scoped);
    TNodePtr BuildAlterTable(TPosition pos, const TTableRef& tr, const TAlterTableParameters& params, TScopedStatePtr scoped);
    TNodePtr BuildDropTable(TPosition pos, const TTableRef& table, bool missingOk, ETableType tableType, TScopedStatePtr scoped);
    TNodePtr BuildWriteTable(TPosition pos, const TString& label, const TTableRef& table, EWriteColumnMode mode, TNodePtr options,
        TScopedStatePtr scoped);
    TNodePtr BuildAnalyze(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TAnalyzeParams& params, TScopedStatePtr scoped);
    TSourcePtr TryMakeSourceFromExpression(TPosition pos, TContext& ctx, const TString& currService, const TDeferredAtom& currCluster,
        TNodePtr node, const TString& view = {});
    void MakeTableFromExpression(TPosition pos, TContext& ctx, TNodePtr node, TDeferredAtom& table, const TString& prefix = {});
    TDeferredAtom MakeAtomFromExpression(TPosition pos, TContext& ctx, TNodePtr node, const TString& prefix = {});
    TString NormalizeTypeString(const TString& str);
}  // namespace NSQLTranslationV1
