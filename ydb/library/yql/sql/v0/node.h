#pragma once

#include <array>
#include <google/protobuf/message.h>
#include <ydb/library/yql/parser/proto_ast/proto_ast.h>
#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <util/generic/vector.h>
#include <util/generic/set.h>
#include <util/generic/map.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/string/builder.h>

#include <library/cpp/enumbitset/enumbitset.h>

namespace NSQLTranslationV0 {
    constexpr const size_t SQL_MAX_INLINE_SCRIPT_LEN = 24;

    using NYql::TPosition;
    using NYql::TAstNode;

    enum class ENodeState {
        Begin,
        Precached = Begin,
        Initialized,
        CountHint,
        Const,
        Aggregated,
        AggregationKey,
        OverWindow,
        Failed,
        End,
    };
    typedef TEnumBitSet<ENodeState, static_cast<int>(ENodeState::Begin), static_cast<int>(ENodeState::End)> TNodeState;

    enum class ESQLWriteColumnMode {
        InsertInto,
        InsertOrAbortInto,
        InsertOrIgnoreInto,
        InsertOrRevertInto,
        UpsertInto,
        ReplaceInto,
        InsertIntoWithTruncate,
        Update,
        Delete,
    };

    enum class EWriteColumnMode {
        Default,
        Insert,
        InsertOrAbort,
        InsertOrIgnore,
        InsertOrRevert,
        Upsert,
        Replace,
        Renew,
        Update,
        UpdateOn,
        Delete,
        DeleteOn,
    };

    enum class EAlterTableIntentnt {
        AddColumn,
        DropColumn
    };

    class TContext;
    class ITableKeys;
    class ISource;
    class IAggregation;
    typedef TIntrusivePtr<IAggregation> TAggregationPtr;

    inline TString DotJoin(const TString& lhs, const TString& rhs) {
        TStringBuilder sb;
        sb << lhs << "." << rhs;
        return sb;
    }

    TString ErrorDistinctByGroupKey(const TString& column);
    TString ErrorDistinctWithoutCorrelation(const TString& column);

    class INode: public TSimpleRefCount<INode> {
    public:
        typedef TIntrusivePtr<INode> TPtr;

        struct TIdPart {
            TString Name;
            TPtr Expr;
            i32 Pos = -1;

            TIdPart(const TString& name)
                : Name(name)
            {
            }
            TIdPart(TPtr expr)
                : Expr(expr)
            {
            }
            TIdPart(i32 pos)
                : Pos(pos)
            {
            }
            TIdPart Clone() const {
                TIdPart res(Name);
                res.Pos = Pos;
                res.Expr = Expr ? Expr->Clone() : nullptr;
                return res;
            }
        };

    public:
        INode(TPosition pos);
        virtual ~INode();

        TPosition GetPos() const;
        const TString& GetLabel() const;
        void SetLabel(const TString& label);

        void SetCountHint(bool isCount);
        bool GetCountHint() const;
        bool Init(TContext& ctx, ISource* src);

        bool IsConstant() const;
        bool IsAggregated() const;
        bool IsAggregationKey() const;
        bool IsOverWindow() const;
        bool HasState(ENodeState state) const {
            PrecacheState();
            return State.Test(state);
        }

        virtual bool IsNull() const;
        virtual bool IsIntegerLiteral() const;
        virtual bool IsAsterisk() const;
        virtual const TString* SubqueryAlias() const;
        virtual TString GetOpName() const;
        virtual const TString* GetLiteral(const TString& type) const;
        virtual const TString* GetColumnName() const;
        virtual void AssumeColumn();
        virtual const TString* GetSourceName() const;
        virtual const TString* GetAtomContent() const;
        virtual size_t GetTupleSize() const;
        virtual TPtr GetTupleElement(size_t index) const;
        virtual ITableKeys* GetTableKeys();
        virtual ISource* GetSource();
        virtual TVector<INode::TPtr>* ContentListPtr();
        virtual TAstNode* Translate(TContext& ctx) const = 0;
        virtual TAggregationPtr GetAggregation() const;
        virtual TPtr WindowSpecFunc(const TPtr& type) const;
        void UseAsInner();
        virtual bool UsedSubquery() const;
        virtual bool IsSelect() const;

        TPtr AstNode() const;
        TPtr AstNode(TAstNode* node) const;
        TPtr AstNode(TPtr node) const;
        TPtr AstNode(const TString& str) const;

        template <typename TVal, typename... TVals>
        void Add(TVal val, TVals... vals) {
            DoAdd(AstNode(val));
            Add(vals...);
        }

        void Add() {}

        // Y() Q() L()
        TPtr Y() const {
            return AstNode();
        }

        template <typename... TVals>
        TPtr Y(TVals... vals) const {
            TPtr node(AstNode());
            node->Add(vals...);
            return node;
        }

        template <typename T>
        TPtr Q(T a) const {
            return Y("quote", a);
        }

        template <typename... TVals>
        TPtr L(TPtr list, TVals... vals) const {
            Y_DEBUG_ABORT_UNLESS(list);
            auto copy = list->ShallowCopy();
            copy->Add(vals...);
            return copy;
        }

        TPtr Clone() const;
    protected:
        virtual TPtr ShallowCopy() const;
        virtual void DoUpdateState() const;
        virtual TPtr DoClone() const = 0;
        void PrecacheState() const;

    private:
        virtual bool DoInit(TContext& ctx, ISource* src);
        virtual void DoAdd(TPtr node);

    protected:
        TPosition Pos;
        TString Label;
        mutable TNodeState State;
        bool AsInner = false;
    };
    typedef INode::TPtr TNodePtr;

    template<class T>
    inline T SafeClone(const T& node) {
        return node ? node->Clone() : nullptr;
    }

    template<class T>
    inline TVector<T> CloneContainer(const TVector<T>& args) {
        TVector<T> cloneArgs;
        cloneArgs.reserve(args.size());
        for (const auto& arg: args) {
            cloneArgs.emplace_back(SafeClone(arg));
        }
        return cloneArgs;
    }

    class TAstAtomNode: public INode {
    public:
        TAstAtomNode(TPosition pos, const TString& content, ui32 flags);

        ~TAstAtomNode() override;

        TAstNode* Translate(TContext& ctx) const override;
        const TString& GetContent() const {
            return Content;
        }

        const TString* GetAtomContent() const override;

    protected:
        TString Content;
        ui32 Flags;

        void DoUpdateState() const override;
    };

    class TAstAtomNodeImpl final: public TAstAtomNode {
    public:
        TAstAtomNodeImpl(TPosition pos, const TString& content, ui32 flags)
            : TAstAtomNode(pos, content, flags)
        {}

        TNodePtr DoClone() const final {
            return new TAstAtomNodeImpl(Pos, Content, Flags);
        }
    };

    class TAstDirectNode final: public INode {
    public:
        TAstDirectNode(TAstNode* node);

        TAstNode* Translate(TContext& ctx) const override;

        TPtr DoClone() const final {
            return new TAstDirectNode(Node);
        }
    protected:
        TAstNode* Node;
    };

    class TAstListNode: public INode {
    public:
        TAstListNode(TPosition pos);
        virtual ~TAstListNode();

        TAstNode* Translate(TContext& ctx) const override;

    protected:
        explicit TAstListNode(const TAstListNode& node);
        explicit TAstListNode(TPosition pos, TVector<TNodePtr>&& nodes);
        TPtr ShallowCopy() const override;
        bool DoInit(TContext& ctx, ISource* src) override;
        void DoAdd(TNodePtr node) override;

        void DoUpdateState() const override;

        void UpdateStateByListNodes(const TVector<TNodePtr>& Nodes) const;

    protected:
        TVector<TNodePtr> Nodes;
        mutable TMaybe<bool> CacheGroupKey;
    };

    class TAstListNodeImpl final: public TAstListNode {
    public:
        TAstListNodeImpl(TPosition pos);
        TAstListNodeImpl(TPosition pos, TVector<TNodePtr> nodes);

    protected:
        TNodePtr DoClone() const final;
    };

    class TCallNode: public TAstListNode {
    public:
        TCallNode(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);
        TCallNode(TPosition pos, const TString& opName, const TVector<TNodePtr>& args)
            : TCallNode(pos, opName, args.size(), args.size(), args)
        {}

        TString GetOpName() const override;
        const TString* GetSourceName() const override;

        const TVector<TNodePtr>& GetArgs() const;

    protected:
        bool DoInit(TContext& ctx, ISource* src) override;
        bool ValidateArguments(TContext& ctx) const;
        TString GetCallExplain() const;

    protected:
        TString OpName;
        i32 MinArgs;
        i32 MaxArgs;
        TVector<TNodePtr> Args;
        mutable TMaybe<bool> CacheGroupKey;

        void DoUpdateState() const override;
    };

    class TCallNodeImpl final: public TCallNode {
        TPtr DoClone() const final;
    public:
        TCallNodeImpl(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);
        TCallNodeImpl(TPosition pos, const TString& opName, const TVector<TNodePtr>& args);
    };

    class TCallNodeDepArgs final : public TCallNode {
        TPtr DoClone() const final;
    public:
        TCallNodeDepArgs(ui32 reqArgsCount, TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);
        TCallNodeDepArgs(ui32 reqArgsCount, TPosition pos, const TString& opName, const TVector<TNodePtr>& args);
    protected:
        bool DoInit(TContext& ctx, ISource* src) override;

    private:
        const ui32 ReqArgsCount;
    };

    class TCallDirectRow final : public TCallNode {
        TPtr DoClone() const final;
    public:
        TCallDirectRow(TPosition pos, const TString& opName, const TVector<TNodePtr>& args);
    protected:
        bool DoInit(TContext& ctx, ISource* src) override;
        void DoUpdateState() const override;
    };

    class TWinAggrEmulation: public TCallNode {
    protected:
        void DoUpdateState() const override;
        bool DoInit(TContext& ctx, ISource* src) override;
        TPtr WindowSpecFunc(const TNodePtr& type) const override;
    public:
        TWinAggrEmulation(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);
    protected:
        template<class TNodeType>
        TPtr CallNodeClone() const {
            return new TNodeType(GetPos(), OpName, MinArgs, MaxArgs, CloneContainer(Args));
        }
        TString FuncAlias;
        TNodePtr WinAggrGround;
    };

    class TWinRowNumber final: public TWinAggrEmulation {
        TPtr DoClone() const final {
            return CallNodeClone<TWinRowNumber>();
        }
    public:
        TWinRowNumber(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);
    };

    class TWinLeadLag final: public TWinAggrEmulation {
        TPtr DoClone() const final {
            return CallNodeClone<TWinLeadLag>();
        }
        bool DoInit(TContext& ctx, ISource* src) override;
    public:
        TWinLeadLag(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);
    };

    class ITableKeys: public INode {
    public:
        enum class EBuildKeysMode {
            CREATE,
            DROP,
            INPUT,
            WRITE
        };

        ITableKeys(TPosition pos);
        virtual const TString* GetTableName() const;
        virtual TNodePtr BuildKeys(TContext& ctx, EBuildKeysMode mode) = 0;

    private:
        /// all TableKeys no clonnable
        TPtr DoClone() const final {
            return {};
        }

        ITableKeys* GetTableKeys() override;
        TAstNode* Translate(TContext& ctx) const override;
    };

    enum class ESampleMode {
        Auto,
        Bernoulli,
        System
    };

    struct TTableRef {
        const TString RefName;
        const TString Cluster;
        TNodePtr Keys;
        TNodePtr Options;

        TTableRef(const TString& refName, const TString& cluster, TNodePtr keys);
        TTableRef(const TTableRef& tr);

        TString ShortName() const;
        TString ServiceName(const TContext& ctx) const;

        bool Check(TContext& ctx) const;
    };

    struct TIdentifier
    {
        TPosition Pos;
        TString Name;

        TIdentifier(TPosition pos, const TString& name)
            : Pos(pos)
            , Name(name) {}
    };

    struct TColumnSchema {
        TPosition Pos;
        TString Name;
        TString Type;
        bool Nullable;
        bool IsTypeString;

        TColumnSchema(TPosition pos, const TString& name, const TString& type, bool nullable, bool isTypeString);
    };

    struct TColumns: public TSimpleRefCount<TColumns> {
        TSet<TString> Real;
        TSet<TString> Artificial;
        TVector<TString> List;
        TVector<bool> NamedColumns;
        bool All = false;
        bool QualifiedAll = false;
        bool HasUnreliable = false;

        bool Add(const TString* column, bool countHint, bool isArtificial = false, bool isReliable = true, bool hasName = true);
        void Merge(const TColumns& columns);
        void SetPrefix(const TString& prefix);
        void SetAll();
        bool IsColumnPossible(TContext& ctx, const TString& column);
    };

    struct TSortSpecification: public TSimpleRefCount<TSortSpecification> {
        TNodePtr OrderExpr;
        bool Ascending;
        TIntrusivePtr<TSortSpecification> Clone() const;
        ~TSortSpecification() {}
    };
    typedef TIntrusivePtr<TSortSpecification> TSortSpecificationPtr;

    enum EFrameType {
        FrameByRows,
        FrameByRange,
    };
    enum EFrameExclusions {
        FrameExclNone,
        FrameExclCurRow,
        FrameExclGroup,
        FrameExclTies,
        FrameExclNoOthers,
    };
    struct TFrameSpecification {
        EFrameType FrameType;
        TNodePtr FrameBegin;
        TNodePtr FrameEnd;
        EFrameExclusions FrameExclusion = FrameExclNone;
    };

    struct TWindowSpecification: public TSimpleRefCount<TWindowSpecification> {
        TMaybe<TString> ExistingWindowName;
        TVector<TNodePtr> Partitions;
        TVector<TSortSpecificationPtr> OrderBy;
        TMaybe<TFrameSpecification> Frame;

        TIntrusivePtr<TWindowSpecification> Clone() const;
        ~TWindowSpecification() {}
    };

    struct THoppingWindowSpec: public TSimpleRefCount<THoppingWindowSpec> {
        TNodePtr TimeExtractor;
        TNodePtr Hop;
        TNodePtr Interval;
        TNodePtr Delay;

        TIntrusivePtr<THoppingWindowSpec> Clone() const;
        ~THoppingWindowSpec() {}
    };

    typedef TIntrusivePtr<TWindowSpecification> TWindowSpecificationPtr;
    typedef TMap<TString, TWindowSpecificationPtr> TWinSpecs;

    typedef TVector<TTableRef> TTableList;

    typedef TIntrusivePtr<THoppingWindowSpec> THoppingWindowSpecPtr;

    bool ValidateAllNodesForAggregation(TContext& ctx, const TVector<TNodePtr>& nodes);

    class TDeferredAtom {
    public:
        TDeferredAtom();
        TDeferredAtom(TPosition pos, const TString& str);
        TDeferredAtom(TNodePtr node, TContext& ctx);
        const TString* GetLiteral() const;
        TNodePtr Build() const;
        TString GetRepr() const;
        bool Empty() const;

    private:
        TMaybe<TString> Explicit;
        TNodePtr Node; // atom or evaluation node
        TString Repr;
    };

    struct TWriteSettings {
        bool Discard;
        TDeferredAtom Label;
    };

    class TColumnNode final: public INode {
    public:
        TColumnNode(TPosition pos, const TString& column, const TString& source);
        TColumnNode(TPosition pos, const TNodePtr& column, const TString& source);

        virtual ~TColumnNode();
        bool IsAsterisk() const override;
        virtual bool IsArtificial() const;
        const TString* GetColumnName() const override;
        const TString* GetSourceName() const override;
        TAstNode* Translate(TContext& ctx) const override;
        void ResetColumn(const TString& column, const TString& source);
        void ResetColumn(const TNodePtr& column, const TString& source);

        void SetUseSourceAsColumn();
        void SetUseSource();
        void ResetAsReliable();
        void SetAsNotReliable();
        bool IsReliable() const;
        bool IsUseSourceAsColumn() const;

    private:
        bool DoInit(TContext& ctx, ISource* src) override;
        TPtr DoClone() const final;

        void DoUpdateState() const override;

    private:
        static const TString Empty;
        TNodePtr Node;
        TString ColumnName;
        TNodePtr ColumnExpr;
        TString Source;
        bool GroupKey = false;
        bool Artificial = false;
        bool Reliable = true;
        bool UseSource = false;
        bool UseSourceAsColumn = false;
    };

    class TArgPlaceholderNode final: public INode
    {
    public:
        static const char* const ProcessRows;
        static const char* const ProcessRow;
    public:
        TArgPlaceholderNode(TPosition pos, const TString &name);

        TAstNode* Translate(TContext& ctx) const override;

        TString GetName() const;
        TNodePtr DoClone() const final;

    protected:
        bool DoInit(TContext& ctx, ISource* src) override;

    private:
        TString Name;
    };

    enum class EAggregateMode {
        Normal,
        Distinct,
        OverWindow,
    };

    class TTupleNode: public TAstListNode {
    public:
        TTupleNode(TPosition pos, const TVector<TNodePtr>& exprs);

        bool IsEmpty() const;
        const TVector<TNodePtr>& Elements() const;
        bool DoInit(TContext& ctx, ISource* src) override;
        size_t GetTupleSize() const override;
        TPtr GetTupleElement(size_t index) const override;
        TNodePtr DoClone() const final;
    private:
        const TVector<TNodePtr> Exprs;
    };

    class TStructNode: public TAstListNode {
    public:
        TStructNode(TPosition pos, const TVector<TNodePtr>& exprs);

        bool DoInit(TContext& ctx, ISource* src) override;
        TNodePtr DoClone() const final;
        const TVector<TNodePtr>& GetExprs() {
            return Exprs;
        }

    private:
        const TVector<TNodePtr> Exprs;
    };

    class IAggregation: public INode {
    public:
        bool IsDistinct() const;

        void DoUpdateState() const override;

        virtual const TString* GetGenericKey() const;

        virtual bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) = 0;

        virtual TNodePtr AggregationTraits(const TNodePtr& type) const;

        virtual TNodePtr AggregationTraitsFactory() const = 0;

        virtual std::vector<ui32> GetFactoryColumnIndices() const;

        virtual void AddFactoryArguments(TNodePtr& apply) const;

        virtual TNodePtr WindowTraits(const TNodePtr& type) const;

        const TString& GetName() const;

        virtual void Join(IAggregation* aggr);

    private:
        virtual TNodePtr GetApply(const TNodePtr& type) const = 0;

    protected:
        IAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode mode);
        TAstNode* Translate(TContext& ctx) const override;

        TString Name;
        const TString Func;
        const EAggregateMode AggMode;
        TString DistinctKey;
    };

    enum class EExprSeat: int {
        Open = 0,
        FlattenBy,
        GroupBy,
        Projection,
        WindowPartitionBy,
        Max
    };

    enum class EExprType: int {
        WithExpression,
        ColumnOnly,
    };

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
        virtual TString MakeLocalName(const TString& name);
        virtual bool AddAggregation(TContext& ctx, TAggregationPtr aggr);
        virtual bool AddFuncOverWindow(TContext& ctx, TNodePtr expr);
        virtual void AddTmpWindowColumn(const TString& column);
        virtual const TVector<TString>& GetTmpWindowColumns() const;
        virtual bool HasAggregations() const;
        virtual void AddWindowSpecs(TWinSpecs winSpecs);
        virtual bool AddAggregationOverWindow(TContext& ctx, const TString& windowName, TAggregationPtr func);
        virtual bool AddFuncOverWindow(TContext& ctx, const TString& windowName, TNodePtr func);
        virtual void SetHoppingWindowSpec(THoppingWindowSpecPtr spec);
        virtual THoppingWindowSpecPtr GetHoppingWindowSpec() const;
        virtual bool IsCompositeSource() const;
        virtual bool IsGroupByColumn(const TString& column) const;
        virtual bool IsFlattenByColumns() const;
        virtual bool IsCalcOverWindow() const;
        virtual bool IsOverWindowSource() const;
        virtual bool IsStream() const;
        virtual bool IsOrdered() const;
        virtual TWriteSettings GetWriteSettings() const;
        virtual bool SetSamplingOptions(TContext& ctx, TPosition pos, ESampleMode mode, TNodePtr samplingRate, TNodePtr samplingSeed);
        virtual bool CalculateGroupingHint(TContext& ctx, const TVector<TString>& columns, ui64& hint) const;
        virtual TNodePtr BuildFilter(TContext& ctx, const TString& label, const TNodePtr& groundNode);
        virtual TNodePtr BuildFilterLambda(const TNodePtr& groundNode);
        virtual TNodePtr BuildFlattenByColumns(const TString& label);
        virtual TNodePtr BuildFlattenColumns(const TString& label);
        virtual TNodePtr BuildPreaggregatedMap(TContext& ctx);
        virtual TNodePtr BuildPrewindowMap(TContext& ctx, const TNodePtr& groundNode);
        virtual TNodePtr BuildAggregation(const TString& label);
        virtual TNodePtr BuildCalcOverWindow(TContext& ctx, const TString& label, const TNodePtr& ground);
        virtual TNodePtr BuildSort(TContext& ctx, const TString& label);
        virtual IJoin* GetJoin();
        virtual ISource* GetCompositeSource();
        virtual bool IsSelect() const;
        virtual bool IsTableSource() const;
        virtual bool ShouldUseSourceAsColumn(const TString& source);
        virtual bool IsJoinKeysInitializing() const;
        virtual const TString* GetWindowName() const;

        virtual bool DoInit(TContext& ctx, ISource* src);
        virtual TNodePtr Build(TContext& ctx) = 0;

        virtual TMaybe<TString> FindColumnMistype(const TString& name) const;

        virtual bool InitFilters(TContext& ctx);
        void AddDependentSource(ISource* usedSource);
        bool IsAlias(EExprSeat exprSeat, const TString& label) const;
        bool IsExprAlias(const TString& label) const;
        bool IsExprSeat(EExprSeat exprSeat, EExprType type = EExprType::WithExpression) const;
        TString GetGroupByColumnAlias(const TString& column) const;

        virtual TWindowSpecificationPtr FindWindowSpecification(TContext& ctx, const TString& windowName) const;

        TIntrusivePtr<ISource> CloneSource() const;

    protected:
        ISource(TPosition pos);
        virtual TAstNode* Translate(TContext& ctx) const;

        void FillSortParts(const TVector<TSortSpecificationPtr>& orderBy, TNodePtr& sortKeySelector, TNodePtr& sortDirection);
        TNodePtr BuildSortSpec(const TVector<TSortSpecificationPtr>& orderBy, const TString& label, const TNodePtr& ground, bool traits = false);

        TVector<TNodePtr>& Expressions(EExprSeat exprSeat);
        const TVector<TNodePtr>& Expressions(EExprSeat exprSeat) const;
        TNodePtr AliasOrColumn(const TNodePtr& node, bool withSource);

        THashSet<TString> ExprAliases;
        THashMap<TString, TString> GroupByColumnAliases;
        TVector<TNodePtr> Filters;
        TSet<TString> GroupKeys;
        TVector<TString> OrderedGroupKeys;
        std::array<TVector<TNodePtr>, static_cast<unsigned>(EExprSeat::Max)> NamedExprs;
        TVector<TAggregationPtr> Aggregations;
        TMultiMap<TString, TAggregationPtr> AggregationOverWindow;
        TMultiMap<TString, TNodePtr> FuncOverWindow;
        TWinSpecs WinSpecs;
        THoppingWindowSpecPtr HoppingWindowSpec;
        TVector<ISource*> UsedSources;
        TString FlattenMode;
        bool FlattenColumns = false;
        THashMap<TString, ui32> GenIndexes;
        TVector<TString> TmpWindowColumns;
    };

    typedef TIntrusivePtr<ISource> TSourcePtr;
    template<>
    inline TVector<TSourcePtr> CloneContainer<TSourcePtr>(const TVector<TSourcePtr>& args) {
        TVector<TSourcePtr> cloneArgs;
        cloneArgs.reserve(args.size());
        for (const auto& arg: args) {
            cloneArgs.emplace_back(arg ? arg->CloneSource() : nullptr);
        }
        return cloneArgs;
    }

    class IJoin: public ISource {
    public:
        virtual ~IJoin();

        virtual IJoin* GetJoin();
        virtual TNodePtr BuildJoinKeys(TContext& ctx, const TVector<TDeferredAtom>& names) = 0;
        virtual void SetupJoin(const TString& joinOp, TNodePtr joinExpr) = 0;
        virtual const THashMap<TString, THashSet<TString>>& GetSameKeysMap() const = 0;
        virtual const TSet<TString> GetJoinLabels() const = 0;

    protected:
        IJoin(TPosition pos);
    };

    class TListOfNamedNodes final: public INode {
    public:
        TListOfNamedNodes(TPosition pos, TVector<TNodePtr>&& exprs);

        TVector<TNodePtr>* ContentListPtr() override;
        TAstNode* Translate(TContext& ctx) const override;
        TPtr DoClone() const final;
    private:
        TVector<TNodePtr> Exprs;
        TString Meaning;
    };

    class TLiteralNode: public TAstListNode {
    public:
        TLiteralNode(TPosition pos, bool isNull);
        TLiteralNode(TPosition pos, const TString& type, const TString& value);
        TLiteralNode(TPosition pos, const TString& value, ui32 nodeFlags);
        bool IsNull() const override;
        const TString* GetLiteral(const TString& type) const override;
        void DoUpdateState() const override;
        TPtr DoClone() const override;
    protected:
        bool Null;
        bool Void;
        TString Type;
        TString Value;
    };

    template<typename T>
    class TLiteralNumberNode: public TLiteralNode {
    public:
        TLiteralNumberNode(TPosition pos, const TString& type, const TString& value);
        TPtr DoClone() const override final;
        bool DoInit(TContext& ctx, ISource* src) override;
        bool IsIntegerLiteral() const override;
    };

    struct TTableArg {
        bool HasAt = false;
        TNodePtr Expr;
        TDeferredAtom Id;
        TString View;
    };

    TString StringContent(TContext& ctx, const TString& str);
    bool TryStringContent(const TString& input, TString& result, ui32& flags, TString& error, TPosition& pos);
    TString IdContent(TContext& ctx, const TString& str);
    TVector<TString> GetContextHints(TContext& ctx);

    TString TypeByAlias(const TString& alias, bool normalize = true);

    TNodePtr BuildAtom(TPosition pos, const TString& content, ui32 flags = NYql::TNodeFlags::ArbitraryContent);
    TNodePtr BuildQuotedAtom(TPosition pos, const TString& content, ui32 flags = NYql::TNodeFlags::ArbitraryContent);

    TNodePtr BuildLiteralNull(TPosition pos);
    TNodePtr BuildLiteralVoid(TPosition pos);
    /// String is checked as quotable, support escaping and multiline
    TNodePtr BuildLiteralSmartString(TContext& ctx, const TString& value);
    TNodePtr BuildLiteralRawString(TPosition pos, const TString& value);
    TNodePtr BuildLiteralBool(TPosition pos, const TString& value);
    TNodePtr BuildEmptyAction(TPosition pos);

    TNodePtr BuildTuple(TPosition pos, const TVector<TNodePtr>& exprs);
    TNodePtr BuildStructure(TPosition pos, const TVector<TNodePtr>& exprs);
    TNodePtr BuildListOfNamedNodes(TPosition pos, TVector<TNodePtr>&& exprs);

    TNodePtr BuildArgPlaceholder(TPosition pos, const TString& name);

    TNodePtr BuildColumn(TPosition pos, const TString& column = TString(), const TString& source = TString());
    TNodePtr BuildColumn(TPosition pos, const TNodePtr& column, const TString& source = TString());
    TNodePtr BuildColumn(TPosition pos, const TDeferredAtom& column, const TString& source = TString());
    TNodePtr BuildAccess(TPosition pos, const TVector<INode::TIdPart>& ids, bool isLookup);
    TNodePtr BuildBind(TPosition pos, const TString& module, const TString& alias);
    TNodePtr BuildLambda(TPosition pos, TNodePtr params, TNodePtr body, const TString& resName = TString());
    TNodePtr BuildCast(TContext& ctx, TPosition pos, TNodePtr expr, const TString& typeName, const TString& paramOne = TString(), const TString& paramTwo = TString());
    TNodePtr BuildBitCast(TContext& ctx, TPosition pos, TNodePtr expr, const TString& typeName, const TString& paramOne = TString(), const TString& paramTwo = TString());
    TNodePtr BuildIsNullOp(TPosition pos, TNodePtr a);
    TNodePtr BuildUnaryOp(TPosition pos, const TString& opName, TNodePtr a);
    TNodePtr BuildBinaryOp(TPosition pos, const TString& opName, TNodePtr a, TNodePtr b);

    TNodePtr BuildCalcOverWindow(TPosition pos, const TString& windowName, TNodePtr call);
    TNodePtr BuildYsonOptionsNode(TPosition pos, bool autoConvert, bool strict);

    TNodePtr BuildShortcutNode(const TNodePtr& node, const TString& baseName);
    TNodePtr BuildDoCall(TPosition pos, const TNodePtr& node);
    TNodePtr BuildTupleResult(TNodePtr tuple, int ensureTupleSize);

    // Implemented in aggregation.cpp
    TAggregationPtr BuildFactoryAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode, bool multi = false);
    TAggregationPtr BuildFactoryAggregationWinAutoarg(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode);
    TAggregationPtr BuildKeyPayloadFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);
    TAggregationPtr BuildPayloadPredicateFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);
    TAggregationPtr BuildTwoArgsFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);
    TAggregationPtr BuildHistogramFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);
    TAggregationPtr BuildLinearHistogramFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);
    template <bool HasKey>
    TAggregationPtr BuildTopFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);
    TAggregationPtr BuildTopFreqFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);
    TAggregationPtr BuildCountDistinctEstimateFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);
    TAggregationPtr BuildListFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);
    TAggregationPtr BuildPercentileFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);
    TAggregationPtr BuildCountAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode);
    TAggregationPtr BuildUserDefinedFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);


    typedef std::function<TNodePtr (const TString& baseName, const TNodePtr& node)> TFuncPrepareNameNode;
    // Implemented in builtin.cpp
    TNodePtr BuildCallable(TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args);
    TNodePtr BuildUdf(TContext& ctx, TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args);
    TNodePtr BuildBuiltinFunc(
        TContext& ctx,
        TPosition pos,
        TString name,
        const TVector<TNodePtr>& args,
        const TString& nameSpace = TString(),
        EAggregateMode aggMode = EAggregateMode::Normal,
        bool* mustUseNamed = nullptr,
        TFuncPrepareNameNode funcPrepareNameNode = {}
    );
    TNodePtr TryBuildDataType(TPosition pos, const TString& stringType);

    // Implemented in join.cpp
    TString NormalizeJoinOp(const TString& joinOp);
    TSourcePtr BuildEquiJoin(TPosition pos, TVector<TSourcePtr>&& sources);

    // Implemented in select.cpp
    TNodePtr BuildSubquery(TSourcePtr source, const TString& alias, bool inSubquery, int ensureTupleSize = -1);
    TNodePtr BuildSubqueryRef(TNodePtr subquery, const TString& alias, int tupleIndex = -1);
    TNodePtr BuildSourceNode(TPosition pos, TSourcePtr source, bool checkExist = false);
    TSourcePtr BuildMuxSource(TPosition pos, TVector<TSourcePtr>&& sources);
    TSourcePtr BuildFakeSource(TPosition pos);
    TSourcePtr BuildNodeSource(TPosition pos, const TNodePtr& node);
    TSourcePtr BuildTableSource(TPosition pos, const TTableRef& table, bool stream, const TString& label = TString());
    TSourcePtr BuildInnerSource(TPosition pos, TNodePtr node, const TString& label = TString());
    TSourcePtr BuildRefColumnSource(TPosition pos, const TString& partExpression);
    TSourcePtr BuildUnionAll(TPosition pos, TVector<TSourcePtr>&& sources);
    TSourcePtr BuildOverWindowSource(TPosition pos, const TString& windowName, ISource* origSource);

    TNodePtr BuildOrderBy(TPosition pos, const TVector<TNodePtr>& keys, const TVector<bool>& order);
    TNodePtr BuildSkipTake(TPosition pos, const TNodePtr& skip, const TNodePtr& take);


    TSourcePtr BuildSelectCore(
        TContext& ctx,
        TPosition pos,
        TSourcePtr source,
        const TVector<TNodePtr>& groupByExpr,
        const TVector<TNodePtr>& groupBy,
        const TVector<TSortSpecificationPtr>& orderBy,
        TNodePtr having,
        TWinSpecs&& windowSpec,
        THoppingWindowSpecPtr hoppingWindowSpec,
        TVector<TNodePtr>&& terms,
        bool distinct,
        TVector<TNodePtr>&& without,
        bool stream,
        const TWriteSettings& settings
    );
    TSourcePtr BuildSelect(TPosition pos, TSourcePtr source, TNodePtr skipTake);


    enum class ReduceMode {
        ByPartition,
        ByAll,
    };
    TSourcePtr BuildReduce(TPosition pos, ReduceMode mode, TSourcePtr source, TVector<TSortSpecificationPtr>&& orderBy,
        TVector<TNodePtr>&& keys, TVector<TNodePtr>&& args, TNodePtr udf, TNodePtr having, const TWriteSettings& settings);
    TSourcePtr BuildProcess(TPosition pos, TSourcePtr source, TNodePtr with, TVector<TNodePtr>&& terms, bool listCall, bool stream, const TWriteSettings& settings);

    TNodePtr BuildSelectResult(TPosition pos, TSourcePtr source, bool writeResult, bool inSubquery);

    // Implemented in insert.cpp
    TSourcePtr BuildWriteValues(TPosition pos, const TString& opertationHumanName, const TVector<TString>& columnsHint, const TVector<TVector<TNodePtr>>& values);
    TSourcePtr BuildWriteValues(TPosition pos, const TString& opertationHumanName, const TVector<TString>& columnsHint, const TVector<TNodePtr>& values);
    TSourcePtr BuildWriteValues(TPosition pos, const TString& opertationHumanName, const TVector<TString>& columnsHint, TSourcePtr source);
    TSourcePtr BuildUpdateValues(TPosition pos, const TVector<TString>& columnsHint, const TVector<TNodePtr>& values);

    EWriteColumnMode ToWriteColumnsMode(ESQLWriteColumnMode sqlWriteColumnMode);
    TNodePtr BuildEraseColumns(TPosition pos, const TVector<TString>& columns);
    TNodePtr BuildWriteColumns(TPosition pos, const TTableRef& table, EWriteColumnMode mode, TSourcePtr values, TNodePtr options = nullptr);
    TNodePtr BuildUpdateColumns(TPosition pos, const TTableRef& table, TSourcePtr values, TSourcePtr source);
    TNodePtr BuildDelete(TPosition pos, const TTableRef& table, TSourcePtr source);

    // Implemented in query.cpp
    TNodePtr BuildTableKey(TPosition pos, const TString& cluster, const TDeferredAtom& name, const TString& view);
    TNodePtr BuildTableKeys(TPosition pos, const TString& cluster, const TString& func, const TVector<TTableArg>& args);
    TNodePtr BuildInputOptions(TPosition pos, const TVector<TString>& hints);
    TNodePtr BuildInputTables(TPosition pos, const TTableList& tables, bool inSubquery);
    TNodePtr BuildCreateTable(TPosition pos, const TTableRef& table, const TVector<TColumnSchema>& columns,
        const TVector<TIdentifier>& pkColumns, const TVector<TIdentifier>& partitionByColumns,
        const TVector<std::pair<TIdentifier, bool>>& orderByColumns);
    TNodePtr BuildAlterTable(TPosition pos, const TTableRef& tr, const TVector<TColumnSchema>& columns, EAlterTableIntentnt mode);
    TNodePtr BuildDropTable(TPosition pos, const TTableRef& table);
    TNodePtr BuildWriteTable(TPosition pos, const TString& label, const TTableRef& table, EWriteColumnMode mode, TNodePtr options = nullptr);
    TNodePtr BuildWriteResult(TPosition pos, const TString& label, TNodePtr settings, const TSet<TString>& clusters);
    TNodePtr BuildCommitClusters(TPosition pos, const TSet<TString>& clusters = TSet<TString>());
    TNodePtr BuildRollbackClusters(TPosition pos, const TSet<TString>& clusters = TSet<TString>());
    TNodePtr BuildQuery(TPosition pos, const TVector<TNodePtr>& blocks, bool topLevel);
    TNodePtr BuildPragma(TPosition pos, const TString& prefix, const TString& name, const TVector<TDeferredAtom>& values, bool valueDefault);
    TNodePtr BuildSqlLambda(TPosition pos, TVector<TString>&& args, TVector<TNodePtr>&& exprSeq);
    TNodePtr BuildEvaluateIfNode(TPosition pos, TNodePtr predicate, TNodePtr thenNode, TNodePtr elseNode);
    TNodePtr BuildEvaluateForNode(TPosition pos, TNodePtr list, TNodePtr bodyNode, TNodePtr elseNode);

    template<class TContainer>
    TMaybe<TString> FindMistypeIn(const TContainer& container, const TString& name) {
        for (auto& item: container) {
            if (NLevenshtein::Distance(name, item) < NYql::DefaultMistypeDistance) {
                return item;
            }
        }
        return {};
    }

    bool Parseui32(TNodePtr from, ui32& to);
    TNodePtr GroundWithExpr(const TNodePtr& ground, const TNodePtr& expr);
    TSourcePtr TryMakeSourceFromExpression(TContext& ctx, TNodePtr node, const TString& view = {});
    void MakeTableFromExpression(TContext& ctx, TNodePtr node, TDeferredAtom& table);
    TDeferredAtom MakeAtomFromExpression(TContext& ctx, TNodePtr node);
    bool TryMakeClusterAndTableFromExpression(TNodePtr node, TString& cluster, TDeferredAtom& table, TContext& ctx);
    TString NormalizeTypeString(const TString& str);
}  // namespace NSQLTranslationV0
