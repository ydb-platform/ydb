#pragma once

#include <google/protobuf/message.h>
#include <ydb/library/yql/utils/resetable_setting.h>
#include <ydb/library/yql/parser/proto_ast/proto_ast.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
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

#include <array>
#include <functional>
#include <variant>

namespace NSQLTranslationV1 {
    constexpr const size_t SQL_MAX_INLINE_SCRIPT_LEN = 24;

    using NYql::TPosition;
    using NYql::TAstNode;

    enum class ENodeState {
        Begin,
        Precached = Begin,
        Initialized,
        CountHint,
        Const,
        MaybeConst,
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

    enum class ETableType {
        Table,
        TableStore,
        ExternalTable
    };

    class TContext;
    class ITableKeys;
    class ISource;
    class IAggregation;
    class TObjectOperatorContext;
    typedef TIntrusivePtr<IAggregation> TAggregationPtr;
    class TColumnNode;
    class TTupleNode;
    class TCallNode;
    class TStructNode;
    class TAccessNode;
    class TLambdaNode;
    class TUdfNode;
    typedef TIntrusivePtr<ISource> TSourcePtr;

    struct TScopedState;
    typedef TIntrusivePtr<TScopedState> TScopedStatePtr;

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

            TIdPart(const TString& name)
                : Name(name)
            {
            }
            TIdPart(TPtr expr)
                : Expr(expr)
            {
            }
            TIdPart Clone() const {
                TIdPart res(Name);
                res.Expr = Expr ? Expr->Clone() : nullptr;
                return res;
            }
        };

    public:
        INode(TPosition pos);
        virtual ~INode();

        TPosition GetPos() const;
        const TString& GetLabel() const;
        TMaybe<TPosition> GetLabelPos() const;
        void SetLabel(const TString& label, TMaybe<TPosition> pos = {});
        bool IsImplicitLabel() const;
        void MarkImplicitLabel(bool isImplicitLabel);

        void SetCountHint(bool isCount);
        bool GetCountHint() const;
        bool Init(TContext& ctx, ISource* src);
        virtual bool InitReference(TContext& ctx);

        bool IsConstant() const;
        bool MaybeConstant() const;
        bool IsAggregated() const;
        bool IsAggregationKey() const;
        bool IsOverWindow() const;
        bool HasState(ENodeState state) const {
            PrecacheState();
            return State.Test(state);
        }

        virtual bool IsNull() const;
        virtual bool IsLiteral() const;
        virtual TString GetLiteralType() const;
        virtual TString GetLiteralValue() const;
        virtual bool IsIntegerLiteral() const;
        virtual TPtr ApplyUnaryOp(TContext& ctx, TPosition pos, const TString& opName) const;
        virtual bool IsAsterisk() const;
        virtual const TString* SubqueryAlias() const;
        virtual TString GetOpName() const;
        virtual const TString* GetLiteral(const TString& type) const;
        virtual const TString* GetColumnName() const;
        virtual void AssumeColumn();
        virtual const TString* GetSourceName() const;
        virtual const TString* GetAtomContent() const;
        virtual bool IsOptionalArg() const;
        virtual size_t GetTupleSize() const;
        virtual TPtr GetTupleElement(size_t index) const;
        virtual ITableKeys* GetTableKeys();
        virtual ISource* GetSource();
        virtual TVector<INode::TPtr>* ContentListPtr();
        virtual TAstNode* Translate(TContext& ctx) const = 0;
        virtual TAggregationPtr GetAggregation() const;
        virtual void CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs);
        virtual TPtr WindowSpecFunc(const TPtr& type) const;
        virtual bool SetViewName(TContext& ctx, TPosition pos, const TString& view);
        virtual bool SetPrimaryView(TContext& ctx, TPosition pos);
        void UseAsInner();
        virtual bool UsedSubquery() const;
        virtual bool IsSelect() const;
        virtual bool HasSelectResult() const;
        virtual const TString* FuncName() const;
        virtual const TString* ModuleName() const;
        virtual bool HasSkip() const;

        virtual TColumnNode* GetColumnNode();
        virtual const TColumnNode* GetColumnNode() const;

        virtual TTupleNode* GetTupleNode();
        virtual const TTupleNode* GetTupleNode() const;

        virtual TCallNode* GetCallNode();
        virtual const TCallNode* GetCallNode() const;

        virtual TStructNode* GetStructNode();
        virtual const TStructNode* GetStructNode() const;

        virtual TAccessNode* GetAccessNode();
        virtual const TAccessNode* GetAccessNode() const;

        virtual TLambdaNode* GetLambdaNode();
        virtual const TLambdaNode* GetLambdaNode() const;

        virtual TUdfNode* GetUdfNode();
        virtual const TUdfNode* GetUdfNode() const;

        using TVisitFunc = std::function<bool (const INode&)>;
        using TVisitNodeSet = std::unordered_set<const INode*>;

        void VisitTree(const TVisitFunc& func) const;
        void VisitTree(const TVisitFunc& func, TVisitNodeSet& visited) const;

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

        virtual void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const;
    private:
        virtual bool DoInit(TContext& ctx, ISource* src);
        virtual void DoAdd(TPtr node);

    protected:
        TPosition Pos;
        TString Label;
        TMaybe<TPosition> LabelPos;
        bool ImplicitLabel = false;
        mutable TNodeState State;
        bool AsInner = false;
    };
    typedef INode::TPtr TNodePtr;

    class IProxyNode : public INode {
    public:
        IProxyNode(TPosition pos, const TNodePtr& parent)
            : INode(pos)
            , Inner(parent)
        {}

    protected:
        virtual bool IsNull() const override;
        virtual bool IsLiteral() const override;
        virtual TString GetLiteralType() const override;
        virtual TString GetLiteralValue() const override;
        virtual bool IsIntegerLiteral() const override;
        virtual TPtr ApplyUnaryOp(TContext& ctx, TPosition pos, const TString& opName) const override;
        virtual bool IsAsterisk() const override;
        virtual const TString* SubqueryAlias() const override;
        virtual TString GetOpName() const override;
        virtual const TString* GetLiteral(const TString &type) const override;
        virtual const TString* GetColumnName() const override;
        virtual void AssumeColumn() override;
        virtual const TString* GetSourceName() const override;
        virtual const TString* GetAtomContent() const override;
        virtual bool IsOptionalArg() const override;
        virtual size_t GetTupleSize() const override;
        virtual TPtr GetTupleElement(size_t index) const override;
        virtual ITableKeys* GetTableKeys() override;
        virtual ISource* GetSource() override;
        virtual TVector<INode::TPtr>* ContentListPtr() override;
        virtual TAggregationPtr GetAggregation() const override;
        virtual void CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) override;
        virtual TPtr WindowSpecFunc(const TPtr& type) const override;
        virtual bool SetViewName(TContext& ctx, TPosition pos, const TString& view) override;
        virtual bool SetPrimaryView(TContext& ctx, TPosition pos) override;
        virtual bool UsedSubquery() const override;
        virtual bool IsSelect() const override;
        virtual bool HasSelectResult() const override;
        virtual const TString* FuncName() const override;
        virtual const TString* ModuleName() const override;
        virtual bool HasSkip() const override;

        virtual TColumnNode* GetColumnNode() override;
        virtual const TColumnNode* GetColumnNode() const override;

        virtual TTupleNode* GetTupleNode() override;
        virtual const TTupleNode* GetTupleNode() const override;

        virtual TCallNode* GetCallNode() override;
        virtual const TCallNode* GetCallNode() const override;

        virtual TStructNode* GetStructNode() override;
        virtual const TStructNode* GetStructNode() const override;

        virtual TAccessNode* GetAccessNode() override;
        virtual const TAccessNode* GetAccessNode() const override;

        virtual TLambdaNode* GetLambdaNode() override;
        virtual const TLambdaNode* GetLambdaNode() const override;

        virtual TUdfNode* GetUdfNode() override;
        virtual const TUdfNode* GetUdfNode() const override;

    protected:
        virtual void DoUpdateState() const override;
        virtual void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const override;
        virtual bool InitReference(TContext& ctx) override;
        virtual bool DoInit(TContext& ctx, ISource* src) override;

    private:
        virtual void DoAdd(TPtr node) override;

    protected:
        const TNodePtr Inner;
    };

    using TTableHints = TMap<TString, TVector<TNodePtr>>;
    void MergeHints(TTableHints& base, const TTableHints& overrides);

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

    TTableHints CloneContainer(const TTableHints& hints);

    class TAstAtomNode: public INode {
    public:
        TAstAtomNode(TPosition pos, const TString& content, ui32 flags, bool isOptionalArg);

        ~TAstAtomNode() override;

        TAstNode* Translate(TContext& ctx) const override;
        const TString& GetContent() const {
            return Content;
        }

        const TString* GetAtomContent() const override;
        bool IsOptionalArg() const override;

    protected:
        TString Content;
        ui32 Flags;
        bool IsOptionalArg_;

        void DoUpdateState() const override;
    };

    class TAstAtomNodeImpl final: public TAstAtomNode {
    public:
        TAstAtomNodeImpl(TPosition pos, const TString& content, ui32 flags, bool isOptionalArg = false)
            : TAstAtomNode(pos, content, flags, isOptionalArg)
        {}

        TNodePtr DoClone() const final {
            return new TAstAtomNodeImpl(Pos, Content, Flags, IsOptionalArg_);
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
        void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const override;

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
        void CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) override;

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
        TCallNode* GetCallNode() override;
        const TCallNode* GetCallNode() const override;

    protected:
        bool DoInit(TContext& ctx, ISource* src) override;
        bool ValidateArguments(TContext& ctx) const;
        TString GetCallExplain() const;
        void CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) override;

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

    class TFuncNodeImpl final : public TCallNode {
        TPtr DoClone() const final;
    public:
        TFuncNodeImpl(TPosition pos, const TString& opName);
        const TString* FuncName() const override;
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
        TCallDirectRow(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);
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
    };

    using TFunctionConfig = TMap<TString, TNodePtr>;

    class TExternalFunctionConfig final: public TAstListNode {
    public:
        TExternalFunctionConfig(TPosition pos, const TFunctionConfig& config)
        : TAstListNode(pos)
        , Config(config)
        {
        }

        bool DoInit(TContext& ctx, ISource* src) override;
        TPtr DoClone() const final;

    private:
        TFunctionConfig Config;
    };

    class TWinRowNumber final: public TWinAggrEmulation {
        TPtr DoClone() const final {
            return CallNodeClone<TWinRowNumber>();
        }
    public:
        TWinRowNumber(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);
    };

    class TWinCumeDist final: public TWinAggrEmulation {
        TPtr DoClone() const final {
            return CallNodeClone<TWinCumeDist>();
        }

        bool DoInit(TContext& ctx, ISource* src) override;
    public:
        TWinCumeDist(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);
    };

    class TWinNTile final: public TWinAggrEmulation {
        TPtr DoClone() const final {
            return CallNodeClone<TWinNTile>();
        }
        bool DoInit(TContext& ctx, ISource* src) override;
    public:
        TWinNTile(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);

    private:
        TSourcePtr FakeSource;
    };

    class TWinLeadLag final: public TWinAggrEmulation {
        TPtr DoClone() const final {
            return CallNodeClone<TWinLeadLag>();
        }
        bool DoInit(TContext& ctx, ISource* src) override;
    public:
        TWinLeadLag(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);
    };

    class TWinRank final: public TWinAggrEmulation {
        TPtr DoClone() const final {
            return CallNodeClone<TWinRank>();
        }
        bool DoInit(TContext& ctx, ISource* src) override;
    public:
        TWinRank(TPosition pos, const TString& opName, i32 minArgs, i32 maxArgs, const TVector<TNodePtr>& args);
    };

    struct TViewDescription {
        TString ViewName = "";
        bool PrimaryFlag = false;

        bool empty() const { return *this == TViewDescription(); }
        bool operator == (const TViewDescription&) const = default;
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

    protected:
        TNodePtr AddView(TNodePtr key, const TViewDescription& view);

    private:
        /// all TableKeys no clonnable
        TPtr DoClone() const final {
            return {};
        }

        ITableKeys* GetTableKeys() override;
        TAstNode* Translate(TContext& ctx) const override;
    };

    enum class ESampleClause {
        TableSample, //from SQL standard, percantage rate (0..100)
        Sample //simplified (implied Bernulli mode), fraction (0..1)
    };

    enum class ESampleMode {
        Bernoulli,
        System
    };

    class TDeferredAtom {
    public:
        TDeferredAtom();
        TDeferredAtom(TPosition pos, const TString& str);
        TDeferredAtom(TNodePtr node, TContext& ctx);
        const TString* GetLiteral() const;
        bool GetLiteral(TString& value, TContext& ctx) const;
        TNodePtr Build() const;
        TString GetRepr() const;
        bool Empty() const;
        bool HasNode() const;

    private:
        TMaybe<TString> Explicit;
        TNodePtr Node; // atom or evaluation node
        TString Repr;
    };

    struct TTopicRef {
        TString RefName;
        TDeferredAtom Cluster;
        TNodePtr Consumers;
        TNodePtr Settings;
        TNodePtr Keys;

        TTopicRef() = default;
        TTopicRef(const TString& refName, const TDeferredAtom& cluster, TNodePtr keys);
        TTopicRef(const TTopicRef&) = default;
        TTopicRef& operator=(const TTopicRef&) = default;
    };

    struct TIdentifier {
        TPosition Pos;
        TString Name;

        TIdentifier(TPosition pos, const TString& name)
            : Pos(pos)
            , Name(name) {}
    };

    struct TColumnConstraints {
        TNodePtr DefaultExpr;
        bool Nullable = true;

        TColumnConstraints(TNodePtr defaultExpr, bool nullable);
    };

    struct TColumnSchema {
        enum class ETypeOfChange {
            Nothing,
            DropNotNullConstraint,
            SetNotNullConstraint, // todo flown4qqqq
            SetFamily
        };

        TPosition Pos;
        TString Name;
        TNodePtr Type;
        bool Nullable;
        TVector<TIdentifier> Families;
        bool Serial;
        TNodePtr DefaultExpr;
        const ETypeOfChange TypeOfChange;

        TColumnSchema(TPosition pos, const TString& name, const TNodePtr& type, bool nullable,
            TVector<TIdentifier> families, bool serial, TNodePtr defaultExpr, ETypeOfChange typeOfChange = ETypeOfChange::Nothing);
    };

    struct TColumns: public TSimpleRefCount<TColumns> {
        TSet<TString> Real;
        TSet<TString> Artificial;
        TVector<TString> List;
        TVector<bool> NamedColumns;
        bool All = false;
        bool QualifiedAll = false;
        bool HasUnreliable = false;
        bool HasUnnamed = false;

        bool Add(const TString* column, bool countHint, bool isArtificial = false, bool isReliable = true);
        TString AddUnnamed();
        void Merge(const TColumns& columns);
        void SetPrefix(const TString& prefix);
        void SetAll();
        bool IsColumnPossible(TContext& ctx, const TString& column) const;
    };

    class TSortSpecification: public TSimpleRefCount<TSortSpecification> {
    public:
        TSortSpecification(const TNodePtr& orderExpr, bool ascending);
        const TNodePtr OrderExpr;
        const bool Ascending;
        TIntrusivePtr<TSortSpecification> Clone() const;
        ~TSortSpecification() {}
    private:
        const TNodePtr CleanOrderExpr;
    };
    typedef TIntrusivePtr<TSortSpecification> TSortSpecificationPtr;

    enum EFrameType {
        FrameByRows,
        FrameByRange,
        FrameByGroups,
    };
    enum EFrameExclusions {
        FrameExclNone, // same as EXCLUDE NO OTHERS
        FrameExclCurRow,
        FrameExclGroup,
        FrameExclTies,
    };
    enum EFrameSettings {
        // keep order
        FrameUndefined,
        FramePreceding,
        FrameCurrentRow,
        FrameFollowing,
    };

    struct TFrameBound: public TSimpleRefCount<TFrameBound> {
        TPosition Pos;
        TNodePtr Bound;
        EFrameSettings Settings = FrameUndefined;

        TIntrusivePtr<TFrameBound> Clone() const;
        ~TFrameBound() {}
    };
    typedef TIntrusivePtr<TFrameBound> TFrameBoundPtr;


    struct TFrameSpecification: public TSimpleRefCount<TFrameSpecification> {
        EFrameType FrameType = FrameByRows;
        TFrameBoundPtr FrameBegin;
        TFrameBoundPtr FrameEnd;
        EFrameExclusions FrameExclusion = FrameExclNone;

        TIntrusivePtr<TFrameSpecification> Clone() const;
        ~TFrameSpecification() {}
    };
    typedef TIntrusivePtr<TFrameSpecification> TFrameSpecificationPtr;

    struct TLegacyHoppingWindowSpec: public TSimpleRefCount<TLegacyHoppingWindowSpec> {
        TNodePtr TimeExtractor;
        TNodePtr Hop;
        TNodePtr Interval;
        TNodePtr Delay;
        bool DataWatermarks;

        TIntrusivePtr<TLegacyHoppingWindowSpec> Clone() const;
        ~TLegacyHoppingWindowSpec() {}
    };
    typedef TIntrusivePtr<TLegacyHoppingWindowSpec> TLegacyHoppingWindowSpecPtr;

    struct TWindowSpecification: public TSimpleRefCount<TWindowSpecification> {
        TMaybe<TString> ExistingWindowName;
        TVector<TNodePtr> Partitions;
        bool IsCompact = false;
        TVector<TSortSpecificationPtr> OrderBy;
        TNodePtr Session;
        TFrameSpecificationPtr Frame;

        TIntrusivePtr<TWindowSpecification> Clone() const;
        ~TWindowSpecification() {}
    };
    typedef TIntrusivePtr<TWindowSpecification> TWindowSpecificationPtr;
    typedef TMap<TString, TWindowSpecificationPtr> TWinSpecs;

    TWinSpecs CloneContainer(const TWinSpecs& specs);

    void WarnIfAliasFromSelectIsUsedInGroupBy(TContext& ctx, const TVector<TNodePtr>& selectTerms, const TVector<TNodePtr>& groupByTerms,
        const TVector<TNodePtr>& groupByExprTerms);
    bool ValidateAllNodesForAggregation(TContext& ctx, const TVector<TNodePtr>& nodes);

    struct TWriteSettings {
        bool Discard = false;
        TDeferredAtom Label;
    };

    class TColumnNode final: public INode {
    public:
        TColumnNode(TPosition pos, const TString& column, const TString& source, bool maybeType);
        TColumnNode(TPosition pos, const TNodePtr& column, const TString& source);

        virtual ~TColumnNode();
        bool IsAsterisk() const override;
        virtual bool IsArtificial() const;
        const TString* GetColumnName() const override;
        const TString* GetSourceName() const override;
        TColumnNode* GetColumnNode() override;
        const TColumnNode* GetColumnNode() const override;
        TAstNode* Translate(TContext& ctx) const override;
        void ResetColumn(const TString& column, const TString& source);
        void ResetColumn(const TNodePtr& column, const TString& source);

        void SetUseSourceAsColumn();
        void SetUseSource();
        void ResetAsReliable();
        void SetAsNotReliable();
        bool IsReliable() const;
        bool IsUseSourceAsColumn() const;
        bool IsUseSource() const;
        bool CanBeType() const;

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
        bool MaybeType = false;
    };

    class TArgPlaceholderNode final: public INode
    {
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
        TTupleNode* GetTupleNode() override;
        const TTupleNode* GetTupleNode() const override;
        bool DoInit(TContext& ctx, ISource* src) override;
        size_t GetTupleSize() const override;
        TPtr GetTupleElement(size_t index) const override;
        TNodePtr DoClone() const final;
    private:
        void CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) override;
        const TString* GetSourceName() const override;

        const TVector<TNodePtr> Exprs;
    };

    class TStructNode: public TAstListNode {
    public:
        TStructNode(TPosition pos, const TVector<TNodePtr>& exprs, const TVector<TNodePtr>& labels, bool ordered);

        bool DoInit(TContext& ctx, ISource* src) override;
        TNodePtr DoClone() const final;
        const TVector<TNodePtr>& GetExprs() {
            return Exprs;
        }
        TStructNode* GetStructNode() override;
        const TStructNode* GetStructNode() const override;

    private:
        void CollectPreaggregateExprs(TContext& ctx, ISource& src, TVector<INode::TPtr>& exprs) override;
        const TString* GetSourceName() const override;

        const TVector<TNodePtr> Exprs;
        const TVector<TNodePtr> Labels;
        const bool Ordered;
    };


    class TUdfNode: public INode {
    public:
        TUdfNode(TPosition pos, const TVector<TNodePtr>& args);
        bool DoInit(TContext& ctx, ISource* src) override final;
        TNodePtr DoClone() const override final;
        TAstNode* Translate(TContext& ctx) const override;
        const TNodePtr GetExternalTypes() const;
        const TString& GetFunction() const;
        const TString& GetModule() const;
        TNodePtr GetRunConfig() const;
        const TDeferredAtom& GetTypeConfig() const;
        TUdfNode* GetUdfNode() override;
        const TUdfNode* GetUdfNode() const override;
    private:
        TVector<TNodePtr> Args;
        const TString* FunctionName;
        const TString* ModuleName;
        TNodePtr ExternalTypesTuple = nullptr;
        TNodePtr RunConfig;
        TDeferredAtom TypeConfig;
    };

    class IAggregation: public INode {
    public:
        bool IsDistinct() const;

        void DoUpdateState() const override;

        virtual const TString* GetGenericKey() const;

        virtual bool InitAggr(TContext& ctx, bool isFactory, ISource* src, TAstListNode& node, const TVector<TNodePtr>& exprs) = 0;

        virtual std::pair<TNodePtr, bool> AggregationTraits(const TNodePtr& type, bool overState, bool many, bool allowAggApply, TContext& ctx) const;

        virtual TNodePtr AggregationTraitsFactory() const = 0;

        virtual std::vector<ui32> GetFactoryColumnIndices() const;

        virtual void AddFactoryArguments(TNodePtr& apply) const;

        virtual TNodePtr WindowTraits(const TNodePtr& type, TContext& ctx) const;

        const TString& GetName() const;

        EAggregateMode GetAggregationMode() const;
        void MarkKeyColumnAsGenerated();

        virtual void Join(IAggregation* aggr);

    private:
        virtual TNodePtr GetApply(const TNodePtr& type, bool many, bool allowAggApply, TContext& ctx) const = 0;

    protected:
        IAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode mode);
        TAstNode* Translate(TContext& ctx) const override;
        TNodePtr WrapIfOverState(const TNodePtr& input, bool overState, bool many, TContext& ctx) const;
        virtual TNodePtr GetExtractor(bool many, TContext& ctx) const = 0;

        TString Name;
        TString Func;
        const EAggregateMode AggMode;
        TString DistinctKey;
        bool IsGeneratedKeyColumn = false;
    };

    enum class EExprSeat: int {
        Open = 0,
        FlattenByExpr,
        FlattenBy,
        GroupBy,
        DistinctAggr,
        WindowPartitionBy,
        Max
    };

    enum class EExprType: int {
        WithExpression,
        ColumnOnly,
    };

    enum class EOrderKind: int {
        None,
        Sort,
        Assume,
        Passthrough
    };

    class TListOfNamedNodes final: public INode {
    public:
        TListOfNamedNodes(TPosition pos, TVector<TNodePtr>&& exprs);

        TVector<TNodePtr>* ContentListPtr() override;
        TAstNode* Translate(TContext& ctx) const override;
        TPtr DoClone() const final;
        void DoVisitChildren(const TVisitFunc& func, TVisitNodeSet& visited) const final;
    private:
        TVector<TNodePtr> Exprs;
        TString Meaning;
    };

    class TLiteralNode: public TAstListNode {
    public:
        TLiteralNode(TPosition pos, bool isNull);
        TLiteralNode(TPosition pos, const TString& type, const TString& value);
        TLiteralNode(TPosition pos, const TString& value, ui32 nodeFlags);
        TLiteralNode(TPosition pos, const TString& value, ui32 nodeFlags, const TString& type);
        bool IsNull() const override;
        const TString* GetLiteral(const TString& type) const override;
        void DoUpdateState() const override;
        TPtr DoClone() const override;
        bool IsLiteral() const override;
        TString GetLiteralType() const override;
        TString GetLiteralValue() const override;
    protected:
        bool Null;
        bool Void;
        TString Type;
        TString Value;
    };

    class TAsteriskNode: public INode {
    public:
        TAsteriskNode(TPosition pos);
        bool IsAsterisk() const override;
        TPtr DoClone() const override;
        TAstNode* Translate(TContext& ctx) const override;
    };

    template<typename T>
    class TLiteralNumberNode: public TLiteralNode {
    public:
        TLiteralNumberNode(TPosition pos, const TString& type, const TString& value, bool implicitType = false);
        TPtr DoClone() const override final;
        bool DoInit(TContext& ctx, ISource* src) override;
        bool IsIntegerLiteral() const override;
        TPtr ApplyUnaryOp(TContext& ctx, TPosition pos, const TString& opName) const override;
    private:
        const bool ImplicitType;
    };

    struct TTableArg {
        bool HasAt = false;
        TNodePtr Expr;
        TDeferredAtom Id;
        TViewDescription View;
    };

    class TTableRows final : public INode {
    public:
        TTableRows(TPosition pos, const TVector<TNodePtr>& args);
        TTableRows(TPosition pos, ui32 argsCount);

        bool DoInit(TContext& ctx, ISource* src) override;

        void DoUpdateState() const override;

        TNodePtr DoClone() const final;
        TAstNode* Translate(TContext& ctx) const override;

    private:
        ui32 ArgsCount;
        TNodePtr Node;
    };

    struct TStringContent {
        TString Content;
        NYql::NUdf::EDataSlot Type = NYql::NUdf::EDataSlot::String;
        TMaybe<TString> PgType;
        ui32 Flags = NYql::TNodeFlags::Default;
    };

    TMaybe<TStringContent> StringContent(TContext& ctx, TPosition pos, const TString& input);
    TMaybe<TStringContent> StringContentOrIdContent(TContext& ctx, TPosition pos, const TString& input);

    struct TTtlSettings {
        enum class EUnit {
            Seconds /* "seconds" */,
            Milliseconds /* "milliseconds" */,
            Microseconds /* "microseconds" */,
            Nanoseconds /* "nanoseconds" */,
        };

        TIdentifier ColumnName;
        TNodePtr Expr;
        TMaybe<EUnit> ColumnUnit;

        TTtlSettings(const TIdentifier& columnName, const TNodePtr& expr, const TMaybe<EUnit>& columnUnit = {});
    };

    struct TTableSettings {
        TNodePtr CompactionPolicy;
        TMaybe<TIdentifier> AutoPartitioningBySize;
        TNodePtr PartitionSizeMb;
        TMaybe<TIdentifier> AutoPartitioningByLoad;
        TNodePtr MinPartitions;
        TNodePtr MaxPartitions;
        TNodePtr UniformPartitions;
        TVector<TVector<TNodePtr>> PartitionAtKeys;
        TMaybe<TIdentifier> KeyBloomFilter;
        TNodePtr ReadReplicasSettings;
        NYql::TResetableSetting<TTtlSettings, void> TtlSettings;
        NYql::TResetableSetting<TNodePtr, void> Tiering;
        TMaybe<TIdentifier> StoreType;
        TNodePtr PartitionByHashFunction;
        TMaybe<TIdentifier> StoreExternalBlobs;

        TNodePtr DataSourcePath;
        NYql::TResetableSetting<TNodePtr, void> Location;
        TVector<NYql::TResetableSetting<std::pair<TIdentifier, TNodePtr>, TIdentifier>> ExternalSourceParameters;

        bool IsSet() const {
            return CompactionPolicy || AutoPartitioningBySize || PartitionSizeMb || AutoPartitioningByLoad
                || MinPartitions || MaxPartitions || UniformPartitions || PartitionAtKeys || KeyBloomFilter
                || ReadReplicasSettings || TtlSettings || Tiering || StoreType || PartitionByHashFunction
                || StoreExternalBlobs || DataSourcePath || Location || ExternalSourceParameters;
        }
    };

    struct TFamilyEntry {
        TFamilyEntry(const TIdentifier& name)
            :Name(name)
        {}

        TIdentifier Name;
        TNodePtr Data;
        TNodePtr Compression;
    };

    struct TVectorIndexSettings {
        enum class EDistance {
              Cosine        /* "cosine" */
            , Manhattan     /* "manhattan" */
            , Euclidean     /* "euclidean" */
        };

        enum class ESimilarity {
              Cosine        /* "cosine" */
            , InnerProduct  /* "inner_product" */
        };

        enum class EVectorType {
              Float         /* "float" */
            , Uint8         /* "uint8" */
            , Int8          /* "int8" */
            , Bit           /* "bit" */
        };

        using TMetric = std::variant<std::monostate, EDistance, ESimilarity>;
        TMetric Metric;
        std::optional<EVectorType> VectorType;
        std::optional<ui32> VectorDimension;

        bool Validate(TContext& ctx) const;
    };

    struct TIndexDescription {
        enum class EType {
            GlobalSync,
            GlobalAsync,
            GlobalSyncUnique,
            GlobalVectorKmeansTree,
        };

        TIndexDescription(const TIdentifier& name, EType type = EType::GlobalSync)
            : Name(name)
            , Type(type)
        {}

        TIdentifier Name;
        EType Type;
        TVector<TIdentifier> IndexColumns;
        TVector<TIdentifier> DataColumns;
        TTableSettings TableSettings;

        using TIndexSettings = std::variant<std::monostate, TVectorIndexSettings>;
        TIndexSettings IndexSettings;
    };

    struct TChangefeedSettings {
        struct TLocalSinkSettings {
            // no special settings
        };

        TNodePtr Mode;
        TNodePtr Format;
        TNodePtr InitialScan;
        TNodePtr VirtualTimestamps;
        TNodePtr ResolvedTimestamps;
        TNodePtr RetentionPeriod;
        TNodePtr TopicPartitions;
        TNodePtr AwsRegion;
        std::optional<std::variant<TLocalSinkSettings>> SinkSettings;
    };

    struct TChangefeedDescription {
        TChangefeedDescription(const TIdentifier& name)
            : Name(name)
            , Disable(false)
        {}

        TIdentifier Name;
        TChangefeedSettings Settings;
        bool Disable;
    };

    struct TCreateTableParameters {
        TVector<TColumnSchema> Columns;
        TVector<TIdentifier> PkColumns;
        TVector<TIdentifier> PartitionByColumns;
        TVector<std::pair<TIdentifier, bool>> OrderByColumns;
        TVector<TIndexDescription> Indexes;
        TVector<TFamilyEntry> ColumnFamilies;
        TVector<TChangefeedDescription> Changefeeds;
        TTableSettings TableSettings;
        ETableType TableType = ETableType::Table;
        bool Temporary = false;
    };

    struct TTableRef;
    struct TAnalyzeParams {
        std::shared_ptr<TTableRef> Table;
        TVector<TString> Columns;
    };

    struct TAlterTableParameters {
        TVector<TColumnSchema> AddColumns;
        TVector<TString> DropColumns;
        TVector<TColumnSchema> AlterColumns;
        TVector<TFamilyEntry> AddColumnFamilies;
        TVector<TFamilyEntry> AlterColumnFamilies;
        TTableSettings TableSettings;
        TVector<TIndexDescription> AddIndexes;
        TVector<TIndexDescription> AlterIndexes;
        TVector<TIdentifier> DropIndexes;
        TMaybe<std::pair<TIdentifier, TIdentifier>> RenameIndexTo;
        TMaybe<TIdentifier> RenameTo;
        TVector<TChangefeedDescription> AddChangefeeds;
        TVector<TChangefeedDescription> AlterChangefeeds;
        TVector<TIdentifier> DropChangefeeds;
        ETableType TableType = ETableType::Table;

        bool IsEmpty() const {
            return AddColumns.empty() && DropColumns.empty() && AlterColumns.empty()
                && AddColumnFamilies.empty() && AlterColumnFamilies.empty()
                && !TableSettings.IsSet()
                && AddIndexes.empty() && AlterIndexes.empty() && DropIndexes.empty() && !RenameIndexTo.Defined()
                && !RenameTo.Defined()
                && AddChangefeeds.empty() && AlterChangefeeds.empty() && DropChangefeeds.empty();
        }
    };

    struct TRoleParameters {
        TMaybe<TDeferredAtom> Password;
        bool IsPasswordEncrypted = false;
        TVector<TDeferredAtom> Roles;
    };

    struct TTopicConsumerSettings {
        struct TLocalSinkSettings {
            // no special settings
        };

        TNodePtr Important;
        NYql::TResetableSetting<TNodePtr, void> ReadFromTs;
        NYql::TResetableSetting<TNodePtr, void> SupportedCodecs;
    };

    struct TTopicConsumerDescription {
        TTopicConsumerDescription(const TIdentifier& name)
            : Name(name)
        {}

        TIdentifier Name;
        TTopicConsumerSettings Settings;
    };
    struct TTopicSettings {
        NYql::TResetableSetting<TNodePtr, void> MinPartitions;
        NYql::TResetableSetting<TNodePtr, void> MaxPartitions;
        NYql::TResetableSetting<TNodePtr, void> RetentionPeriod;
        NYql::TResetableSetting<TNodePtr, void> RetentionStorage;
        NYql::TResetableSetting<TNodePtr, void> SupportedCodecs;
        NYql::TResetableSetting<TNodePtr, void> PartitionWriteSpeed;
        NYql::TResetableSetting<TNodePtr, void> PartitionWriteBurstSpeed;
        NYql::TResetableSetting<TNodePtr, void> MeteringMode;
        NYql::TResetableSetting<TNodePtr, void> AutoPartitioningStabilizationWindow;
        NYql::TResetableSetting<TNodePtr, void> AutoPartitioningUpUtilizationPercent;
        NYql::TResetableSetting<TNodePtr, void> AutoPartitioningDownUtilizationPercent;
        NYql::TResetableSetting<TNodePtr, void> AutoPartitioningStrategy;

        bool IsSet() const {
            return MinPartitions ||
                   MaxPartitions ||
                   RetentionPeriod ||
                   RetentionStorage ||
                   SupportedCodecs ||
                   PartitionWriteSpeed ||
                   PartitionWriteBurstSpeed ||
                   MeteringMode ||
                   AutoPartitioningStabilizationWindow ||
                   AutoPartitioningUpUtilizationPercent ||
                   AutoPartitioningDownUtilizationPercent ||
                   AutoPartitioningStrategy
            ;
        }
    };

    struct TCreateTopicParameters {
        TVector<TTopicConsumerDescription> Consumers;
        TTopicSettings TopicSettings;
        bool ExistingOk;
    };

    struct TAlterTopicParameters {
        TVector<TTopicConsumerDescription> AddConsumers;
        THashMap<TString, TTopicConsumerDescription> AlterConsumers;
        TVector<TIdentifier> DropConsumers;
        TTopicSettings TopicSettings;
        bool MissingOk;
    };

    struct TDropTopicParameters {
        bool MissingOk;
    };

    TString IdContent(TContext& ctx, const TString& str);
    TString IdContentFromString(TContext& ctx, const TString& str);
    TTableHints GetContextHints(TContext& ctx);

    TString TypeByAlias(const TString& alias, bool normalize = true);

    TNodePtr BuildAtom(TPosition pos, const TString& content, ui32 flags = NYql::TNodeFlags::ArbitraryContent,
        bool isOptionalArg = false);
    TNodePtr BuildQuotedAtom(TPosition pos, const TString& content, ui32 flags = NYql::TNodeFlags::ArbitraryContent);

    TNodePtr BuildLiteralNull(TPosition pos);
    TNodePtr BuildLiteralVoid(TPosition pos);
    /// String is checked as quotable, support escaping and multiline
    TNodePtr BuildLiteralSmartString(TContext& ctx, const TString& value);

    struct TExprOrIdent {
        TNodePtr Expr;
        TString Ident;
    };
    TMaybe<TExprOrIdent> BuildLiteralTypedSmartStringOrId(TContext& ctx, const TString& value);

    TNodePtr BuildLiteralRawString(TPosition pos, const TString& value, bool isUtf8 = false);
    TNodePtr BuildLiteralBool(TPosition pos, bool value);
    TNodePtr BuildEmptyAction(TPosition pos);

    TNodePtr BuildTuple(TPosition pos, const TVector<TNodePtr>& exprs);

    TNodePtr BuildStructure(TPosition pos, const TVector<TNodePtr>& exprs);
    TNodePtr BuildStructure(TPosition pos, const TVector<TNodePtr>& exprsUnlabeled, const TVector<TNodePtr>& labels);
    TNodePtr BuildOrderedStructure(TPosition pos, const TVector<TNodePtr>& exprsUnlabeled, const TVector<TNodePtr>& labels);

    TNodePtr BuildListOfNamedNodes(TPosition pos, TVector<TNodePtr>&& exprs);

    TNodePtr BuildArgPlaceholder(TPosition pos, const TString& name);

    TNodePtr BuildColumn(TPosition pos, const TString& column = TString(), const TString& source = TString());
    TNodePtr BuildColumn(TPosition pos, const TNodePtr& column, const TString& source = TString());
    TNodePtr BuildColumn(TPosition pos, const TDeferredAtom& column, const TString& source = TString());
    TNodePtr BuildColumnOrType(TPosition pos, const TString& column = TString());
    TNodePtr BuildAccess(TPosition pos, const TVector<INode::TIdPart>& ids, bool isLookup);
    TNodePtr BuildMatchRecognizeVarAccess(TPosition pos, const TString& var, const TString& column, bool theSameVar);
    TNodePtr BuildBind(TPosition pos, const TString& module, const TString& alias);
    TNodePtr BuildLambda(TPosition pos, TNodePtr params, TNodePtr body, const TString& resName = TString());
    TNodePtr BuildLambda(TPosition pos, TNodePtr params, const TVector<TNodePtr>& bodies);
    TNodePtr BuildDataType(TPosition pos, const TString& typeName);
    TMaybe<TString> LookupSimpleType(const TStringBuf& alias, bool flexibleTypes, bool isPgType);
    TNodePtr BuildSimpleType(TContext& ctx, TPosition pos, const TString& typeName, bool dataOnly);
    TNodePtr BuildIsNullOp(TPosition pos, TNodePtr a);
    TNodePtr BuildBinaryOp(TContext& ctx, TPosition pos, const TString& opName, TNodePtr a, TNodePtr b);
    TNodePtr BuildBinaryOpRaw(TPosition pos, const TString& opName, TNodePtr a, TNodePtr b);

    TNodePtr BuildCalcOverWindow(TPosition pos, const TString& windowName, TNodePtr call);
    TNodePtr BuildYsonOptionsNode(TPosition pos, bool autoConvert, bool strict, bool fastYson);

    TNodePtr BuildDoCall(TPosition pos, const TNodePtr& node);
    TNodePtr BuildTupleResult(TNodePtr tuple, size_t ensureTupleSize);
    TNodePtr BuildNamedExprReference(TNodePtr parent, const TString& name, TMaybe<size_t> tupleIndex);
    TNodePtr BuildNamedExpr(TNodePtr parent);

    // Implemented in aggregation.cpp
    TAggregationPtr BuildFactoryAggregation(TPosition pos, const TString& name, const TString& func, EAggregateMode aggMode, bool multi = false);
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
    TAggregationPtr BuildPGFactoryAggregation(TPosition pos, const TString& name, EAggregateMode aggMode);
    TAggregationPtr BuildNthFactoryAggregation(TPosition pos, const TString& name, const TString& factory, EAggregateMode aggMode);


    // Implemented in builtin.cpp
    TNodePtr BuildCallable(TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args, bool forReduce = false);
    TNodePtr BuildUdf(TContext& ctx, TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args);
    TNodePtr BuildBuiltinFunc(
        TContext& ctx,
        TPosition pos,
        TString name,
        const TVector<TNodePtr>& args,
        const TString& nameSpace = TString(),
        EAggregateMode aggMode = EAggregateMode::Normal,
        bool* mustUseNamed = nullptr,
        bool warnOnYqlNameSpace = true
    );

    // Implemented in query.cpp
    TNodePtr BuildCreateUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TMaybe<TRoleParameters>& params, TScopedStatePtr scoped);
    TNodePtr BuildCreateGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TMaybe<TRoleParameters>& params, TScopedStatePtr scoped);
    TNodePtr BuildAlterUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TRoleParameters& params, TScopedStatePtr scoped);
    TNodePtr BuildRenameUser(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TDeferredAtom& newName, TScopedStatePtr scoped);
    TNodePtr BuildAlterGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TVector<TDeferredAtom>& toChange, bool isDrop,
        TScopedStatePtr scoped);
    TNodePtr BuildRenameGroup(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TDeferredAtom& name, const TDeferredAtom& newName, TScopedStatePtr scoped);
    TNodePtr BuildDropRoles(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& toDrop, bool isUser, bool missingOk, TScopedStatePtr scoped);
    TNodePtr BuildGrantPermissions(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& permissions, const TVector<TDeferredAtom>& schemaPaths, const TVector<TDeferredAtom>& roleName, TScopedStatePtr scoped);
    TNodePtr BuildRevokePermissions(TPosition pos, const TString& service, const TDeferredAtom& cluster, const TVector<TDeferredAtom>& permissions, const TVector<TDeferredAtom>& schemaPaths, const TVector<TDeferredAtom>& roleName, TScopedStatePtr scoped);
    TNodePtr BuildUpsertObjectOperation(TPosition pos, const TString& objectId, const TString& typeId,
        std::map<TString, TDeferredAtom>&& features, const TObjectOperatorContext& context);
    TNodePtr BuildCreateObjectOperation(TPosition pos, const TString& objectId, const TString& typeId,
        bool existingOk, bool replaceIfExists, std::map<TString, TDeferredAtom>&& features, const TObjectOperatorContext& context);
    TNodePtr BuildAlterObjectOperation(TPosition pos, const TString& secretId, const TString& typeId,
        std::map<TString, TDeferredAtom>&& features, std::set<TString>&& featuresToReset, const TObjectOperatorContext& context);
    TNodePtr BuildDropObjectOperation(TPosition pos, const TString& secretId, const TString& typeId,
        bool missingOk, std::map<TString, TDeferredAtom>&& options, const TObjectOperatorContext& context);
    TNodePtr BuildCreateAsyncReplication(TPosition pos, const TString& id,
        std::vector<std::pair<TString, TString>>&& targets,
        std::map<TString, TNodePtr>&& settings,
        const TObjectOperatorContext& context);
    TNodePtr BuildAlterAsyncReplication(TPosition pos, const TString& id,
        std::map<TString, TNodePtr>&& settings,
        const TObjectOperatorContext& context);
    TNodePtr BuildDropAsyncReplication(TPosition pos, const TString& id, bool cascade, const TObjectOperatorContext& context);
    TNodePtr BuildWriteResult(TPosition pos, const TString& label, TNodePtr settings);
    TNodePtr BuildCommitClusters(TPosition pos);
    TNodePtr BuildRollbackClusters(TPosition pos);
    TNodePtr BuildQuery(TPosition pos, const TVector<TNodePtr>& blocks, bool topLevel, TScopedStatePtr scoped);
    TNodePtr BuildPragma(TPosition pos, const TString& prefix, const TString& name, const TVector<TDeferredAtom>& values, bool valueDefault);
    TNodePtr BuildSqlLambda(TPosition pos, TVector<TString>&& args, TVector<TNodePtr>&& exprSeq);
    TNodePtr BuildWorldIfNode(TPosition pos, TNodePtr predicate, TNodePtr thenNode, TNodePtr elseNode, bool isEvaluate);
    TNodePtr BuildWorldForNode(TPosition pos, TNodePtr list, TNodePtr bodyNode, TNodePtr elseNode, bool isEvaluate, bool isParallel);

    TNodePtr BuildCreateTopic(TPosition pos, const TTopicRef& tr, const TCreateTopicParameters& params,
                              TScopedStatePtr scoped);
    TNodePtr BuildAlterTopic(TPosition pos, const TTopicRef& tr, const TAlterTopicParameters& params,
                              TScopedStatePtr scoped);
    TNodePtr BuildDropTopic(TPosition pos, const TTopicRef& topic, const TDropTopicParameters& params,
                            TScopedStatePtr scoped);

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
    const TString* DeriveCommonSourceName(const TVector<TNodePtr> &nodes);
}  // namespace NSQLTranslationV1
