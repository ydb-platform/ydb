#pragma once

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <yql/essentials/core/yql_statistics.h>
#include <yql/essentials/core/yql_cost_function.h>

#include <unordered_map>
#include <memory>
#include <map>
#include <sstream>

#include <yql/essentials/core/cbo/cbo_interesting_orderings.h>

namespace NYql {

/**
 * OptimizerNodes are the internal representations of operators inside the
 * Cost-based optimizer. Currently we only support RelOptimizerNode - a node that
 * is an input relation to the equi-join, and JoinOptimizerNode - an inner join
 * that connects two sets of relations.
*/
enum EOptimizerNodeKind: ui32
{
    RelNodeType,
    JoinNodeType
};

/**
 * BaseOptimizerNode is a base class for the internal optimizer nodes
 * It records a pointer to statistics and records the current cost of the
 * operator tree, rooted at this node
*/
struct IBaseOptimizerNode {
    EOptimizerNodeKind Kind;
    TOptimizerStatistics Stats;

    IBaseOptimizerNode(EOptimizerNodeKind k) : Kind(k) {}
    IBaseOptimizerNode(EOptimizerNodeKind k, TOptimizerStatistics s) :
        Kind(k), Stats(std::move(s)) {}

    virtual TVector<TString> Labels()=0;
    virtual void Print(std::stringstream& stream, int ntabs=0)=0;
};

enum EJoinKind: ui32
{
    InnerJoin,
    LeftJoin,
    RightJoin,
    OuterJoin,
    LeftOnly /* == LeftAntiJoin */,
    RightOnly /* == RightAntiJoin */,
    LeftSemi,
    RightSemi,
    Cross,
    Exclusion
};

EJoinKind ConvertToJoinKind(const TString& joinString);
TString ConvertToJoinString(const EJoinKind kind);

struct TCardinalityHints {
    enum ECardOperation : ui32 {
        Add,
        Subtract,
        Multiply,
        Divide,
        Replace
    };

    struct TCardinalityHint {
        TVector<TString> JoinLabels;
        ECardOperation Operation;
        double Value;
        TString StringRepr;
        bool Applied = false;

        double ApplyHint(double originalValue) {
            Applied = true;

            switch (Operation) {
                case Add:
                    return originalValue + Value;
                case Subtract:
                    return originalValue - Value;
                case Multiply:
                    return originalValue * Value;
                case Divide:
                    return originalValue / Value;
                case Replace:
                    return Value;
            }
        }
    };

    TVector<TCardinalityHint> Hints;
    void PushBack(TVector<TString> labels, ECardOperation op, double value, TString stringRepr) {
        Hints.push_back({.JoinLabels = std::move(labels), .Operation = op, .Value = value, .StringRepr = std::move(stringRepr)});
    }
};

struct TJoinAlgoHints {
    struct TJoinAlgoHint {
        TVector<TString> JoinLabels;
        EJoinAlgoType Algo;
        TString StringRepr;
        bool Applied = false;
    };

    TVector<TJoinAlgoHint> Hints;

    void PushBack(TVector<TString> labels, EJoinAlgoType algo, TString stringRepr) {
        Hints.push_back({.JoinLabels = std::move(labels), .Algo = algo, .StringRepr = std::move(stringRepr)});
    }
};

struct TJoinOrderHints {
    struct ITreeNode {
        enum _ : ui32 {
            Relation,
            Join
        };

        virtual TVector<TString> Labels() = 0;
        bool IsRelation() { return Type == Relation; }
        bool IsJoin() { return Type == Join; }
        virtual ~ITreeNode() = default;

        ui32 Type;
    };

    struct TJoinNode: public ITreeNode {
        TJoinNode(std::shared_ptr<ITreeNode> lhs, std::shared_ptr<ITreeNode> rhs)
            : Lhs(std::move(lhs))
            , Rhs(std::move(rhs))
        {
            this->Type = ITreeNode::Join;
        }

        TVector<TString> Labels() override {
            auto labels = Lhs->Labels();
            auto rhsLabels = Rhs->Labels();
            labels.insert(labels.end(), std::make_move_iterator(rhsLabels.begin()), std::make_move_iterator(rhsLabels.end()));
            return labels;
        }

        std::shared_ptr<ITreeNode> Lhs;
        std::shared_ptr<ITreeNode> Rhs;
    };

    struct TRelationNode: public ITreeNode {
        TRelationNode(TString label)
            : Label(std::move(label))
        {
            this->Type = ITreeNode::Relation;
        }

        TVector<TString> Labels() override { return {Label}; }

        TString Label;
    };

    struct TJoinOrderHint {
        std::shared_ptr<ITreeNode> Tree;
        TString StringRepr;
        bool Applied = false;
    };

    TVector<TJoinOrderHint> Hints;

    void PushBack(std::shared_ptr<ITreeNode> hintTree, TString stringRepr) {
        Hints.push_back({.Tree = std::move(hintTree), .StringRepr = std::move(stringRepr)});
    }
};

struct TOptimizerHints {
    std::shared_ptr<TCardinalityHints> BytesHints = std::make_shared<TCardinalityHints>();
    std::shared_ptr<TCardinalityHints> CardinalityHints = std::make_shared<TCardinalityHints>();
    std::shared_ptr<TJoinAlgoHints> JoinAlgoHints = std::make_shared<TJoinAlgoHints>();
    std::shared_ptr<TJoinOrderHints> JoinOrderHints = std::make_shared<TJoinOrderHints>();

    TVector<TString> GetUnappliedString();

    /*
     *   The function accepts string with four type of expressions: array of (JoinAlgo | Rows | Bytes | JoinOrder):
     *   1) JoinAlgo(t1 t2 ... tn Map | Grace | Lookup) to change join algo for join, where these labels take part
     *   2) Rows(t1 t2 ... tn (*|/|+|-|#) Number) to change cardinality for join, where these labels take part or labels only
     *   3) Bytes(t1 t2 ... tn (*|/|+|-|#) Number) to change byte size for join, where these labels take part or labels only
     *   4) JoinOrder( (t1 t2) (t3 (t4 ...)) ) - fixate this join subtree in the general join tree
     */
    static TOptimizerHints Parse(const TString&);
};

/**
 * This is a temporary structure for KQP provider
 * We will soon be supporting multiple providers and we will need to design
 * some interfaces to pass provider-specific context to the optimizer
*/
struct IProviderContext {
    virtual ~IProviderContext() = default;

    virtual double ComputeJoinCost(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const double outputRows,
        const double outputByteSize,
        EJoinAlgoType joinAlgo
    ) const = 0;

    virtual TOptimizerStatistics ComputeJoinStats(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NDq::TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint = nullptr
    ) const = 0;

    virtual TOptimizerStatistics ComputeJoinStatsV1(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NDq::TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide
    ) const = 0;

    virtual TOptimizerStatistics ComputeJoinStatsV2(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NDq::TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide,
        TCardinalityHints::TCardinalityHint* maybeBytesHint
    ) const = 0;

    virtual bool IsJoinApplicable(const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TVector<NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NDq::TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind
    ) = 0;
};

/**
 * Default provider context with default cost and stats computation.
*/

struct TBaseProviderContext : public IProviderContext {
    TBaseProviderContext() {}

    double ComputeJoinCost(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const double outputRows,
        const double outputByteSize,
        EJoinAlgoType joinAlgo
    ) const override;

    bool IsJoinApplicable(
        const std::shared_ptr<IBaseOptimizerNode>& leftStats,
        const std::shared_ptr<IBaseOptimizerNode>& rightStats,
        const TVector<NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NDq::TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind
    ) override;

    TOptimizerStatistics ComputeJoinStats(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NDq::TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint = nullptr
    ) const override;

    TOptimizerStatistics ComputeJoinStatsV1(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NDq::TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide
    ) const override;

    TOptimizerStatistics ComputeJoinStatsV2(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NDq::TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide,
        TCardinalityHints::TCardinalityHint* maybeBytesHint
    ) const override;

    static const TBaseProviderContext& Instance();
};

/**
 * RelOptimizerNode adds a label to base class
 * This is the label assinged to the input by equi-Join
*/
struct TRelOptimizerNode : public IBaseOptimizerNode {
    TString Label;

    TRelOptimizerNode(TString label, TOptimizerStatistics stats) :
        IBaseOptimizerNode(RelNodeType, std::move(stats)), Label(label) { }

    virtual ~TRelOptimizerNode() {}

    virtual TVector<TString> Labels();
    virtual void Print(std::stringstream& stream, int ntabs=0);
};

/**
 * JoinOptimizerNode records the left and right arguments of the join
 * as well as the set of join conditions.
 * It also has methods to compute the statistics and cost of a join,
 * based on pre-computed costs and statistics of the children.
*/
struct TJoinOptimizerNode : public IBaseOptimizerNode {
    std::shared_ptr<IBaseOptimizerNode> LeftArg;
    std::shared_ptr<IBaseOptimizerNode> RightArg;
    TVector<NDq::TJoinColumn> LeftJoinKeys;
    TVector<NDq::TJoinColumn> RightJoinKeys;
    EJoinKind JoinType;
    EJoinAlgoType JoinAlgo;
    /////////////////// 'ANY' flag means leaving only one row from the join side.
    bool LeftAny;
    bool RightAny;
    ///////////////////
    bool IsReorderable;

    TJoinOptimizerNode(const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        TVector<NDq::TJoinColumn> leftKeys,
        TVector<NDq::TJoinColumn> rightKeys,
        const EJoinKind joinType,
        const EJoinAlgoType joinAlgo,
        bool leftAny,
        bool rightAny,
        bool nonReorderable = false
    );
    virtual ~TJoinOptimizerNode() {}
    virtual TVector<TString> Labels();
    virtual void Print(std::stringstream& stream, int ntabs=0);
};

struct IOptimizerNew {
    using TPtr = std::shared_ptr<IOptimizerNew>;
    IProviderContext& Pctx;

    IOptimizerNew(IProviderContext& ctx) : Pctx(ctx) {}
    virtual ~IOptimizerNew() = default;
    virtual std::shared_ptr<TJoinOptimizerNode> JoinSearch(
        const std::shared_ptr<TJoinOptimizerNode>& joinTree,
        const TOptimizerHints& hints = {}
    ) = 0;
};

struct TExprContext;

class IOptimizerFactory : private TNonCopyable {
public:
    using TPtr = std::shared_ptr<IOptimizerFactory>;
    using TLogger = std::function<void(const TString&)>;

    virtual ~IOptimizerFactory() = default;

    struct TNativeSettings {
        ui32 MaxDPhypDPTableSize = 100000;
    };
    virtual IOptimizerNew::TPtr MakeJoinCostBasedOptimizerNative(IProviderContext& pctx, TExprContext& ctx, const TNativeSettings& settings) const = 0;

    struct TPGSettings {
        TLogger Logger = [](const TString&){};
    };
    virtual IOptimizerNew::TPtr MakeJoinCostBasedOptimizerPG(IProviderContext& pctx, TExprContext& ctx, const TPGSettings& settings) const = 0;
};

} // namespace NYql
