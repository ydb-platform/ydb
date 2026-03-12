#pragma once

#include "cbo_interesting_orderings.h"

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <yql/essentials/core/statistics/yql_statistics.h>

#include <unordered_map>
#include <memory>
#include <map>
#include <sstream>
#include <utility>

/*
 * CBO optimizer interfaces — owned by ydb/core/kqp.
 */

namespace NYql {
    struct TExprContext;
} // namespace NYql

namespace NKikimr::NKqp {

/**
 * OptimizerNodes are the internal representations of operators inside the
 * Cost-based optimizer. Currently we only support RelOptimizerNode - a node that
 * is an input relation to the equi-join, and JoinOptimizerNode - an inner join
 * that connects two sets of relations.
 */
enum EOptimizerNodeKind: ui32 {
    RelNodeType,
    JoinNodeType
};

/**
 * BaseOptimizerNode is a base class for the internal optimizer nodes.
 * It records a pointer to statistics and the current cost of the operator tree.
 */
class IBaseOptimizerNode {
public:
    EOptimizerNodeKind Kind;
    NYql::TOptimizerStatistics Stats;

    explicit IBaseOptimizerNode(EOptimizerNodeKind k)
        : Kind(k)
    {
    }

    IBaseOptimizerNode(EOptimizerNodeKind k, NYql::TOptimizerStatistics s)
        : Kind(k)
        , Stats(std::move(s))
    {
    }

    virtual TVector<TString> Labels() = 0;
    virtual void Print(std::stringstream& stream, int ntabs = 0) = 0;
};

enum EJoinKind: ui32 {
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
    enum ECardOperation: ui32 {
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
    class ITreeNode {
    public:
        enum EKind: ui32 {
            Relation,
            Join
        };

        virtual TVector<TString> Labels() = 0;

        bool IsRelation() {
            return Type == Relation;
        }

        bool IsJoin() {
            return Type == Join;
        }

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
        explicit TRelationNode(TString label)
            : Label(std::move(label))
        {
            this->Type = ITreeNode::Relation;
        }

        TVector<TString> Labels() override {
            return {Label};
        }

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
     *   1) JoinAlgo(t1 t2 ... tn Map | Grace | Lookup) to change join algo for join
     *   2) Rows(t1 t2 ... tn (*|/|+|-|#) Number) to change cardinality for join
     *   3) Bytes(t1 t2 ... tn (*|/|+|-|#) Number) to change byte size for join
     *   4) JoinOrder( (t1 t2) (t3 (t4 ...)) ) - fixate this join subtree
     */
    static TOptimizerHints Parse(const TString&);
};

struct IProviderContext {
    virtual ~IProviderContext() = default;

    virtual double ComputeJoinCost(
        const NYql::TOptimizerStatistics& leftStats,
        const NYql::TOptimizerStatistics& rightStats,
        const double outputRows,
        const double outputByteSize,
        EJoinAlgoType joinAlgo) const = 0;

    virtual NYql::TOptimizerStatistics ComputeJoinStats(
        const NYql::TOptimizerStatistics& leftStats,
        const NYql::TOptimizerStatistics& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint = nullptr) const = 0;

    virtual NYql::TOptimizerStatistics ComputeJoinStatsV1(
        const NYql::TOptimizerStatistics& leftStats,
        const NYql::TOptimizerStatistics& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide) const = 0;

    virtual NYql::TOptimizerStatistics ComputeJoinStatsV2(
        const NYql::TOptimizerStatistics& leftStats,
        const NYql::TOptimizerStatistics& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide,
        TCardinalityHints::TCardinalityHint* maybeBytesHint) const = 0;

    virtual bool IsJoinApplicable(const std::shared_ptr<IBaseOptimizerNode>& left,
                                  const std::shared_ptr<IBaseOptimizerNode>& right,
                                  const TVector<TJoinColumn>& leftJoinKeys,
                                  const TVector<TJoinColumn>& rightJoinKeys,
                                  EJoinAlgoType joinAlgo,
                                  EJoinKind joinKind) = 0;
};

struct TBaseProviderContext: public IProviderContext {
    TBaseProviderContext() {
    }

    double ComputeJoinCost(
        const NYql::TOptimizerStatistics& leftStats,
        const NYql::TOptimizerStatistics& rightStats,
        const double outputRows,
        const double outputByteSize,
        EJoinAlgoType joinAlgo) const override;

    bool IsJoinApplicable(
        const std::shared_ptr<IBaseOptimizerNode>& leftStats,
        const std::shared_ptr<IBaseOptimizerNode>& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind) override;

    NYql::TOptimizerStatistics ComputeJoinStats(
        const NYql::TOptimizerStatistics& leftStats,
        const NYql::TOptimizerStatistics& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint = nullptr) const override;

    NYql::TOptimizerStatistics ComputeJoinStatsV1(
        const NYql::TOptimizerStatistics& leftStats,
        const NYql::TOptimizerStatistics& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide) const override;

    NYql::TOptimizerStatistics ComputeJoinStatsV2(
        const NYql::TOptimizerStatistics& leftStats,
        const NYql::TOptimizerStatistics& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide,
        TCardinalityHints::TCardinalityHint* maybeBytesHint) const override;

    static const TBaseProviderContext& Instance();
};

struct TRelOptimizerNode: public IBaseOptimizerNode {
    TString Label;

    TRelOptimizerNode(TString label, NYql::TOptimizerStatistics stats)
        : IBaseOptimizerNode(RelNodeType, std::move(stats))
        , Label(std::move(label))
    {
    }

    virtual ~TRelOptimizerNode() {
    }

    TVector<TString> Labels() override;
    void Print(std::stringstream& stream, int ntabs = 0) override;
};

struct TJoinOptimizerNode: public IBaseOptimizerNode {
    std::shared_ptr<IBaseOptimizerNode> LeftArg;
    std::shared_ptr<IBaseOptimizerNode> RightArg;
    TVector<TJoinColumn> LeftJoinKeys;
    TVector<TJoinColumn> RightJoinKeys;
    EJoinKind JoinType;
    EJoinAlgoType JoinAlgo;
    bool LeftAny;
    bool RightAny;
    bool IsReorderable;

    TJoinOptimizerNode(const std::shared_ptr<IBaseOptimizerNode>& left,
                       const std::shared_ptr<IBaseOptimizerNode>& right,
                       TVector<TJoinColumn> leftKeys,
                       TVector<TJoinColumn> rightKeys,
                       const EJoinKind joinType,
                       const EJoinAlgoType joinAlgo,
                       bool leftAny,
                       bool rightAny,
                       bool nonReorderable = false);
    virtual ~TJoinOptimizerNode() {
    }
    TVector<TString> Labels() override;
    void Print(std::stringstream& stream, int ntabs = 0) override;
};

class IOptimizerNew {
public:
    using TPtr = std::shared_ptr<IOptimizerNew>;
    IProviderContext& Pctx;

    explicit IOptimizerNew(IProviderContext& ctx)
        : Pctx(ctx)
    {
    }
    virtual ~IOptimizerNew() = default;
    virtual std::shared_ptr<TJoinOptimizerNode> JoinSearch(
        const std::shared_ptr<TJoinOptimizerNode>& joinTree,
        const TOptimizerHints& hints = {}) = 0;
};

struct TCBOSettings {
    ui32 MaxDPhypDPTableSize = 100000;
    ui32 ShuffleEliminationJoinNumCutoff = 14;
};

class IOptimizerFactory: private TNonCopyable {
public:
    using TPtr = std::shared_ptr<IOptimizerFactory>;
    using TLogger = std::function<void(const TString&)>;

    virtual ~IOptimizerFactory() = default;

    virtual IOptimizerNew::TPtr MakeJoinCostBasedOptimizerNative(IProviderContext& pctx, NYql::TExprContext& ctx, const TCBOSettings& settings) const = 0;

    struct TPGSettings {
        TLogger Logger = [](const TString&) {};
    };
    virtual IOptimizerNew::TPtr MakeJoinCostBasedOptimizerPG(IProviderContext& pctx, NYql::TExprContext& ctx, const TPGSettings& settings) const = 0;
};

} // namespace NKikimr::NKqp

// Backward-compatible aliases in the NYql namespace.
// Files that include this header and use NYql::T... types will continue to work.
namespace NYql {
using EOptimizerNodeKind = NKikimr::NKqp::EOptimizerNodeKind;
// Enum values of EOptimizerNodeKind (unscoped, need explicit import)
using NKikimr::NKqp::RelNodeType;
using NKikimr::NKqp::JoinNodeType;
using IBaseOptimizerNode = NKikimr::NKqp::IBaseOptimizerNode;
using EJoinKind          = NKikimr::NKqp::EJoinKind;
// Enum values of EJoinKind (unscoped, need explicit import)
using NKikimr::NKqp::InnerJoin;
using NKikimr::NKqp::LeftJoin;
using NKikimr::NKqp::RightJoin;
using NKikimr::NKqp::OuterJoin;
using NKikimr::NKqp::LeftOnly;
using NKikimr::NKqp::RightOnly;
using NKikimr::NKqp::LeftSemi;
using NKikimr::NKqp::RightSemi;
using NKikimr::NKqp::Cross;
using NKikimr::NKqp::Exclusion;
using TCardinalityHints  = NKikimr::NKqp::TCardinalityHints;
using TJoinAlgoHints     = NKikimr::NKqp::TJoinAlgoHints;
using TJoinOrderHints    = NKikimr::NKqp::TJoinOrderHints;
using TOptimizerHints    = NKikimr::NKqp::TOptimizerHints;
using IProviderContext   = NKikimr::NKqp::IProviderContext;
using TBaseProviderContext = NKikimr::NKqp::TBaseProviderContext;
using TRelOptimizerNode  = NKikimr::NKqp::TRelOptimizerNode;
using TJoinOptimizerNode = NKikimr::NKqp::TJoinOptimizerNode;
using IOptimizerNew      = NKikimr::NKqp::IOptimizerNew;
using TCBOSettings       = NKikimr::NKqp::TCBOSettings;
using IOptimizerFactory  = NKikimr::NKqp::IOptimizerFactory;

// Using declarations (not inline wrappers) so both names resolve to the same entity,
// avoiding ambiguity when both NYql::* and NKikimr::NKqp::* are in scope.
using NKikimr::NKqp::ConvertToJoinKind;
using NKikimr::NKqp::ConvertToJoinString;
} // namespace NYql
