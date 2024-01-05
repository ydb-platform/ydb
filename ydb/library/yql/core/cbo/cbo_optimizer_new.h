#pragma once

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <ydb/library/yql/core/yql_statistics.h>
#include <ydb/library/yql/core/yql_cost_function.h>

#include <unordered_map>
#include <memory>
#include <map>
#include <sstream>


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
    std::shared_ptr<TOptimizerStatistics> Stats;

    IBaseOptimizerNode(EOptimizerNodeKind k) : Kind(k) {}
    IBaseOptimizerNode(EOptimizerNodeKind k, std::shared_ptr<TOptimizerStatistics> s) : 
        Kind(k), Stats(s) {}

    virtual TVector<TString> Labels()=0;
    virtual void Print(std::stringstream& stream, int ntabs=0)=0;
};

/**
 * RelOptimizerNode adds a label to base class
 * This is the label assinged to the input by equi-Join
*/
struct TRelOptimizerNode : public IBaseOptimizerNode {
    TString Label;

    TRelOptimizerNode(TString label, std::shared_ptr<TOptimizerStatistics> stats) : 
        IBaseOptimizerNode(RelNodeType, stats), Label(label) { }
    virtual ~TRelOptimizerNode() {}

    virtual TVector<TString> Labels();
    virtual void Print(std::stringstream& stream, int ntabs=0);
};

enum EJoinKind: ui32
{
    InnerJoin,
    LeftJoin,
    RightJoin,
    OuterJoin,
    LeftOnly,
    RightOnly,
    LeftSemi,
    RightSemi,
    Cross,
    Exclusion
};

EJoinKind ConvertToJoinKind(const TString& joinString);
TString ConvertToJoinString(const EJoinKind kind);


/**
 * JoinOptimizerNode records the left and right arguments of the join
 * as well as the set of join conditions.
 * It also has methods to compute the statistics and cost of a join,
 * based on pre-computed costs and statistics of the children.
*/
struct TJoinOptimizerNode : public IBaseOptimizerNode {
    std::shared_ptr<IBaseOptimizerNode> LeftArg;
    std::shared_ptr<IBaseOptimizerNode> RightArg;
    std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>> JoinConditions;
    EJoinKind JoinType;
    bool IsReorderable;

    TJoinOptimizerNode(const std::shared_ptr<IBaseOptimizerNode>& left, const std::shared_ptr<IBaseOptimizerNode>& right, 
        const std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>>& joinConditions, const EJoinKind joinType, bool nonReorderable=false);
    virtual ~TJoinOptimizerNode() {}
    virtual TVector<TString> Labels();
    virtual void Print(std::stringstream& stream, int ntabs=0);
};

struct IOptimizerNew {
    virtual ~IOptimizerNew() = default;
    virtual std::shared_ptr<TJoinOptimizerNode> JoinSearch(const std::shared_ptr<TJoinOptimizerNode>& joinTree) = 0;
};

} // namespace NYql
