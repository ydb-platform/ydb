#pragma once

#include <util/generic/vector.h>

#include <unordered_map>
#include <map>

namespace NYql {

struct IOptimizer {
    using TVarId = std::tuple<int, int>;

    struct TVar {
        char Name = 0; // debug name: 'a', 'b', 'c', ...
    };

    struct TRel {
        double Rows = 0;
        double TotalCost = 0;

        std::vector<TVar> TargetVars;
    };

    struct TEq {
        std::vector<TVarId> Vars;
    };

    struct TInput {
        std::vector<TRel> Rels;
        std::vector<TEq> EqClasses;

        TString ToString();
    };

    enum class EJoinType {
        Unknown,
        Inner
    };

    enum class EJoinStrategy {
        Unknown,
        Hash,
        Loop
    };

    struct TJoinNode {
        EJoinType Mode = EJoinType::Unknown;
        EJoinStrategy Strategy = EJoinStrategy::Unknown;
        // only a = b supported yet
        TVarId LeftVar = {};
        TVarId RightVar = {};
        std::vector<int> Rels = {};
        int Outer = -1; // index in Nodes
        int Inner = -1; // index in Nodes
    };

    struct TOutput {
        std::vector<TJoinNode> Nodes;
        TInput* Input = nullptr;

        TString ToString() const;
    };

    virtual ~IOptimizer() = default;
    virtual TOutput JoinSearch() = 0;
};

} // namespace NYql
