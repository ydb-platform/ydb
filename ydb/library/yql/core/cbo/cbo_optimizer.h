#pragma once

#include <util/generic/vector.h>

#include <unordered_map>
#include <map>

namespace NYql {

struct IOptimizer {
    using TVarId = std::tuple<int, int>;

    struct TVar {
        char name = 0; // debug name: 'a', 'b', 'c', ...
    };

    struct TRel {
        double rows = 0;
        double total_cost = 0;

        std::vector<TVar> TargetVars;
    };

    struct TEq {
        std::vector<TVarId> Vars;
    };

    struct TInput {
        std::vector<TRel> Rels;
        std::vector<TEq> EqClasses;
    };

    enum class EJoinType {
        UNKNOWN,
        INNER
    };

    enum class EJoinStrategy {
        UNKNOWN,
        HASH,
        LOOP
    };

    struct TJoinNode {
        EJoinType Mode = EJoinType::UNKNOWN;
        EJoinStrategy Strategy = EJoinStrategy::UNKNOWN;
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
