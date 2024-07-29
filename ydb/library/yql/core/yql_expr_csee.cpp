#include "yql_expr_csee.h"
#include "yql_expr_type_annotation.h"
#include "yql_expr_optimize.h"
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/hash_set.h>
#include <util/system/env.h>
#include <tuple>

namespace NYql {

namespace {
    static constexpr bool UseDeterminsticHash = false;

    struct TLambdaFrame {
        TLambdaFrame(const TExprNode* lambda, const TLambdaFrame* prev)
            : Lambda(lambda)
            , Prev(prev)
        {}

        TLambdaFrame() = default;

        const TExprNode* Lambda = nullptr;
        const TLambdaFrame* Prev = nullptr;
    };

    bool IsArgInScope(const TLambdaFrame& frame, const TExprNode& arg) {
        for (auto curr = &frame; curr; curr = curr->Prev) {
            if (const auto lambda = curr->Lambda) {
                YQL_ENSURE(lambda->IsLambda());
                for (ui32 i = 0U; i < lambda->Head().ChildrenSize(); ++i) {
                    if (lambda->Head().Child(i) == &arg) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    ui16 GetDependencyLevel(const TExprNode& node) {
        if (const auto lambda = node.GetDependencyScope()->first) {
            return 1 + GetDependencyLevel(*lambda);
        }
        return 0;
    }

    enum class EDependencyScope : ui8 {
        None = 0,
        Inner = 1,
        Outer = 2,
        Mixed = Inner | Outer
    };

    EDependencyScope CheckDependencyScope(const TLambdaFrame& frame, const TExprNode& node) {
        if (!node.IsAtom()) {
            if (const auto scope = node.GetDependencyScope()) {
                const auto outerLambda = scope->first;
                const auto innerLambda = scope->second;
                if (bool innerFound = false; innerLambda || outerLambda) {
                    for (auto curr = &frame; curr; curr = curr->Prev) {
                        if (!innerFound && innerLambda) {
                            if (curr->Lambda == innerLambda) {
                                innerFound = true;
                            } else {
                                continue;
                            }
                        }
                        if (curr->Lambda == outerLambda) {
                            return curr->Lambda == &node ? EDependencyScope::None : EDependencyScope::Inner;
                        }
                    }
                    return innerFound ? EDependencyScope::Mixed : EDependencyScope::Outer;
                }
            }
        }
        return EDependencyScope::None;
    }

    ui64 CalculateHash(ui16 depth, TExprNode& node, const TLambdaFrame& currFrame, const TColumnOrderStorage& coStore) {
        const auto dependency = CheckDependencyScope(currFrame, node);
        switch (dependency) {
            case EDependencyScope::None:
                if (const auto hash = node.GetHash()) {
                    return hash;
                }
                break;
            case EDependencyScope::Inner:
                if (const auto hash = node.GetHashAbove()) {
                    return hash;
                }
                break;
            case EDependencyScope::Outer:
                if (const auto hash = node.GetHashBelow()) {
                    return hash;
                }
                break;
            case EDependencyScope::Mixed:
                break;
        }

        ui64 hash = node.GetTypeAnn()->GetHash();
        hash = CseeHash(ui32(node.Type()), hash);
        for (auto c: node.GetAllConstraints()) {
            hash = CseeHash(c->GetHash(), hash);
        }
        hash = AddColumnOrderHash(coStore.Lookup(node.UniqueId()), hash);

        switch (node.Type()) {
        case TExprNode::Atom: {
            if constexpr (UseDeterminsticHash) {
                hash = CseeHash(node.Content().data(), node.Content().size(), hash);
            } else {
                // can hash ptr due to intern
                const char* ptr = node.Content().data();
                hash = CseeHash(&ptr, sizeof(ptr), hash);
            }

            hash = CseeHash(node.GetFlagsToCompare(), hash);
            break;
        }
        case TExprNode::Callable:
            if constexpr (UseDeterminsticHash) {
                hash = CseeHash(node.Content().data(), node.Content().size(), hash);
            } else {
                // can hash ptr due to intern
                const char* ptr = node.Content().data();
                hash = CseeHash(&ptr, sizeof(ptr), hash);
            }
            [[fallthrough]];
        case TExprNode::List: {
            const auto size = node.ChildrenSize();
            hash = CseeHash(size, hash);

            if (node.UnorderedChildren()) {
                TSmallVec<ui64> hashes;
                hashes.reserve(size);
                for (ui32 i = 0U; i < node.ChildrenSize(); ++i) {
                    hashes.emplace_back(CalculateHash(depth, *node.Child(i), currFrame, coStore));
                };
                std::sort(hashes.begin(), hashes.end());
                hash = std::accumulate(hashes.cbegin(), hashes.cend(), ~hash, [] (ui64 hash, ui64 childHash) {
                    return CseeHash(childHash, hash);
                });
            } else {
                for (ui32 i = 0U; i < node.ChildrenSize(); ++i) {
                    const auto childHash = CalculateHash(depth, *node.Child(i), currFrame, coStore);
                    hash = CseeHash(childHash, hash);
                }
            }
            break;
        }
        case TExprNode::Lambda: {
            if (const ui32 size = node.ChildrenSize())
                hash = CseeHash(size, hash);

            const auto& args = node.Head();
            hash = CseeHash(args.ChildrenSize(), hash);

            for (ui32 i = 0; i < args.ChildrenSize(); ++i) {
                const auto& arg = *args.Child(i);
                hash = CseeHash(arg.GetTypeAnn()->GetHash(), hash);
                for (auto c: arg.GetAllConstraints()) {
                    hash = CseeHash(c->GetHash(), hash);
                }
            }

            TLambdaFrame newFrame(&node, &currFrame);
            for (ui32 i = 1U; i < node.ChildrenSize(); ++i) {
                const auto lambdaHash = CalculateHash(depth + 1, *node.Child(i), newFrame, coStore);
                hash = CseeHash(lambdaHash, hash);
            }
            break;
        }
        case TExprNode::Argument:
            switch (dependency) {
                case EDependencyScope::Inner: {
                    hash = CseeHash(GetDependencyLevel(node), hash);
                    hash = CseeHash(node.GetArgIndex(), hash);
                    break;
                }
                case EDependencyScope::Outer: {
                    if constexpr (UseDeterminsticHash) {
                        hash = CseeHash(node.UniqueId(), hash);
                    } else {
                        const auto ptr = &node;
                        hash = CseeHash(&ptr, sizeof(ptr), hash);
                    }
                    break;
                }
                case EDependencyScope::None:
                case EDependencyScope::Mixed:
                    Y_ABORT("Strange argument.");
            }
            break;
        case TExprNode::World:
            break;
        default:
            YQL_ENSURE(false, "Unexpected");
        }

        if (hash == 0) {
            hash = 1;
        }

        switch (dependency) {
            case EDependencyScope::None:
                node.SetHash(hash);
                break;
            case EDependencyScope::Inner:
                node.SetHashAbove(hash);
                break;
            case EDependencyScope::Outer:
                node.SetHashBelow(hash);
                break;
            case EDependencyScope::Mixed:
                break;
        }
        return hash;
    }

    using TEqualResults = THashMap<std::pair<const TExprNode*, const TExprNode*>, bool>;
    bool DoEqualNodes(const TExprNode& left, TLambdaFrame& currLeftFrame, const TExprNode& right, TLambdaFrame& currRightFrame,
        TEqualResults& visited, const TColumnOrderStorage& coStore);
    bool EqualNodes(const TExprNode& left, TLambdaFrame& currLeftFrame, const TExprNode& right, TLambdaFrame& currRightFrame,
        TEqualResults& visited, const TColumnOrderStorage& coStore)
    {
        if (&left == &right) {
            return true;
        }

        auto key = std::make_pair(&left, &right);
        if (auto it = visited.find(key); it != visited.end()) {
            return it->second;
        }

        bool res = DoEqualNodes(left, currLeftFrame, right, currRightFrame, visited, coStore);
        visited[key] = res;
        return res;
    }

    bool DoEqualNodes(const TExprNode& left, TLambdaFrame& currLeftFrame, const TExprNode& right, TLambdaFrame& currRightFrame,
        TEqualResults& visited, const TColumnOrderStorage& coStore)
    {
        if (left.Type() != right.Type()) {
            return false;
        }

        if (left.GetTypeAnn() != right.GetTypeAnn()) {
            return false;
        }

        if (left.GetAllConstraints() != right.GetAllConstraints()) {
            return false;
        }
        auto l = coStore.Lookup(left.UniqueId());
        auto r = coStore.Lookup(right.UniqueId());
        if (l && r && *l != *r) {
            return false;
        }

        switch (left.Type()) {
        case TExprNode::Atom:
            // compare pointers due to intern
            return left.Content().data() == right.Content().data() && left.GetFlagsToCompare() == right.GetFlagsToCompare();

        case TExprNode::Callable:
            // compare pointers due to intern
            if (left.Content().data() != right.Content().data()) {
                return false;
            }
            [[fallthrough]];
        case TExprNode::List:
            if (left.ChildrenSize() != right.ChildrenSize()) {
                return false;
            }

            if (left.UnorderedChildren() && right.UnorderedChildren()) {
                if (2U == left.ChildrenSize()) {
                    return EqualNodes(left.Head(), currLeftFrame, right.Head(), currRightFrame, visited, coStore)
                        && EqualNodes(left.Tail(), currLeftFrame, right.Tail(), currRightFrame, visited, coStore)
                        || EqualNodes(left.Head(), currLeftFrame, right.Tail(), currRightFrame, visited, coStore)
                        && EqualNodes(left.Tail(), currLeftFrame, right.Head(), currRightFrame, visited, coStore);
                } else {
                    TSmallVec<const TExprNode*> lNodes, rNodes;
                    lNodes.reserve(left.ChildrenSize());
                    rNodes.reserve(right.ChildrenSize());

                    left.ForEachChild([&lNodes](const TExprNode& child){ return lNodes.emplace_back(&child); });
                    right.ForEachChild([&rNodes](const TExprNode& child){ return rNodes.emplace_back(&child); });

                    const auto order = [](const TExprNode* l, const TExprNode* r) { return l->GetHashAbove() < r->GetHashAbove(); };
                    std::sort(lNodes.begin(), lNodes.end(), order);
                    std::sort(rNodes.begin(), rNodes.end(), order);

                    for (ui32 i = 0; i < lNodes.size(); ++i) {
                        if (!EqualNodes(*lNodes[i], currLeftFrame, *rNodes[i], currRightFrame, visited, coStore)) {
                            return false;
                        }
                    }
                }
            } else {
                for (ui32 i = 0; i < left.ChildrenSize(); ++i) {
                    if (!EqualNodes(*left.Child(i), currLeftFrame, *right.Child(i), currRightFrame, visited, coStore)) {
                        return false;
                    }
                }
            }

            return true;
        case TExprNode::Lambda: {
            if (left.IsComplete() != right.IsComplete()) {
                return false;
            }

            if (left.ChildrenSize() != right.ChildrenSize()) {
                return false;
            }

            const auto& leftArgs = left.Head();
            const auto& rightArgs = right.Head();
            if (leftArgs.ChildrenSize() != rightArgs.ChildrenSize()) {
                return false;
            }

            for (ui32 i = 0; i < leftArgs.ChildrenSize(); ++i) {
                const auto& leftArg = *leftArgs.Child(i);
                const auto& rightArg = *rightArgs.Child(i);
                if (leftArg.GetTypeAnn() != rightArg.GetTypeAnn()) {
                    return false;
                }
                if (leftArg.GetAllConstraints() != rightArg.GetAllConstraints()) {
                    return false;
                }
            }

            TLambdaFrame newLeftFrame(&left, &currLeftFrame);
            TLambdaFrame newRightFrame(&right, &currRightFrame);

            for (ui32 i = 1U; i < left.ChildrenSize(); ++i) {
                if (!EqualNodes(*left.Child(i), newLeftFrame, *right.Child(i), newRightFrame, visited, coStore))
                    return false;
            }
            return true;
        }
        case TExprNode::Argument: {
            if (currLeftFrame.Lambda && currRightFrame.Lambda && IsArgInScope(currLeftFrame, left) && IsArgInScope(currRightFrame, right)) {
                const ui16 leftRelativeLevel = GetDependencyLevel(left);
                const ui16 rightRelativeLevel = GetDependencyLevel(right);
                return leftRelativeLevel == rightRelativeLevel && left.GetArgIndex() == right.GetArgIndex();
            } else {
                return &left == &right;
            }
        }
        case TExprNode::Arguments:
            break;
        case TExprNode::World:
            return true;
        }

        YQL_ENSURE(false, "Unexpected");
        return false;
    }

    using TCompareResults = THashMap<std::pair<const TExprNode*, const TExprNode*>, int>;
    int DoCompareNodes(const TExprNode& left, const TExprNode& right, TCompareResults& visited);
    int CompareNodes(const TExprNode& left, const TExprNode& right, TCompareResults& visited) {
        if (&left == &right) {
            return 0;
        }

        auto key = std::make_pair(&left, &right);
        if (auto it = visited.find(key); it != visited.end()) {
            return it->second;
        }

        int res = DoCompareNodes(left, right, visited);
        visited[key] = res;
        return res;
    }

    int DoCompareNodes(const TExprNode& left, const TExprNode& right, TCompareResults& visited) {
        if (left.Type() != right.Type()) {
            return (int)left.Type() - (int)right.Type();
        }

        switch (left.Type()) {
        case TExprNode::Atom:
            if (left.Content().size() != right.Content().size()) {
                return (int)left.Content().size() - (int)right.Content().size();
            }

            // compare pointers due to intern
            if (left.Content().data() != right.Content().data()) {
                if (const auto res = left.Content().compare(right.Content())) {
                    return res;
                }
            }

            return (int)left.GetFlagsToCompare() - (int)right.GetFlagsToCompare();
        case TExprNode::Callable:
            if (left.Content().size() != right.Content().size()) {
                return (int)left.Content().size() - (int)right.Content().size();
            }

            // compare pointers due to intern
            if (left.Content().data() != right.Content().data()) {
                if (const auto res = left.Content().compare(right.Content())) {
                    return res;
                }
            }

            [[fallthrough]];
        case TExprNode::List:
            if (left.ChildrenSize() != right.ChildrenSize()) {
                return (int)left.ChildrenSize() - (int)right.ChildrenSize();
            }

            for (ui32 i = 0; i < left.ChildrenSize(); ++i) {
                if (const auto res = CompareNodes(*left.Child(i), *right.Child(i), visited)) {
                    return res;
                }
            }

            return 0;
        case TExprNode::Lambda: {
            if (left.ChildrenSize() != right.ChildrenSize()) {
                return (int)left.ChildrenSize() - (int)right.ChildrenSize();
            }

            const auto& leftArgs = left.Head();
            const auto& rightArgs = right.Head();
            if (leftArgs.ChildrenSize() != rightArgs.ChildrenSize()) {
                return (int)leftArgs.ChildrenSize() - (int)rightArgs.ChildrenSize();
            }

            for (ui32 i = 1U; i < left.ChildrenSize(); ++i) {
                if (const auto c = CompareNodes(*left.Child(i), *right.Child(i), visited))
                    return c;
            }
            return 0;
        }
        case TExprNode::Argument:
            if (left.GetArgIndex() != right.GetArgIndex()) {
                return (int)left.GetArgIndex() - (int)right.GetArgIndex();
            }

            return (int)left.GetDependencyScope()->first->GetLambdaLevel() - (int)right.GetDependencyScope()->first->GetLambdaLevel();
        case TExprNode::Arguments:
            break;
        case TExprNode::World:
            return 0;
        }

        YQL_ENSURE(false, "Unexpected");
        return 0;
    }

    void CalculateCompletness(TExprNode& node, bool insideDependsOn, ui16 level, TNodeSet& closures,
        TNodeMap<TNodeSet>& visited, TNodeMap<TNodeSet>& visitedInsideDependsOn) {
        switch (node.Type()) {
            case TExprNode::Atom:
                node.SetDependencyScope(nullptr, nullptr);
                return;
            case TExprNode::Argument:
                closures.emplace(node.GetDependencyScope()->first);
                if (insideDependsOn) {
                    node.SetUsedInDependsOn();
                }
                return;
            default: break;
        }

        const auto ins = (insideDependsOn ? visitedInsideDependsOn : visited).emplace(&node, TNodeSet{});
        if (!ins.second) {
            closures.insert(ins.first->second.cbegin(), ins.first->second.cend());
            return;
        }

        auto& internal = ins.first->second;

        if (TExprNode::Lambda == node.Type()) {
            node.SetLambdaLevel(level);
            node.Head().ForEachChild(std::bind(&TExprNode::SetDependencyScope, std::placeholders::_1, &node, &node));
            for (ui32 i = 1U; i < node.ChildrenSize(); ++i) {
                CalculateCompletness(*node.Child(i), insideDependsOn, level + 1, internal, visited, visitedInsideDependsOn);
            }
            internal.erase(&node);
        } else {
            insideDependsOn = insideDependsOn || node.IsCallable("DependsOn");
            node.ForEachChild(std::bind(&CalculateCompletness, std::placeholders::_1, insideDependsOn, level, std::ref(internal),
                std::ref(visited), std::ref(visitedInsideDependsOn)));
        }

        const TExprNode* outerLambda = nullptr;
        const TExprNode* innerLambda = nullptr;
        for (const auto lambda : internal) {
            if (!outerLambda || lambda->GetLambdaLevel() < outerLambda->GetLambdaLevel()) {
                outerLambda = lambda;
            }
            if (!innerLambda || lambda->GetLambdaLevel() > innerLambda->GetLambdaLevel()) {
                innerLambda = lambda;
            }
        }

        node.SetDependencyScope(outerLambda, innerLambda);
        closures.insert(internal.cbegin(), internal.cend());
    }

    ui64 CalcHash(TExprNode& node, const TColumnOrderStorage& coStore) {
        TLambdaFrame frame;
        return CalculateHash(0, node, frame, coStore);
    }

    bool EqualNodes(const TExprNode& left, const TExprNode& right, const TColumnOrderStorage& coStore) {
        TEqualResults visited;
        TLambdaFrame frame;
        return EqualNodes(left, frame, right, frame, visited, coStore);
    }

    TExprNode::TPtr VisitNode(TExprNode& node, TExprNode* currentLambda, ui16 level,
        std::unordered_multimap<ui64, TExprNode*>& uniqueNodes,
        std::unordered_multimap<ui64, TExprNode*>& incompleteNodes,
        TNodeMap<TExprNode*>& renames, const TColumnOrderStorage& coStore,
        const TNodeSet& reachable) {

        if (node.Type() == TExprNode::Argument) {
            return nullptr;
        }

        const auto find = renames.emplace(&node, nullptr);
        if (!find.second) {
            return find.first->second;
        }

        const auto hash = CalcHash(node, coStore);

        if (node.Type() == TExprNode::Lambda) {
            for (ui32 i = 1U; i < node.ChildrenSize(); ++i) {
                if (auto newNode = VisitNode(*node.Child(i), &node, level + 1U, uniqueNodes, incompleteNodes, renames, coStore, reachable)) {
                    node.ChildRef(i) = std::move(newNode);
                }
            }
        } else {
            for (ui32 i = 0; i < node.ChildrenSize(); ++i) {
                if (auto newNode = VisitNode(*node.Child(i), currentLambda, level, uniqueNodes, incompleteNodes, renames, coStore, reachable)) {
                    node.ChildRef(i) = std::move(newNode);
                }
            }
        }

        if (const auto kind = node.GetTypeAnn()->GetKind(); ETypeAnnotationKind::Flow != kind && ETypeAnnotationKind::Stream != kind || node.IsLambda()) {
            auto& nodesSet = node.IsComplete() ? uniqueNodes : incompleteNodes;

            const auto pair = nodesSet.equal_range(hash);
            auto iter = pair.first;
            while (pair.second != iter) {
                // search for duplicates
                if (iter->second->Dead()) {
                    iter = nodesSet.erase(iter);
                    continue;
                }

                if (!reachable.contains(iter->second)) {
                    iter = nodesSet.erase(iter);
                    continue;
                }

                if (!EqualNodes(node, *iter->second, coStore)) {
#ifndef NDEBUG
                    if (!GetEnv("YQL_ALLOW_CSEE_HASH_COLLISION")) {
                        YQL_ENSURE(false, "Node -BEGIN-\n" << node.Dump() << "-END-" << " has same hash as -BEGIN-\n"
                                                        << iter->second->Dump() << "-END-");
                    }
#endif
                    ++iter;
                    continue;
                }

                if (iter->second == &node)
                    return nullptr;

                find.first->second = iter->second;
                if (node.Type() == TExprNode::Atom) {
                    iter->second->NormalizeAtomFlags(node);
                }

                return iter->second;
            }

            nodesSet.emplace_hint(iter, hash, &node);
        }
        return nullptr;
    }
}

IGraphTransformer::TStatus UpdateCompletness(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext&) {
    YQL_PROFILE_SCOPE(DEBUG, "UpdateCompletness");
    output = input;
    // process closures
    TNodeSet closures;
    TNodeMap<TNodeSet> visited;
    TNodeMap<TNodeSet> visitedInsideDependsOn;
    CalculateCompletness(*input, false, 0, closures, visited, visitedInsideDependsOn);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus EliminateCommonSubExpressions(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx, bool forSubGraph, const TColumnOrderStorage& coStore)
{
    YQL_PROFILE_SCOPE(DEBUG, forSubGraph ? "EliminateCommonSubExpressionsForSubGraph" : "EliminateCommonSubExpressions");
    output = input;
    TNodeSet reachable;
    VisitExpr(*output, [&](const TExprNode& node) {
        reachable.emplace(&node);
        return true;
    });

    TNodeMap<TExprNode*> renames;
    //Cerr << "INPUT\n" << output->Dump() << "\n";
    std::unordered_multimap<ui64, TExprNode*> incompleteNodes;
    const auto newNode = VisitNode(*output, nullptr, 0, ctx.UniqueNodes, incompleteNodes, renames, coStore, reachable);
    YQL_ENSURE(forSubGraph || !newNode);
    //Cerr << "OUTPUT\n" << output->Dump() << "\n";
    return IGraphTransformer::TStatus::Ok;
}

int CompareNodes(const TExprNode& left, const TExprNode& right) {
    TCompareResults visited;
    return CompareNodes(left, right, visited);
}

}
