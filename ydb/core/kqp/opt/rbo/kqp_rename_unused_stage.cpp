#include "kqp_rbo_rules.h"
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>
#include <typeinfo>

using namespace NYql::NNodes;

namespace NKikimr {
namespace NKqp {

struct Scope {
    Scope() {}

    TVector<int> ParentScopes;
    bool TopScope = false;
    bool IdentityMap = true;
    THashSet<TInfoUnit, TInfoUnit::THashFunction> Unrenameable;
    TVector<TInfoUnit> OutputIUs;
    TVector<std::shared_ptr<IOperator>> Operators;

    TString ToString(TExprContext &ctx) {
        auto res = TStringBuilder() << "{parents: [";
        for (int p : ParentScopes) {
            res << p << ",";
        }
        res << "], Identity: " << IdentityMap << ", TopScope: " << TopScope << ", Unrenameable: {";
        for (auto &iu : Unrenameable) {
            res << iu.GetFullName() << ",";
        }
        res << "}, Output: {";
        for (auto &iu : OutputIUs) {
            res << iu.GetFullName() << ",";
        }
        res << "}, Operators: [";
        for (auto &op : Operators) {
            res << op->ToString(ctx) << ",";
        }
        res << "]}";
        return res;
    }
};

struct TIOperatorSharedPtrHash {
    size_t operator()(const std::shared_ptr<IOperator> &p) const { return p ? THash<int64_t>{}((int64_t)p.get()) : 0; }
};

class Scopes {
  public:
    void ComputeScopesRec(std::shared_ptr<IOperator> &op, int &currScope);
    void ComputeScopes(std::shared_ptr<IOperator> &op);

    THashMap<int, Scope> ScopeMap;
    THashMap<std::shared_ptr<IOperator>, int, TIOperatorSharedPtrHash> RevScopeMap;
};

void Scopes::ComputeScopesRec(std::shared_ptr<IOperator> &op, int &currScope) {
    if (RevScopeMap.contains(op)) {
        return;
    }
    bool makeNewScope =
        (op->Kind == EOperator::Map && CastOperator<TOpMap>(op)->Project) || (op->Kind == EOperator::Project) || (op->Parents.size() >= 2);

    //YQL_CLOG(TRACE, CoreDq) << "Op: " << op->ToString() << ", nparents = " << op->Parents.size();

    if (makeNewScope) {
        currScope++;
        auto newScope = Scope();
        // FIXME: The top scope is a scope with id=1
        if (currScope == 1) {
            newScope.TopScope = true;
        }

        if (op->Kind == EOperator::Map && CastOperator<TOpMap>(op)->Project) {
            auto map = CastOperator<TOpMap>(op);
            newScope.OutputIUs = map->GetOutputIUs();
            newScope.IdentityMap = false;
        } else if (op->Kind == EOperator::Project) {
            auto project = CastOperator<TOpProject>(op);
            newScope.OutputIUs = project->GetOutputIUs();
            newScope.IdentityMap = false;
        }
        ScopeMap[currScope] = newScope;
    }

    if (op->Kind == EOperator::Source) {
        for (auto iu : op->GetOutputIUs()) {
            ScopeMap.at(currScope).Unrenameable.insert(iu);
        }
    }

    ScopeMap.at(currScope).Operators.push_back(op);
    RevScopeMap[op] = currScope;
    for (auto c : op->Children) {
        ComputeScopesRec(c, currScope);
    }
}

void Scopes::ComputeScopes(std::shared_ptr<IOperator> &op) {
    int currScope = 0;
    ScopeMap[0] = Scope();
    ComputeScopesRec(op, currScope);
    for (auto &[id, sc] : ScopeMap) {
        auto topOp = sc.Operators[0];
        for (auto &p : topOp->Parents) {
            auto parentScopeId = RevScopeMap.at(p.lock());
            sc.ParentScopes.push_back(parentScopeId);
            if (topOp->Parents.size() >= 2) {
                auto &parentScope = ScopeMap.at(parentScopeId);
                for (auto iu : sc.OutputIUs) {
                    parentScope.Unrenameable.insert(iu);
                }
            }
        }
    }
}

struct TIntTUnitPairHash {
    size_t operator()(const std::pair<int, TInfoUnit> &p) const { return THash<int>{}(p.first) ^ TInfoUnit::THashFunction{}(p.second); }
};


/**
 * Global stage that removed unnecessary renames and unused columns
 */

TRenameStage::TRenameStage() {
    Props.RequireParents = true;
}

void TRenameStage::RunStage(TOpRoot &root, TRBOContext &ctx) {

    YQL_CLOG(TRACE, CoreDq) << "Before compute parents";

    for (auto it : root) {
        YQL_CLOG(TRACE, CoreDq) << "Iterator: " << it.Current->ToString(ctx.ExprCtx);
        for (auto c : it.Current->Children) {
            YQL_CLOG(TRACE, CoreDq) << "Child: " << c->ToString(ctx.ExprCtx);
        }
    }

    root.ComputeParents();

    // We need to build scopes for the plan, because same aliases and variable names may be
    // used multiple times in different scopes
    auto scopes = Scopes();
    scopes.ComputeScopes(root.GetInput());

    for (auto &[id, sc] : scopes.ScopeMap) {
        YQL_CLOG(TRACE, CoreDq) << "Scope map: " << id << ": " << sc.ToString(ctx.ExprCtx);
    }

    // Build a rename map by startingg at maps that rename variables and project
    // Follow the parent scopes as far as possible and pick the top-most mapping
    // If at any point there are multiple parent scopes - stop

    THashMap<std::pair<int, TInfoUnit>, TVector<std::pair<int, TInfoUnit>>, TIntTUnitPairHash> renameMap;

    int newAliasId = 1;

    for (auto iter : root) {
        if (iter.Current->Kind == EOperator::Map && CastOperator<TOpMap>(iter.Current)->Project) {
            auto map = CastOperator<TOpMap>(iter.Current);

            for (auto [to, body] : map->MapElements) {

                auto scopeId = scopes.RevScopeMap.at(map);
                auto scope = scopes.ScopeMap.at(scopeId);
                auto parentScopes = scope.ParentScopes;

                // If we're not in the final scope that exports variables to the user,
                // generate a unique new alias for the variable to avoid collisions
                auto exportTo = to;
                if (!scope.TopScope) {
                    TString newAlias = "#" + std::to_string(newAliasId++);
                    exportTo = TInfoUnit(newAlias, to.ColumnName);
                }

                // "Export" the result of map output to the upper scope, but only if there is one
                // parent scope only
                auto source = std::make_pair(scopeId, to);
                auto target = std::make_pair(parentScopes[0], exportTo);
                renameMap[source].push_back(target);

                // if (parentScopes.size()==1) {
                //     renameMap[source].push_back(target);
                // }

                // If the map element is a rename, record the rename in the map within the same scope
                // However skip all unrenamable uis
                if (std::holds_alternative<TInfoUnit>(body)) {
                    auto sourceIU = std::get<TInfoUnit>(body);
                    if (!scope.Unrenameable.contains(sourceIU)) {
                        source = std::make_pair(scopeId, sourceIU);
                        target = std::make_pair(scopeId, to);
                        renameMap[source].push_back(target);
                    }
                }
            }
        }
    }

    for (auto &[key, value] : renameMap) {
        if (value.size() == 1) {
            YQL_CLOG(TRACE, CoreDq) << "Rename map: " << key.second.GetFullName() << "," << key.first << " -> "
                                    << value[0].second.GetFullName() << "," << value[0].first;
        } else {
            YQL_CLOG(TRACE, CoreDq) << "Rename map: " << key.second.GetFullName() << "," << key.first << " -> ";
            for (auto v : value) {
                YQL_CLOG(TRACE, CoreDq) << v.second.GetFullName() << "," << v.first;
            }
        }
    }

    // Make a transitive closure of rename map
    THashMap<std::pair<int, TInfoUnit>, std::pair<int, TInfoUnit>, TIntTUnitPairHash> closedMap;
    for (auto &[k, v] : renameMap) {
        if (v.size() == 1) {
            closedMap[k] = v[0];
        }
    }

    bool fixpointReached = false;
    while (!fixpointReached) {

        fixpointReached = true;
        for (auto &[k, v] : closedMap) {
            if (closedMap.contains(v)) {
                fixpointReached = false;
            }

            while (closedMap.contains(v)) {
                v = closedMap.at(v);
            }
            closedMap[k] = v;
        }
    }

    // Add unique aliases

    // Iterate through the plan, applying renames to one operator at a time

    for (auto it : root) {
        // Build a subset of the map for the current scope only
        auto scopeId = scopes.RevScopeMap.at(it.Current);

        // Exclude all IUs from OpReads in this scope
        // THashSet<TInfoUnit, TInfoUnit::THashFunction> exclude;
        // for (auto & op : scopes.ScopeMap.at(scopeId).Operators) {
        //    if (op->Kind == EOperator::Source) {
        //        for (auto iu : op->GetOutputIUs()) {
        //            exclude.insert(iu);
        //        }
        //    }
        //}

        auto scopedRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>();
        for (auto &[k, v] : closedMap) {
            // if (k.first == scopeId && !exclude.contains(k.second)) {
            if (k.first == scopeId) {
                scopedRenameMap.emplace(k.second, v.second);
            }
        }

        YQL_CLOG(TRACE, CoreDq) << "Applying renames to operator: " << scopeId << ":" << it.Current->ToString(ctx.ExprCtx);
        for (auto &[k, v] : scopedRenameMap) {
            YQL_CLOG(TRACE, CoreDq) << "From " << k.GetFullName() << ", To " << v.GetFullName();
        }

        it.Current->RenameIUs(scopedRenameMap, ctx.ExprCtx);
    }
}

}
}