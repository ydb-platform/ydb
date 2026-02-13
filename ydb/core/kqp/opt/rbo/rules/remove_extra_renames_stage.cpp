#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
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
    bool MultipleConsumers = false;
    TVector<TInfoUnit> OutputIUs;
    TVector<std::shared_ptr<IOperator>> Operators;

    TString ToString(TExprContext &ctx) {
        auto res = TStringBuilder() << "{parents: [";
        for (int p : ParentScopes) {
            res << p << ",";
        }
        res << "], MultipleConsumers: " << MultipleConsumers << ", TopScope: " << TopScope << ", Output: {";
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
    // We create a new scope for projection operators: map, project and group-by
    // Also we create a new scope for Read, beacause of current limitation that it cannot rename output vars

    bool makeNewScope =
        (op->Kind == EOperator::Map && CastOperator<TOpMap>(op)->Project) || 
        (op->Kind == EOperator::Project) || 
        (op->Kind == EOperator::Aggregate) ||
        (op->Parents.size() >= 2);

    //YQL_CLOG(TRACE, CoreDq) << "Op: " << op->ToString() << ", nparents = " << op->Parents.size();

    if (makeNewScope) {
        currScope++;
        auto newScope = Scope();
        // FIXME: The top scope is a scope with id=1
        if (currScope == 1) {
            newScope.TopScope = true;
        }

        newScope.OutputIUs = op->GetOutputIUs();
        newScope.MultipleConsumers = op->Parents.size() >= 2;
        ScopeMap[currScope] = newScope;
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
            auto parentScopeId = RevScopeMap.at(p.first.lock());
            sc.ParentScopes.push_back(parentScopeId);
        }
    }
}

struct TIntTUnitPairHash {
    size_t operator()(const std::pair<int, TInfoUnit> &p) const { return THash<int>{}(p.first) ^ TInfoUnit::THashFunction{}(p.second); }
};


/**
 * Global stage that removed unnecessary renames and unused columns
 */

TRenameStage::TRenameStage() : IRBOStage("Remove redundant maps") {
    Props = ERuleProperties::RequireParents;
}

void TRenameStage::RunStage(TOpRoot &root, TRBOContext &ctx) {
    // We need to build scopes for the plan, because same aliases and variable names may be
    // used multiple times in different scopes
    auto scopes = Scopes();
    scopes.ComputeScopes(root.GetInput());

    for (auto &[id, sc] : scopes.ScopeMap) {
        YQL_CLOG(TRACE, CoreDq) << "Scope map: " << id << ": " << sc.ToString(ctx.ExprCtx);
    }

    // Build a rename map by starting at maps that rename variables and project
    // Follow the parent scopes as far as possible and pick the top-most mapping
    // If at any point there are multiple parent scopes - stop

    THashMap<std::pair<int, TInfoUnit>, THashSet<std::pair<int, TInfoUnit>, TIntTUnitPairHash>, TIntTUnitPairHash> renameMap;

    // We record specific IUs with their scope that are "poisoned"
    // They arrise when an operator has multiple consumers or when a single column is mapped into multiple ones
    // This column cannot be renamed, and we need to propagate the poison through all the scopes where this column
    // is live.
    THashSet<std::pair<int, TInfoUnit>, TIntTUnitPairHash> poison;

    for (auto iter : root) {
        // FIXME: If there is no scope for this operator, that means it from a subplan that we didn't process
        // while building the scope map. We skip them for now
        if (!scopes.RevScopeMap.contains(iter.Current)) {
            continue;
        }

        TVector<TInfoUnit> mapsTo;
        THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> mapsFrom;
        
        if (iter.Current->Kind == EOperator::Map) {
            auto map = CastOperator<TOpMap>(iter.Current);
            for (const auto& mapElement: map->MapElements) {
                mapsTo.push_back(mapElement.GetElementName());
                if (mapElement.IsRename()) {
                    auto from = mapElement.GetRename();
                    if (mapElement.GetElementName() != from) {
                        mapsFrom[mapElement.GetElementName()] = from;
                    }
                }
            }
        }
        else if (iter.Current->Kind == EOperator::Source) {
            auto read = CastOperator<TOpRead>(iter.Current);
            for (size_t i = 0; i < read->OutputIUs.size(); i++) {
                auto from = TInfoUnit(read->Alias, read->Columns[i]);
                auto to = read->OutputIUs[i];
                mapsTo.push_back(to);
                if (to != from) {
                    mapsFrom[to] = from;
                }
            }
        }
        else if (iter.Current->Kind == EOperator::Aggregate) {
            auto agg = CastOperator<TOpAggregate>(iter.Current);
            for (const auto& col : agg->KeyColumns) {
                mapsTo.push_back(col);
            }
            for (const auto& trait : agg->AggregationTraitsList) {
                mapsTo.push_back(trait.OriginalColName);
                mapsTo.push_back(trait.ResultColName);
            }
        }

        for (const auto& to : mapsTo) {
            auto scopeId = scopes.RevScopeMap.at(iter.Current);
            auto scope = scopes.ScopeMap.at(scopeId);
            auto parentScopes = scope.ParentScopes;

            // "Export" the result of map output to the upper scope, but only if there is one
            // parent scope only
            // If there are multiple scopes, we need to poison the 
            auto source = std::make_pair(scopeId, to);
            auto target = std::make_pair(parentScopes[0], to);

            if (parentScopes.size()==1) {
                YQL_CLOG(TRACE, CoreDq) << "Rename map: " << source.second.GetFullName() << "," << source.first << " -> "
                    << target.second.GetFullName() << "," << target.first;
                renameMap[source].insert(target);
            }
            else {
                // We poison the column if it can be potentially used in multiple scopes above
                // FIXME: We can improve this by really checking if its used by multiple scopes. Could be the 
                // case where its used only by one of the multiple consumers
                poison.insert(source);
            }

            // If the map element is a rename, record the rename in the map within the same scope
            if (mapsFrom.contains(to)) {
                auto sourceIU = mapsFrom.at(to);
                source = std::make_pair(scopeId, sourceIU);
                target = std::make_pair(scopeId, to);
                renameMap[source].insert(target);
                YQL_CLOG(TRACE, CoreDq) << "Rename map: " << source.second.GetFullName() << "," << source.first << " -> "
                        << target.second.GetFullName() << "," << target.first;
            }
        }
    }

    for (auto &[key, value] : renameMap) {
        if (value.size() == 1) {
            YQL_CLOG(TRACE, CoreDq) << "Full Rename map: " << key.second.GetFullName() << "," << key.first << " -> "
                                    << (*value.begin()).second.GetFullName() << "," << (*value.begin()).first;
        } else {
            YQL_CLOG(TRACE, CoreDq) << "Full Rename map: " << key.second.GetFullName() << "," << key.first << " -> ";
            for (const auto& v : value) {
                YQL_CLOG(TRACE, CoreDq) << v.second.GetFullName() << "," << v.first;
            }
        }
    }


    // Closed map is a closure of the renaming map
    THashMap<std::pair<int, TInfoUnit>, std::pair<int, TInfoUnit>, TIntTUnitPairHash> closedMap;

    // We also record a stop list where we avoid renaming rvalues (use values), but we can always
    // rename rvalues (defines)
    // Initially stop list contains all the references in the plan, we then remove the ones that
    // are unnecessary
    THashSet<std::pair<int, TInfoUnit>, TIntTUnitPairHash> stopList;

    for (auto &[k, v] : renameMap) {
        stopList.insert(k);

        // If the rename map has only one target, everything is fine, we record it in the closure
        if (v.size() == 1) {
            closedMap[k] = *v.begin();
            stopList.insert(*v.begin());
        }

        // Otherwise, we don't add it to the closure and add the destination of the mapping to the
        // poison list
        else {
            for (auto deadEnd : v) {
                poison.insert(deadEnd);
                stopList.insert(deadEnd);
            }
        }
    }

    // Propagate poison through the map: column can be exported to its parent scopes w/o changing the name,
    // we propagate the poison in such cases
    bool fixpointReached = false;
    while (!fixpointReached) {
        fixpointReached = true;
        for (auto &[k, v] : closedMap) {
            if (closedMap.contains(v) && poison.contains(k) && !poison.contains(v) && k.second == v.second) {
                fixpointReached = false;
                poison.insert(v);
            }
        }
    }

    // Now we compute the transitive closure

    fixpointReached = false;
    while (!fixpointReached) {

        fixpointReached = true;

        // pick any mapping in the current closure
        for (auto &[k, v] : closedMap) {

            // If there is a potential to add to the closure and the current column is not poisoned,
            // follow the remap chain and compute the final target
            if (closedMap.contains(v) && !poison.contains(k)) {
                fixpointReached = false;
            }

            // Follow remap chain
            while (closedMap.contains(v)) {
                auto newVar = closedMap.at(v);

                // if the current column is poisoned and a new name is assigned (vs. just export to upper scope)
                // then we don't add the target to the closure
                if (poison.contains(v) && newVar.second != v.second) {
                    break;
                } 
                // Otherwise, we'll continue up the chain and clear the current column from the use stoplist
                else {
                    stopList.erase(v);
                }
                v = newVar;
            }
            closedMap[k] = v;
        }
    }

    for (auto &[key, value] : closedMap) {
        YQL_CLOG(TRACE, CoreDq) << "Closed map: " << key.second.GetFullName() << "," << key.first << " -> "
                                    << value.second.GetFullName() << "," << value.first << ", poison: " << poison.contains(key) << ", stopList: " << stopList.contains(key);
        
    }

    // Iterate through the plan, applying renames to one operator at a time

    for (auto it : root) {
        // FIXME: If there is no scope for this operator, that means it from a subplan that we didn't process
        // while building the scope map. We skip them for now
        if (!scopes.RevScopeMap.contains(it.Current)) {
            continue;
        }

        // Build a subset of the map for the current scope only
        auto scopeId = scopes.RevScopeMap.at(it.Current);

        auto scopedRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>();
        auto scopedStopList = THashSet<TInfoUnit, TInfoUnit::THashFunction>();

        for (auto &[k, v] : closedMap) {
            if (k.first == scopeId && !poison.contains(k)) {
                scopedRenameMap.emplace(k.second, v.second);
            }
        }

        for (auto s : stopList) {
            if (s.first == scopeId) {
                scopedStopList.insert(s.second);
            }
        }

        // If we have anything but the map operator, apply the stop list to the mapping
        if (it.Current->Kind != EOperator::Map && it.Current->Kind != EOperator::Aggregate) {
            for (auto s : scopedStopList) {
                if (scopedRenameMap.contains(s)) {
                    scopedRenameMap.erase(s);
                }
            }
        }

        YQL_CLOG(TRACE, CoreDq) << "Applying renames to operator: " << scopeId << ":" << it.Current->ToString(ctx.ExprCtx);
        for (auto &[k, v] : scopedRenameMap) {
            YQL_CLOG(TRACE, CoreDq) << "From " << k.GetFullName() << ", To " << v.GetFullName();
        }

        it.Current->RenameIUs(scopedRenameMap, ctx.ExprCtx, scopedStopList);
        YQL_CLOG(TRACE, CoreDq) << "After apply: " << it.Current->ToString(ctx.ExprCtx);


    }
}

}
}