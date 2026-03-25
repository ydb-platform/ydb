#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <typeinfo>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NDq;

namespace NKikimr {
namespace NKqp {

TPruneColumnsStage::TPruneColumnsStage() : IRBOStage("Prune unnecessary columns") {
    Props = ERuleProperties::RequireParents;
}

void TPruneColumnsStage::RunStage(TOpRoot &root, TRBOContext &ctx) {
    Y_UNUSED(ctx);

    // Collect column uses
    // A column is used if it appears inside a lambda, group-by list, as a join column, or in the final projection
    
    THashSet<TInfoUnit, TInfoUnit::THashFunction> usedSet;
    for (auto it : root) {

        auto usedIUs = it.Current->GetUsedIUs(root.PlanProps);
        for (const auto& iu : usedIUs) {
            usedSet.insert(iu);
        }
    }

    // Add all IUs visible at the root level
    for (const auto& iu : root.GetInput()->GetOutputIUs()) {
        usedSet.insert(iu);
    }

    // At this point we might be missing some intermediate IUs that 
    // were renamed into some IUs of the Used Set
    // We first create a rename map
    THashMap<TInfoUnit, THashSet<TInfoUnit, TInfoUnit::THashFunction>, TInfoUnit::THashFunction> renameMap;

    for (auto it : root) {
        if (it.Current->Kind == EOperator::Map) {
            auto map = CastOperator<TOpMap>(it.Current);
            auto renames = map->GetRenames();
            for (const auto& [to, from] : renames) {
                if (to != from) {
                    if (!renameMap.contains(from)) {
                        renameMap.insert({from, {}});
                    }
                    renameMap.at(from).insert(to);
                }
            }
        }
    }

    // Now we check every key in the rename map if it maps to a used IU
    // Add it to the usedSet if that's the case
    THashMap<TInfoUnit, THashSet<TInfoUnit, TInfoUnit::THashFunction>, TInfoUnit::THashFunction> closedMap;
    for (const auto& [from, to] : renameMap) {
        closedMap.insert({from, to});
    }

    bool closure = false;
    while (!closure) {
        closure = true;

        for (auto& [from, toSet] : closedMap) {
            for (const auto& t : toSet) {
                if (closedMap.contains(t)) {
                    auto& finalSet = closedMap.at(t);
                    if (std::any_of(finalSet.begin(), finalSet.end(), [&toSet] (const TInfoUnit& u) {
                        return !toSet.contains(u);
                    })) {
                        closure = false;
                        for (const auto& iu : finalSet) {
                            toSet.insert(iu);
                        }
                        break;
                    }
                }
            }
            if (!closure) {
                break;
            }
        }
    }

    for (const auto& [from, toSet] : closedMap) {
        if(usedSet.contains(from) || std::any_of(toSet.begin(), toSet.end(), [&usedSet](const TInfoUnit& t) {
            return usedSet.contains(t);
        })) {
            usedSet.insert(from);
            for (const auto & t : toSet) {
                usedSet.insert(t);
            }
        }
    }

    // Visit all read and map operators and remove unused columns
    for (auto it : root) {
        if (it.Current->Kind == EOperator::Source) {
            auto read = CastOperator<TOpRead>(it.Current);
            TVector<TString> newColumns;
            TVector<TInfoUnit> newIUs;

            for (size_t i = 0; i < read->OutputIUs.size(); i++) {
                if (usedSet.contains(read->OutputIUs[i])) {
                    newColumns.push_back(read->Columns[i]);
                    newIUs.push_back(read->OutputIUs[i]);
                }
            }

            // If we have a column store read, and we don't fetch any attributes,
            // we'll have a problem. So we leave the first attribute in the read
            if (read->StorageType == EStorageType::ColumnStorage && newColumns.empty() && !read->Columns.empty()) {
                newColumns.push_back(read->Columns[0]);
                newIUs.push_back(read->OutputIUs[0]);
            }

            read->Columns = newColumns;
            read->OutputIUs = newIUs;

        } else if (it.Current->Kind == EOperator::Map) {
            auto map = CastOperator<TOpMap>(it.Current);
            TVector<TMapElement> newMapElements;

            for (const auto& mapElement : map->MapElements) {
                if (mapElement.IsRename() && usedSet.contains(mapElement.GetElementName())) {
                    newMapElements.emplace_back(mapElement.GetElementName(), mapElement.GetRename(), map->Pos, &ctx.ExprCtx, &root.PlanProps);
                } else {
                    newMapElements.emplace_back(mapElement.GetElementName(), mapElement.GetExpression());
                }
            }
            map->MapElements = newMapElements;
        }
    }
}
}
}
