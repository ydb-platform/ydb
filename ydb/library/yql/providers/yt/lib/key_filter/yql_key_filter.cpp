#include "yql_key_filter.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/string/builder.h>

namespace NYql {

using namespace NNodes;

void TSortMembersCollection::AddTableInfo(size_t tableIndex, const TString& tableName,
    const TVector<TString>& sortMembers,
    const TTypeAnnotationNode::TListType& sortedByTypes,
    const TVector<bool>& sortDirections)
{
    THashMap<TString, TMemberDescrPtr>* nextMembers = &Members;
    for (size_t i = 0; i < sortMembers.size(); ++i) {
        if (!sortDirections.empty() && !sortDirections.at(i)) {
            break;
        }

        TMemberDescrPtr member;
        if (auto p = nextMembers->FindPtr(sortMembers[i])) {
            member = *p;
        }
        else {
            member = MakeIntrusive<TMemberDescr>();
            YQL_ENSURE(IsDataOrOptionalOfData(sortedByTypes[i], member->IsColumnOptional, member->ColumnType),
                "Table " << tableName.Quote() << ", field " << sortMembers[i].Quote()
                << " has incompatible type " << *sortedByTypes[i]);
            nextMembers->emplace(sortMembers[i], member);
        }

        member->Tables.insert(tableIndex);
        nextMembers = &member->NextMembers;
    }
}

bool TSortMembersCollection::ApplyRanges(const TKeyFilterPredicates& ranges, TExprContext& ctx) {
    if (!ApplyRangesImpl(0, ranges, ctx)) {
        return false;
    }

    DropMembersWithoutRanges(Members);

    // Keep table indexes only in leafs
    ApplyRecurs(Members, [&] (const TString& /*name*/, TMemberDescr& member) -> bool {
        if (!member.NextMembers.empty()) {
            for (auto& item: member.NextMembers) {
                for (auto t: item.second->Tables) {
                    member.Tables.erase(t);
                }
            }
            return true;
        }
        return false;
    });

    return true;
}

bool TSortMembersCollection::ApplyRanges(const TVector<TKeyFilterPredicates>& ranges, TExprContext& ctx) {
    for (size_t i = 0; i < ranges.size(); ++i) {
        if (!ApplyRangesImpl(i, ranges[i], ctx)) {
            return false;
        }
    }

    // Ensure that all OR clauses have at least first key
    for (auto& item: Members) {
        for (size_t i = 0; i < ranges.size(); ++i) {
            if (!item.second->Ranges.contains(i)) {
                item.second->Ranges.clear();
                break;
            }
        }
    }

    DropMembersWithoutRanges(Members);

    // Keep table indexes only in leafs
    ApplyRecurs(Members, [&] (const TString& /*name*/, TMemberDescr& member) -> bool {
        if (!member.NextMembers.empty()) {
            for (auto& item: member.NextMembers) {
                for (auto t: item.second->Tables) {
                    member.Tables.erase(t);
                }
            }
            return true;
        }
        return false;
    });

    return true;
}

bool TSortMembersCollection::ApplyRangesImpl(size_t groupIndex, const TKeyFilterPredicates& ranges, TExprContext& ctx) {
    bool hasErrors = false;
    ApplyRecurs(Members, [&] (const TString& name, TMemberDescr& member) -> bool {
        if (hasErrors) {
            return false;
        }

        auto memberRanges = ranges.equal_range(name);
        if (memberRanges.first == memberRanges.second) {
            return false;
        }

        // ensure that we can compare right part of every range with current column
        bool foundEqualComparisons = false;
        for (auto it = memberRanges.first; it != memberRanges.second; ++it) {
            TString cmpOp;
            TExprNode::TPtr value;
            std::tie(cmpOp, value) = it->second;
            const TDataExprType* dataType = nullptr;

            if (value) {
                bool isOptional = false;
                if (!EnsureDataOrOptionalOfData(*value, isOptional, dataType, ctx)) {
                    hasErrors = true;
                    return false;
                }

                if (!GetSuperType(dataType->GetSlot(), member.ColumnType->GetSlot())) {
                    ctx.AddError(TIssue(ctx.GetPosition(value->Pos()),
                        TStringBuilder() << "Column " << name.Quote()
                        << " of " << *static_cast<const TTypeAnnotationNode*>(member.ColumnType)
                        << " type cannot be compared with " << *value->GetTypeAnn()));
                    hasErrors = true;
                    return false;
                }
            }
            else {
                // Not(Exists(member)) case
                if (!member.IsColumnOptional) {
                    return false;
                }
            }

            member.Ranges.emplace(groupIndex, std::make_tuple(cmpOp, value, dataType));
            if (cmpOp == TCoCmpEqual::CallableName()) {
                foundEqualComparisons = true;
            }
        }
        // ensure that ranges contain at least one '==' operation to continue with next member of the compound key
        return foundEqualComparisons;
    });

    return !hasErrors;
}

void TSortMembersCollection::DropMembersWithoutRanges(TMemberDescrMap& members) {
    TVector<TString> toDrop;
    for (auto& item: members) {
        if (item.second->Ranges.empty()) {
            toDrop.push_back(item.first);
        }
        else {
            DropMembersWithoutRanges(item.second->NextMembers);
        }
    }
    for (auto& name: toDrop) {
        members.erase(name);
    }
}

void TSortMembersCollection::ApplyRecurs(TMemberDescrMap& members, const std::function<bool(const TString&, TMemberDescr&)>& func) {
    for (auto& item: members) {
        if (func(item.first, *item.second)) {
            ApplyRecurs(item.second->NextMembers, func);
        }
    }
}

} // NYql
