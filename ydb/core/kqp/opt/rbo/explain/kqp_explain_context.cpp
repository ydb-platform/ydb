#include "kqp_explain_context.h"

#include <util/generic/hash_set.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NKikimr::NKqp {

TString TExplainContext::UnknownExpressionName(const NYql::TExprNode::TPtr& node) {
    auto [it, inserted] = UnknownExpressions_.try_emplace(node);
    if (inserted) {
        it->second.Name = TStringBuilder() << 'f' << UnknownExpressions_.size();
    }
    return it->second.Name;
}

TString TExplainContext::DescribeUnknownExpression(const NYql::TExprNode::TPtr& node) {
    const auto name = UnknownExpressionName(node);
    auto& entry = UnknownExpressions_.at(node);
    if (entry.Description) {
        return *entry.Description;
    }

    constexpr size_t MaxNodes = 4096;
    constexpr size_t MaxInputs = 8;
    THashSet<const NYql::TExprNode*> visited;
    TVector<const NYql::TExprNode*> pending;
    if (node) pending.push_back(node.Get());
    TSet<TString> inputs;
    bool truncated = false;
    while (!pending.empty()) {
        if (visited.size() >= MaxNodes || !RemainingNodes_) {
            truncated = true;
            break;
        }
        const auto* current = pending.back();
        pending.pop_back();
        if (!visited.insert(current).second) continue;
        --RemainingNodes_;
        if (current->IsCallable("Parameter") && current->ChildrenSize() == 2 && current->Head().IsAtom()) {
            const auto input = current->Head().Content();
            if (input.size() > 128 || (inputs.size() == MaxInputs && !inputs.contains(TString(input)))) {
                truncated = true;
                break;
            }
            inputs.emplace(input);
            continue;
        }
        if (current->ChildrenSize() > MaxNodes - visited.size() - pending.size()) {
            truncated = true;
            break;
        }
        for (size_t i = current->ChildrenSize(); i; --i) {
            pending.push_back(current->Child(i - 1));
        }
    }

    TStringBuilder result;
    result << name << '(';
    TStringBuf separator;
    for (const auto& input : inputs) {
        result << separator << input;
        separator = ", ";
    }
    if (truncated) result << separator << "…";
    result << ')';
    entry.Description = result;
    return *entry.Description;
}

} // namespace NKikimr::NKqp
