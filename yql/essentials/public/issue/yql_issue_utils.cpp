#include "yql_issue_utils.h"

#include <util/system/yassert.h>

#include <tuple>
#include <list>
#include <algorithm>
#include <deque>

namespace NYql {

TIssue TruncateIssueLevels(const TIssue& topIssue, TTruncateIssueOpts opts) {
    // [issue, level, parent, visibleParent]
    std::list<std::tuple<const TIssue*, ui32, size_t, size_t>> issueQueue;
    // [issue, targetIssue, level, parent, visible, visibleParent, targetSkipIssue]
    std::deque<std::tuple<const TIssue*, TIssue*, ui32, size_t, bool, size_t, TIssue*>> issues;
    // [depth from bottom, position]
    std::list<size_t> leafs;

    const auto depthBeforeLeaf = std::max(opts.KeepTailLevels, ui32(1)) - 1;
    const auto maxLevels = std::max(opts.MaxLevels - std::min(opts.MaxLevels, depthBeforeLeaf + 1), ui32(1));

    issueQueue.emplace_front(&topIssue, 0, 0, 0);
    while (!issueQueue.empty()) {
        const auto issue = std::get<0>(issueQueue.back());
        const auto level = std::get<1>(issueQueue.back());
        const auto parent = std::get<2>(issueQueue.back());
        const auto visibleParent = std::get<3>(issueQueue.back());
        issueQueue.pop_back();

        const bool visible = issue->GetSubIssues().empty() || level < maxLevels;
        const auto pos = issues.size();
        issues.emplace_back(issue, nullptr, level, parent, visible, visibleParent, nullptr);
        if (issue->GetSubIssues().empty()) {
            if (level != 0) {
                leafs.push_back(pos);
            }
        } else {
            for (auto subIssue : issue->GetSubIssues()) {
                issueQueue.emplace_front(subIssue.Get(), level + 1, pos, visible ? pos : visibleParent);
            }
        }
    }

    if (depthBeforeLeaf && !leafs.empty()) {
        for (size_t pos: leafs) {
            ui32 depth = depthBeforeLeaf;
            auto parent = std::get<3>(issues.at(pos));
            while (depth && parent) {
                auto& visible = std::get<4>(issues.at(parent));
                auto& visibleParent = std::get<5>(issues.at(pos));
                if (!visible || visibleParent != parent) {
                    visible = true;
                    visibleParent = parent; // Update visible parent
                    --depth;
                    pos = parent;
                    parent = std::get<3>(issues.at(parent));
                } else {
                    break;
                }
            }
        }
    }
    leafs.clear();

    TIssue result;
    for (auto& i: issues) {
        const auto srcIssue = std::get<0>(i);
        auto& targetIssue = std::get<1>(i);
        const auto level = std::get<2>(i);
        const auto parent = std::get<3>(i);
        const auto visible = std::get<4>(i);
        const auto visibleParent = std::get<5>(i);

        if (0 == level) {
            targetIssue = &result;
            targetIssue->CopyWithoutSubIssues(*srcIssue);
        } else if (visible) {
            auto& parentRec = issues.at(visibleParent);
            auto& parentTargetIssue = std::get<1>(parentRec);
            if (parent != visibleParent) {
                auto& parentSkipIssue = std::get<6>(parentRec);
                if (!parentSkipIssue) {
                    const auto parentIssue = std::get<0>(parentRec);
                    auto newIssue = MakeIntrusive<TIssue>("(skipped levels)");
                    newIssue->SetCode(parentIssue->GetCode(), parentIssue->GetSeverity());
                    parentTargetIssue->AddSubIssue(newIssue);
                    parentSkipIssue = newIssue.Get();
                }
                auto newIssue = MakeIntrusive<TIssue>(TString{});
                newIssue->CopyWithoutSubIssues(*srcIssue);
                parentSkipIssue->AddSubIssue(newIssue);
                targetIssue = newIssue.Get();
            } else {
                auto newIssue = MakeIntrusive<TIssue>(TString{});
                newIssue->CopyWithoutSubIssues(*srcIssue);
                parentTargetIssue->AddSubIssue(newIssue);
                targetIssue = newIssue.Get();
            }
        }
    }
    return result;
}

} // namspace NYql
