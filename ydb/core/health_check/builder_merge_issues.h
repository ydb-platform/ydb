#pragma once

#include "health_check_data.h"

namespace NKikimr::NHealthCheck {

struct TMergeIssuesContext {
    std::unordered_map<ETags, TList<TSelfCheckResult::TIssueRecord>> recordsMap;
    std::unordered_set<TString> removeIssuesIds;

    TMergeIssuesContext(TList<TSelfCheckResult::TIssueRecord>& records) {
        for (auto it = records.begin(); it != records.end(); ) {
            auto move = it++;
            recordsMap[move->Tag].splice(recordsMap[move->Tag].end(), records, move);
        }
    }

    void RemoveUnlinkIssues(TList<TSelfCheckResult::TIssueRecord>& records) {
        bool isRemovingIssuesIteration = true;
        while (isRemovingIssuesIteration) {
            isRemovingIssuesIteration = false;

            std::unordered_set<TString> necessaryIssuesIds;
            for (auto it = records.begin(); it != records.end(); it++) {
                auto reasons = it->IssueLog.reason();
                for (auto reasonIt = reasons.begin(); reasonIt != reasons.end(); reasonIt++) {
                    necessaryIssuesIds.insert(*reasonIt);
                }
            }

            for (auto it = records.begin(); it != records.end(); ) {
                if (!necessaryIssuesIds.contains(it->IssueLog.id()) && removeIssuesIds.contains(it->IssueLog.id())) {
                    auto reasons = it->IssueLog.reason();
                    for (auto reasonIt = reasons.begin(); reasonIt != reasons.end(); reasonIt++) {
                        removeIssuesIds.insert(*reasonIt);
                    }
                    isRemovingIssuesIteration = true;
                    it = records.erase(it);
                } else {
                    it++;
                }
            }
        }

        {
            std::unordered_set<TString> issueIds;
            for (auto it = records.begin(); it != records.end(); it++) {
                issueIds.insert(it->IssueLog.id());
            }

            for (auto it = records.begin(); it != records.end(); it++) {
                auto reasons = it->IssueLog.mutable_reason();
                for (auto reasonIt = reasons->begin(); reasonIt != reasons->end(); ) {
                    if (!issueIds.contains(*reasonIt)) {
                        reasonIt = reasons->erase(reasonIt);
                    } else {
                        reasonIt++;
                    }
                }
            }
        }
    }

    void RenameMergingIssues(TList<TSelfCheckResult::TIssueRecord>& records) {
        for (auto it = records.begin(); it != records.end(); it++) {
            if (it->IssueLog.count() > 0) {
                TString message = it->IssueLog.message();
                switch (it->Tag) {
                    case ETags::GroupState: {
                        message = std::regex_replace(message.c_str(), std::regex("^Group has "), "Groups have ");
                        message = std::regex_replace(message.c_str(), std::regex("^Group is "), "Groups are ");
                        message = std::regex_replace(message.c_str(), std::regex("^Group "), "Groups ");
                        break;
                    }
                    case ETags::VDiskState: {
                        message = std::regex_replace(message.c_str(), std::regex("^VDisk has "), "VDisks have ");
                        message = std::regex_replace(message.c_str(), std::regex("^VDisk is "), "VDisks are ");
                        message = std::regex_replace(message.c_str(), std::regex("^VDisk "), "VDisks ");
                        break;
                    }
                    case ETags::PDiskState: {
                        message = std::regex_replace(message.c_str(), std::regex("^PDisk has "), "PDisks have ");
                        message = std::regex_replace(message.c_str(), std::regex("^PDisk is "), "PDisks are ");
                        message = std::regex_replace(message.c_str(), std::regex("^PDisk "), "PDisks ");
                        break;
                    }
                    default:
                        break;
                }
                it->IssueLog.set_message(message);
            }
        }
    }

    void FillRecords(TList<TSelfCheckResult::TIssueRecord>& records) {
        for(auto it = recordsMap.begin(); it != recordsMap.end(); ++it) {
            records.splice(records.end(), it->second);
        }
        RemoveUnlinkIssues(records);
        RenameMergingIssues(records);
    }

    TList<TSelfCheckResult::TIssueRecord>& GetRecords(ETags tag) {
        return recordsMap[tag];
    }
};

} // NKikimr::NHealthCheck