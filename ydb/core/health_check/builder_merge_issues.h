#pragma once

#include "health_check_structs.h"

#include <regex>

namespace NKikimr::NHealthCheck {

struct TMergeIssuesContext {
    std::unordered_map<ETags, TList<TSelfCheckResult::TIssueRecord>> recordsMap;
    std::unordered_set<TString> removeIssuesIds;

    static std::shared_ptr<TList<TSelfCheckResult::TIssueRecord>> FindChildrenRecords(TList<TSelfCheckResult::TIssueRecord>& records, TSelfCheckResult::TIssueRecord& parent) {
        std::shared_ptr<TList<TSelfCheckResult::TIssueRecord>> children(new TList<TSelfCheckResult::TIssueRecord>);
        std::unordered_set<TString> childrenIds;
        for (auto reason: parent.IssueLog.reason()) {
            childrenIds.insert(reason);
        }

        for (auto it = records.begin(); it != records.end(); ) {
            if (childrenIds.contains(it->IssueLog.id())) {
                auto move = it++;
                children->splice(children->end(), records, move);
            } else {
                it++;
            }
        }

        return children;
    }

    void MoveDataInFirstRecord(TList<TSelfCheckResult::TIssueRecord>& similar) {
        auto mainReasons = similar.begin()->IssueLog.mutable_reason();
        std::unordered_set<TString> ids;
        ids.insert(similar.begin()->IssueLog.id());
        std::unordered_set<TString> mainReasonIds;
        for (auto it = mainReasons->begin(); it != mainReasons->end(); it++) {
            mainReasonIds.insert(*it);
        }

        for (auto it = std::next(similar.begin(), 1); it != similar.end(); ) {
            if (ids.contains(it->IssueLog.id())) {
                it++;
                continue;
            }
            ids.insert(it->IssueLog.id());

            switch (similar.begin()->Tag) {
                case ETags::GroupState: {
                    auto mainGroupIds = similar.begin()->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_id();
                    auto donorGroupIds = it->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_id();
                    mainGroupIds->Add(donorGroupIds->begin(), donorGroupIds->end());
                    break;
                }
                case ETags::VDiskState: {
                    auto mainVdiskIds = similar.begin()->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_id();
                    auto donorVdiskIds = it->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_id();
                    mainVdiskIds->Add(donorVdiskIds->begin(), donorVdiskIds->end());
                    break;
                }
                case ETags::PDiskState: {
                    auto mainPdisk = similar.begin()->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk();
                    auto donorPdisk = it->IssueLog.mutable_location()->mutable_storage()->mutable_pool()->mutable_group()->mutable_vdisk()->mutable_pdisk();
                    mainPdisk->Add(donorPdisk->begin(), donorPdisk->end());
                    break;
                }
                default:
                    break;
            }

            auto donorReasons = it->IssueLog.mutable_reason();
            for (auto donorReasonIt = donorReasons->begin(); donorReasonIt != donorReasons->end(); donorReasonIt++) {
                if (!mainReasonIds.contains(*donorReasonIt)) {
                    mainReasons->Add(donorReasonIt->c_str());
                    mainReasonIds.insert(*donorReasonIt);
                }
            }

            removeIssuesIds.insert(it->IssueLog.id());
            it = similar.erase(it);
        }

        similar.begin()->IssueLog.set_count(ids.size());
        similar.begin()->IssueLog.set_listed(ids.size());
    }

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

    static bool FindRecordsForMerge(TList<TSelfCheckResult::TIssueRecord>& records, TList<TSelfCheckResult::TIssueRecord>& similar, TList<TSelfCheckResult::TIssueRecord>& merged) {
        while (!records.empty() && similar.empty()) {
            similar.splice(similar.end(), records, records.begin());
            for (auto it = records.begin(); it != records.end(); ) {
                bool isSimilar = it->IssueLog.status() == similar.begin()->IssueLog.status()
                    && it->IssueLog.message() == similar.begin()->IssueLog.message()
                    && it->IssueLog.level() == similar.begin()->IssueLog.level() ;
                if (isSimilar && similar.begin()->Tag == ETags::VDiskState) {
                    isSimilar = it->IssueLog.location().storage().node().id() == similar.begin()->IssueLog.location().storage().node().id();
                }
                if (isSimilar) {
                    auto move = it++;
                    similar.splice(similar.end(), records, move);
                } else {
                    ++it;
                }
            }

            if (similar.size() == 1) {
                merged.splice(merged.end(), similar);
            }
        }

        return !similar.empty();
    }

    void MergeLevelRecords(TList<TSelfCheckResult::TIssueRecord>& records) {
        TList<TSelfCheckResult::TIssueRecord> handled;
        while (!records.empty()) {
            TList<TSelfCheckResult::TIssueRecord> similar;
            if (FindRecordsForMerge(records, similar, handled)) {
                MoveDataInFirstRecord(similar);
                handled.splice(handled.end(), similar, similar.begin());
            }
        }
        records.splice(records.end(), handled);
    }

    void MergeLevelRecords(ETags levelTag) {
        auto& records = GetRecords(levelTag);
        MergeLevelRecords(records);
    }

    void MergeLevelRecords(ETags levelTag, ETags upperTag) {
        auto& levelRecords = GetRecords(levelTag);
        auto& upperRecords = GetRecords(upperTag);

        for (auto it = upperRecords.begin(); it != upperRecords.end(); it++) {
            auto children = FindChildrenRecords(levelRecords, *it);
            if (children->size() > 1) {
                MergeLevelRecords(*children);
            }
            levelRecords.splice(levelRecords.end(), *children);
        }
    }
};

} // NKikimr::NHealthCheck