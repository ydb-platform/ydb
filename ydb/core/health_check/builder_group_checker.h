#pragma once

namespace NKikimr::NHealthCheck {

class TGroupChecker {
    TString ErasureSpecies;
    bool LayoutCorrect;
    int FailedDisks = 0;
    std::array<int, Ydb::Monitoring::StatusFlag::Status_ARRAYSIZE> DisksColors = {};
    TStackVec<std::pair<ui32, int>> FailedRealms;

    void IncrementFor(ui32 realm) {
        auto itRealm = FindIf(FailedRealms, [realm](const std::pair<ui32, int>& p) -> bool {
            return p.first == realm;
        });
        if (itRealm == FailedRealms.end()) {
            itRealm = FailedRealms.insert(FailedRealms.end(), { realm, 1 });
        } else {
            itRealm->second++;
        }
    }

public:
    TGroupChecker(const TString& erasure, const bool layoutCorrect = true)
        : ErasureSpecies(erasure)
        , LayoutCorrect(layoutCorrect)
    {}

    void AddVDiskStatus(Ydb::Monitoring::StatusFlag::Status status, ui32 realm) {
        ++DisksColors[status];
        switch (status) {
            case Ydb::Monitoring::StatusFlag::BLUE: // disk is good, but not available
            // No yellow or orange status here - this is intentional - they are used when a disk is running out of space, but is currently available
            case Ydb::Monitoring::StatusFlag::RED: // disk is bad, probably not available
            case Ydb::Monitoring::StatusFlag::GREY: // the status is absent, the disk is not available
                IncrementFor(realm);
                ++FailedDisks;
                break;
            default:
                break;
        }
    }

    void ReportStatus(TSelfCheckResult& context) const {
        context.OverallStatus = Ydb::Monitoring::StatusFlag::GREEN;
        if (!LayoutCorrect) {
            context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Group layout is incorrect", ETags::GroupState);
        }
        if (ErasureSpecies == NONE) {
            if (FailedDisks > 0) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", ETags::GroupState, {ETags::VDiskState});
            } else if (DisksColors[Ydb::Monitoring::StatusFlag::YELLOW] > 0 || DisksColors[Ydb::Monitoring::StatusFlag::ORANGE] > 0) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", ETags::GroupState, {ETags::VDiskState});
            }
        } else if (ErasureSpecies == BLOCK_4_2) {
            if (FailedDisks > 2) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", ETags::GroupState, {ETags::VDiskState});
            } else if (FailedDisks > 1) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Group has no redundancy", ETags::GroupState, {ETags::VDiskState});
            } else if (FailedDisks > 0) {
                if (DisksColors[Ydb::Monitoring::StatusFlag::BLUE] == FailedDisks) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Group degraded", ETags::GroupState, {ETags::VDiskState});
                } else {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", ETags::GroupState, {ETags::VDiskState});
                }
            } else if (DisksColors[Ydb::Monitoring::StatusFlag::YELLOW] > 0 || DisksColors[Ydb::Monitoring::StatusFlag::ORANGE] > 0) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", ETags::GroupState, {ETags::VDiskState});
            }
        } else if (ErasureSpecies == MIRROR_3_DC) {
            if (FailedRealms.size() > 2 || (FailedRealms.size() == 2 && FailedRealms[0].second > 1 && FailedRealms[1].second > 1)) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::RED, "Group failed", ETags::GroupState, {ETags::VDiskState});
            } else if (FailedRealms.size() == 2) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::ORANGE, "Group has no redundancy", ETags::GroupState, {ETags::VDiskState});
            } else if (FailedDisks > 0) {
                if (DisksColors[Ydb::Monitoring::StatusFlag::BLUE] == FailedDisks) {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::BLUE, "Group degraded", ETags::GroupState, {ETags::VDiskState});
                } else {
                    context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", ETags::GroupState, {ETags::VDiskState});
                }
            } else if (DisksColors[Ydb::Monitoring::StatusFlag::YELLOW] > 0 || DisksColors[Ydb::Monitoring::StatusFlag::ORANGE] > 0) {
                context.ReportStatus(Ydb::Monitoring::StatusFlag::YELLOW, "Group degraded", ETags::GroupState, {ETags::VDiskState});
            }
        }
    }
};

}