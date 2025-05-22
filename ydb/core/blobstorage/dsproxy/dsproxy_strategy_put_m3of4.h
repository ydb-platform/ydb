#pragma once

#include "defs.h"
#include "dsproxy_strategy_m3of4_base.h"

namespace NKikimr {

class TPut3of4Strategy : public TMirror3of4StrategyBase {
    // TODO(alexvru): use Accelerate property somehow
    const TEvBlobStorage::TEvPut::ETactic Tactic;

public:
    TPut3of4Strategy(TEvBlobStorage::TEvPut::ETactic tactic, bool /*accelerate*/ = false)
        : Tactic(tactic)
    {}

    EStrategyOutcome Process(TLogContext& /*logCtx*/, TBlobState& state, const TBlobStorageGroupInfo& info,
            TBlackboard& /*blackboard*/, TGroupDiskRequests& groupDiskRequests,
            const TAccelerationParams& accelerationParams) override {
        Y_UNUSED(accelerationParams);
        if (!CheckFailModel(state, info)) {
            state.WholeSituation = TBlobState::ESituation::Error;
            return EStrategyOutcome::Error("failure model exceeded");
        }

        if (state.WholeSituation != TBlobState::ESituation::Unknown && state.WholeSituation != TBlobState::ESituation::Present) {
            return EStrategyOutcome::DONE; // most likely this is get-with-restore put and the blob is absent
        }

        switch (Tactic) {
            case TEvBlobStorage::TEvPut::TacticMaxThroughput:
                return Process(state, info, groupDiskRequests, false);

            case TEvBlobStorage::TEvPut::TacticMinLatency:
            case TEvBlobStorage::TEvPut::TacticDefault:
                return Process(state, info, groupDiskRequests, true);

            default:
                Y_ABORT("unexpected Tactic");
        }
    }

protected:
    TRope GetDataBuffer(TBlobState& state, const TBlobStorageGroupInfo& info) {
        for (ui32 i = 0; i < info.Type.TotalPartCount(); ++i) {
            if (info.Type.PartSize(TLogoBlobID(state.Id, i + 1)) && state.Parts[i].Data.IsMonolith()) {
                return state.Parts[i].Data.GetMonolith();
            }
        }
        const TIntervalVec<i32> interval(0, state.Id.BlobSize());
        Y_ABORT_UNLESS(interval.IsSubsetOf(state.Whole.Here()), "missing blob data State# %s", state.ToString().data());
        std::array<TRope, 3> parts;
        ErasureSplit((TErasureType::ECrcMode)state.Id.CrcMode(), info.Type,
            state.Whole.Data.Read(0, state.Id.BlobSize()), parts);
        state.Parts[0].Data.SetMonolith(std::move(parts[0]));
        return parts[1]; // must be the same as parts[0]
    }

    EStrategyOutcome Process(TBlobState& state, const TBlobStorageGroupInfo& info, TGroupDiskRequests& groupDiskRequests, bool minLatency) {
        // first of all, look around
        TBlobStorageGroupInfo::TSubgroupVDisks error(&info.GetTopology());
        TBlobStorageGroupInfo::TSubgroupVDisks dataPresent(&info.GetTopology());
        TBlobStorageGroupInfo::TSubgroupVDisks anyPresent(&info.GetTopology());
        TBlobStorageGroupInfo::TSubgroupVDisks data(&info.GetTopology());
        TBlobStorageGroupInfo::TSubgroupVDisks any(&info.GetTopology());
        for (ui8 diskIdx = 0; diskIdx < info.Type.BlobSubgroupSize(); ++diskIdx) {
            const TBlobStorageGroupInfo::TSubgroupVDisks mask(&info.GetTopology(), diskIdx);
            for (ui8 partIdx = 0; partIdx < info.Type.TotalPartCount(); ++partIdx) {
                const bool dataPart = info.Type.PartSize(TLogoBlobID(state.Id, partIdx + 1));
                switch (state.Disks[diskIdx].DiskParts[partIdx].Situation) {
                    case TBlobState::ESituation::Error:
                        error |= {&info.GetTopology(), diskIdx};
                        break;

                    case TBlobState::ESituation::Present:
                        anyPresent |= mask;
                        if (dataPart) {
                            dataPresent |= mask;
                        }
                        [[fallthrough]];

                    case TBlobState::ESituation::Sent:
                        any |= mask;
                        if (dataPart) {
                            data |= mask;
                        }
                        break;

                    case TBlobState::ESituation::Unknown:
                    case TBlobState::ESituation::Absent:
                    case TBlobState::ESituation::Lost:
                        break;
                }
            }
        }
        if (dataPresent.GetNumSetItems() >= 3 && anyPresent.GetNumSetItems() >= 5) {
            state.WholeSituation = TBlobState::ESituation::Present;
            return EStrategyOutcome::DONE;
        }

        TGroups groups;
        const ui32 requiredNumDataParts = minLatency ? 4 : 3;
        const ui32 requiredNumMetadataParts = minLatency ? 4 : 2;
        const ui32 requiredNumParts = requiredNumDataParts + requiredNumMetadataParts;

        for (bool ignoreSlowDisks : {true, false}) {
            for (bool considerLost : {true, false}) {
                // fix the data part of the subgroup
                for (auto& group : groups.Groups) {
                    for (ui8 diskIdx : group.DiskIdx) {
                        if (data.GetNumSetItems() >= requiredNumDataParts || dataPresent.GetNumSetItems() >= 3) {
                            break; // we already have required set of data replicas
                        }
                        auto& disk = state.Disks[diskIdx];
                        if (ignoreSlowDisks && disk.IsSlow) {
                            continue;
                        }
                        auto& part = disk.DiskParts[group.PartIdx];
                        switch (auto& s = part.Situation) {
                            case TBlobState::ESituation::Error:
                            case TBlobState::ESituation::Present:
                            case TBlobState::ESituation::Sent:
                                break; // this part/disk is ignored for now

                            case TBlobState::ESituation::Unknown:
                            case TBlobState::ESituation::Absent:
                            case TBlobState::ESituation::Lost: {
                                // look for the data counterpart situation -- can we use this part to store data?
                                using E = TBlobState::ESituation;
                                const auto cs = disk.DiskParts[!group.PartIdx].Situation;
                                if (cs == E::Error || cs == E::Present || cs == E::Sent) {
                                    break; // we should not fill in this part
                                }
                                if (considerLost && s != TBlobState::ESituation::Lost) {
                                    break; // we are looking for lost parts now -- this is not the lost one
                                }
                                // send request to this disk
                                groupDiskRequests.AddPut(disk.OrderNumber,
                                    TLogoBlobID(state.Id, group.PartIdx + 1),
                                    GetDataBuffer(state, info),
                                    diskIdx == group.DiskIdx[0] ? TDiskPutRequest::ReasonInitial : TDiskPutRequest::ReasonError,
                                    diskIdx != group.DiskIdx[0],
                                    state.BlobIdx);
                                s = TBlobState::ESituation::Sent;
                                any |= {&info.GetTopology(), diskIdx};
                                data += {&info.GetTopology(), diskIdx};
                                break;
                            }
                        }
                    }
                }

                // then we restore required amount of metadata parts
                for (i8 diskIdx = info.Type.BlobSubgroupSize() - 1; diskIdx >= 0 &&
                        any.GetNumSetItems() < requiredNumParts && anyPresent.GetNumSetItems() < 5; --diskIdx) {
                    if (any[diskIdx] || error[diskIdx]) {
                        continue; // we do not put metadata parts on disk already containing data or metadata parts
                    }
                    auto& disk = state.Disks[diskIdx];
                    if (ignoreSlowDisks && disk.IsSlow) {
                        continue;
                    }
                    auto& part = disk.DiskParts[2];
                    Y_ABORT_UNLESS(part.Situation != TBlobState::ESituation::Present && part.Situation != TBlobState::ESituation::Sent);
                    if (considerLost && part.Situation != TBlobState::ESituation::Lost) {
                        continue; // here we process only lost disks
                    }

                    const bool handoff = (ui8)diskIdx < info.Type.BlobSubgroupSize() - requiredNumMetadataParts;
                    groupDiskRequests.AddPut(disk.OrderNumber,
                        TLogoBlobID(state.Id, 3),
                        TRope(TString()),
                        handoff ? TDiskPutRequest::ReasonError : TDiskPutRequest::ReasonInitial,
                        handoff,
                        state.BlobIdx);
                    part.Situation = TBlobState::ESituation::Sent;
                    any |= {&info.GetTopology(), (ui8)diskIdx};
                }
            }
        }

        Y_ABORT_UNLESS(anyPresent != any || dataPresent != data, "state# %s", state.ToString().data());
        return EStrategyOutcome::IN_PROGRESS;
    }
};

} // NKikimr
