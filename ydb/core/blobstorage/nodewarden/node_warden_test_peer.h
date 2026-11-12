#pragma once
#include "node_warden_impl.h"

namespace NKikimr {
namespace NStorage {
class TNodeWardenTestPeer {
public:
    using TRestartDrainReminder = TNodeWarden::TEvPrivate::TEvRestartDrainReminder;
    static auto& AddPDisk(TNodeWarden& warden, ui32 pdiskId) {
        NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk record;
        return warden.LocalPDisks.emplace(TPDiskKey(1, pdiskId), TPDiskRecord(std::move(record))).first->second.Record;
    }
    static void TrackPath(TNodeWarden& warden, ui32 pdiskId) {
        const auto& record = warden.LocalPDisks.at(TPDiskKey(1, pdiskId)).Record;
        warden.PDiskByPath.emplace(record.GetPath(), TNodeWarden::TPDiskByPathInfo{.RunningPDiskId = TPDiskKey(1, pdiskId)});
    }
    static const auto& GetPDisk(TNodeWarden& warden, ui32 pdiskId) {
        return warden.LocalPDisks.at(TPDiskKey(1, pdiskId)).Record;
    }
    static ui32 GetPDiskRefCount(const TNodeWarden& warden, ui32 pdiskId) {
        return warden.LocalPDisks.at(TPDiskKey(1, pdiskId)).RefCount;
    }

    static void InitializeReplQuoters(TNodeWarden& warden, ui64 bytesPerSecond) {
        warden.ReplNodeRequestQuoter = std::make_shared<TReplQuoter>(bytesPerSecond);
        warden.ReplNodeResponseQuoter = std::make_shared<TReplQuoter>(bytesPerSecond);
    }

    static void AddReplPDisk(TNodeWarden& warden, ui32 pdiskId, NPDisk::EDeviceType type, ui64 bytesPerSecond) {
        auto& record = warden.LocalPDisks.emplace(TPDiskKey(1, pdiskId),
            TPDiskRecord(NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk{})).first->second;
        record.Record.SetPDiskCategory(TPDiskCategory(type, 0).GetRaw());
        record.ReplPDiskReadQuoter = std::make_shared<TReplQuoter>(bytesPerSecond);
        record.ReplPDiskWriteQuoter = std::make_shared<TReplQuoter>(bytesPerSecond);
    }

    static void ApplyReplBrokerConfig(TNodeWarden& warden,
            const NKikimrBlobStorage::TNodeWardenServiceSet::TReplBrokerConfig& config) {
        warden.ApplyReplBrokerConfig(config);
    }

    static const TReplQuoter::TPtr& GetReplNodeRequestQuoter(const TNodeWarden& warden) {
        return warden.ReplNodeRequestQuoter;
    }

    static const TReplQuoter::TPtr& GetReplNodeResponseQuoter(const TNodeWarden& warden) {
        return warden.ReplNodeResponseQuoter;
    }

    static const TReplQuoter::TPtr& GetReplPDiskReadQuoter(const TNodeWarden& warden, ui32 pdiskId) {
        return warden.LocalPDisks.at(TPDiskKey(1, pdiskId)).ReplPDiskReadQuoter;
    }

    static const TReplQuoter::TPtr& GetReplPDiskWriteQuoter(const TNodeWarden& warden, ui32 pdiskId) {
        return warden.LocalPDisks.at(TPDiskKey(1, pdiskId)).ReplPDiskWriteQuoter;
    }
};
}

}
