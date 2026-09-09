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
        warden.PDiskByPath.emplace(record.GetPath(), TNodeWarden::TPDiskByPathInfo{TPDiskKey(1, pdiskId), {}});
    }
    static const auto& GetPDisk(TNodeWarden& warden, ui32 pdiskId) {
        return warden.LocalPDisks.at(TPDiskKey(1, pdiskId)).Record;
    }
};
}

}
