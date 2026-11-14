#include "blobstorage_syncstate.h"
#include <ydb/core/protos/blobstorage.pb.h>
#include <util/stream/input.h>
#include <util/stream/str.h>

Y_DECLARE_OUT_SPEC(, NKikimr::TSyncState, stream, value) {
    value.Output(stream);
}

namespace NKikimr {

void TSyncState::Serialize(IOutputStream &s) const {
    s.Write(&Guid, sizeof(Guid));
    s.Write(&SyncedLsn, sizeof(SyncedLsn));
}

bool TSyncState::Deserialize(IInputStream &s) {
    if (s.Load(&Guid, sizeof(Guid)) != sizeof(Guid))
        return false;
    if (s.Load(&SyncedLsn, sizeof(SyncedLsn)) != sizeof(SyncedLsn))
        return false;
    return true;
}

void TSyncState::Output(IOutputStream &str) const {
    str << "[" << Guid << " " << SyncedLsn << "]";
}

TString TSyncState::ToString() const {
    TStringStream str;
    Output(str);
    return str.Str();
}

TSyncState SyncStateFromSyncState(const NKikimrBlobStorage::TSyncState &x) {
    return TSyncState(x.GetGuid(), x.GetSyncedLsn());
}

void SyncStateFromSyncState(const TSyncState &state, NKikimrBlobStorage::TSyncState *proto) {
    proto->SetGuid(state.Guid);
    proto->SetSyncedLsn(state.SyncedLsn);
}

} // namespace NKikimr
