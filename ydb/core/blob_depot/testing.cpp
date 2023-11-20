#include "testing.h"
#include "blob_depot_tablet.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    bool IsBlobDepotActor(IActor *actor) {
        return dynamic_cast<TBlobDepot*>(actor);
    }

    void ValidateBlobDepot(IActor *actor, NTesting::TGroupOverseer& overseer) {
        if (auto *x = dynamic_cast<TBlobDepot*>(actor)) {
            x->Validate(overseer);
        } else {
            Y_ABORT();
        }
    }

    void OnSuccessfulGetResult(IActor *actor, TLogoBlobID id) {
        if (auto *x = dynamic_cast<TBlobDepot*>(actor)) {
            x->OnSuccessfulGetResult(id);
        } else {
            Y_ABORT();
        }
    }

    void TBlobDepot::Validate(NTesting::TGroupOverseer& overseer) const {
        Y_ABORT_UNLESS(Config.HasVirtualGroupId());
        overseer.EnumerateBlobs(Config.GetVirtualGroupId(), [&](TLogoBlobID userId, NTesting::EBlobState userState) {
            switch (userState) {
                case NTesting::EBlobState::NOT_WRITTEN:
                case NTesting::EBlobState::CERTAINLY_COLLECTED_OR_NEVER_WRITTEN:
                    Y_ABORT();

                case NTesting::EBlobState::POSSIBLY_WRITTEN:
                    break;

                case NTesting::EBlobState::CERTAINLY_WRITTEN: {
                    Cerr << userId.ToString() << Endl;
                    const TData::TKey key(userId);
                    const TData::TValue *value = Data->FindKey(key);
                    Y_ABORT_UNLESS(value); // key must exist
                    ui32 numDataBytes = 0;
                    EnumerateBlobsForValueChain(value->ValueChain, TabletID(), [&](TLogoBlobID id, ui32, ui32 size) {
                        const ui32 groupId = Info()->GroupFor(id.Channel(), id.Generation());
                        const auto state = overseer.GetBlobState(groupId, id);
                        Y_VERIFY_S(state == NTesting::EBlobState::CERTAINLY_WRITTEN,
                            "UserId# " << userId.ToString() << " UserState# " << (int)userState
                            << " Id# " << id.ToString() << " State# " << (int)state);
                        numDataBytes += size;
                    });
                    Y_ABORT_UNLESS(numDataBytes == userId.BlobSize());
                    break;
                }

                case NTesting::EBlobState::POSSIBLY_COLLECTED:
                    break;
            }
        });
    }

    void TBlobDepot::OnSuccessfulGetResult(TLogoBlobID id) const {
        (void)id; // FIXME(alexvru): handle race with blob deletion
    }

} // NKikimr::NBlobDepot
