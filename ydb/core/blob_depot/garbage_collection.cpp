#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TGarbageCollectionManager {
        TBlobDepot *Self;

    public:
        TGarbageCollectionManager(TBlobDepot *self)
            : Self(self)
        {}

        void Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev) {
            std::vector<std::pair<std::map<TString, TDataValue>::iterator, EKeepState>> updates;

            const auto& record = ev->Get()->Record;
            auto processFlags = [&](const auto& items, EKeepState state) {
                for (const NKikimrProto::TLogoBlobID& item : items) {
                    const TLogoBlobID id = LogoBlobIDFromLogoBlobID(item);
                    const TString key(reinterpret_cast<const char*>(id.GetRaw()), 3 * sizeof(ui64));
                    if (const auto it = Self->Data.find(key); it == Self->Data.end()) {
                        if (state == EKeepState::Keep) {
                            STLOG(PRI_CRIT, BLOB_DEPOT, BDT05, "received Keep on nonexistent blob",
                                (TabletId, Self->TabletID()), (BlobId, id.ToString()));
                            return false; // we can't allow Keep on nonexistent blobs
                        }
                    } else if (it->second.KeepState < state) {
                        updates.emplace_back(it, state);
                    }
                }
                return true;
            };

            const bool success = processFlags(record.GetKeep(), EKeepState::Keep) &&
                processFlags(record.GetDoNotKeep(), EKeepState::DoNotKeep);
            if (!success) {
                auto [response, _] = TEvBlobDepot::MakeResponseFor(ev, Self->SelfId(), NKikimrProto::ERROR,
                    "missing key for Keep/DoNotKeep items");
                TActivationContext::Send(response.release());
                return;
            }

            auto [response, _] = TEvBlobDepot::MakeResponseFor(ev, Self->SelfId(), NKikimrProto::OK, std::nullopt);
            TActivationContext::Send(response.release());
        }
    };

    TBlobDepot::TGarbageCollectionManagerPtr TBlobDepot::CreateGarbageCollectionManager() {
        return TGarbageCollectionManagerPtr(new TGarbageCollectionManager(this));
    }

    void TBlobDepot::TGarbageCollectionManagerDeleter::operator ()(TGarbageCollectionManager *object) const {
        delete object;
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev) {
        GarbageCollectionManager->Handle(ev);
    }

} // NKikimr::NBlobDepot
