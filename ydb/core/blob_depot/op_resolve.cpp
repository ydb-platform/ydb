#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvResolve::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDR01, "TEvResolve", (TabletId, TabletID()), (Msg, ev->Get()->ToString()),
            (Sender, ev->Sender), (Recipient, ev->Recipient), (Cookie, ev->Cookie));

        // collect records if needed

        auto response = std::make_unique<TEvBlobDepot::TEvResolveResult>(NKikimrProto::OK, std::nullopt);
        ui32 messageSize = response->CalculateSerializedSize();
        auto sendMessage = [&](bool more) {
            if (more) {
                response->Record.SetStatus(NKikimrProto::OVERRUN);
            }

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDR02, "sending TEvResolveResult", (TabletId, TabletID()), (Msg, response->Record));

            auto handle = std::make_unique<IEventHandle>(ev->Sender, SelfId(), response.release(), 0, ev->Cookie);
            if (ev->InterconnectSession) {
                handle->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
            }
            TActivationContext::Send(handle.release());

            if (more) {
                response = std::make_unique<TEvBlobDepot::TEvResolveResult>(NKikimrProto::OK, std::nullopt);
                messageSize = response->CalculateSerializedSize();
            }
        };

        ui32 itemIndex = 0;
        for (const auto& item : ev->Get()->Record.GetItems()) {
            auto beginIt = !item.HasBeginningKey() ? Data.begin()
                : item.GetIncludeBeginning() ? Data.lower_bound(item.GetBeginningKey())
                : Data.upper_bound(item.GetBeginningKey());

            auto endIt = !item.HasEndingKey() ? Data.end()
                : item.GetIncludeEnding() ? Data.upper_bound(item.GetEndingKey())
                : Data.lower_bound(item.GetEndingKey());

            ui32 numItems = 0;
            auto addKey = [&](auto it) {
                NKikimrBlobDepot::TEvResolveResult::TResolvedKey resolvedKey;
                resolvedKey.SetItemIndex(itemIndex);
                resolvedKey.SetKey(it->first);

                const ui32 keySize = resolvedKey.ByteSizeLong();
                if (messageSize + keySize > EventMaxByteSize) {
                    sendMessage(true);
                }

                // put resolved key into the result
                resolvedKey.Swap(response->Record.AddResolvedKeys());
                messageSize += keySize;
                ++numItems;

                return !item.HasMaxKeys() || numItems != item.GetMaxKeys();
            };

            if (item.GetReverse()) {
                while (beginIt != endIt && addKey(--endIt)) {}
            } else {
                while (beginIt != endIt && addKey(beginIt++)) {}
            }

            ++itemIndex;
        }

        sendMessage(false);
    }

} // NKikimr::NBlobDepot
