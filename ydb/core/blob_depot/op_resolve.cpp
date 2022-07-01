#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvResolve::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT07, "TEvResolve", (TabletId, TabletID()), (Msg, ev->Get()->ToString()),
            (Sender, ev->Sender), (Recipient, ev->Recipient), (Cookie, ev->Cookie));

        // collect records if needed

        auto response = std::make_unique<TEvBlobDepot::TEvResolveResult>(NKikimrProto::OK, std::nullopt);
        ui32 messageSize = response->CalculateSerializedSize();
        auto sendMessage = [&](bool more) {
            if (more) {
                response->Record.SetStatus(NKikimrProto::OVERRUN);
            }

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT08, "sending TEvResolveResult", (TabletId, TabletID()), (Msg, response->Record));

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
            std::optional<TStringBuf> begin = item.HasBeginningKey() ? std::make_optional(item.GetBeginningKey()) : std::nullopt;
            std::optional<TStringBuf> end = item.HasEndingKey() ? std::make_optional(item.GetEndingKey()) : std::nullopt;

            ui32 numItems = 0;
            auto addKey = [&](TStringBuf key, const TDataValue& value) {
                NKikimrBlobDepot::TEvResolveResult::TResolvedKey resolvedKey;
                resolvedKey.SetItemIndex(itemIndex);
                resolvedKey.SetKey(key.data(), key.size());
                resolvedKey.MutableValueChain()->CopyFrom(value.ValueChain);

                if (value.Meta) {
                    resolvedKey.SetMeta(value.Meta.data(), value.Meta.size());
                }

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

            TScanFlags flags;
            if (item.GetIncludeBeginning()) {
                flags |= EScanFlags::INCLUDE_BEGIN;
            }
            if (item.GetIncludeEnding()) {
                flags |= EScanFlags::INCLUDE_END;
            }
            if (item.GetReverse()) {
                flags |= EScanFlags::REVERSE;
            }

            ScanRange(begin, end, flags, addKey);

            ++itemIndex;
        }

        sendMessage(false);
    }

} // NKikimr::NBlobDepot
