#include "blob_depot_tablet.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvBlobDepot::TEvResolve::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT17, "TEvResolve", (TabletId, TabletID()), (Msg, ev->Get()->ToString()),
            (Sender, ev->Sender), (Recipient, ev->Recipient), (Cookie, ev->Cookie));

        // collect records if needed

        auto response = std::make_unique<TEvBlobDepot::TEvResolveResult>(NKikimrProto::OK, std::nullopt);
        ui32 messageSize = response->CalculateSerializedSize();
        auto sendMessage = [&](bool more) {
            if (more) {
                response->Record.SetStatus(NKikimrProto::OVERRUN);
            }

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT18, "sending TEvResolveResult", (TabletId, TabletID()), (Msg, response->Record));

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
            TData::TKey begin;
            TData::TKey end;

            if (item.HasBeginningKey()) {
                begin = TData::TKey::FromBinaryKey(item.GetBeginningKey(), Config);
            }
            if (item.HasEndingKey()) {
                end = TData::TKey::FromBinaryKey(item.GetEndingKey(), Config);
            }

            ui32 numItems = 0;
            auto addKey = [&](const TData::TKey& key, const TData::TValue& value) {
                NKikimrBlobDepot::TEvResolveResult::TResolvedKey resolvedKey;
                resolvedKey.SetItemIndex(itemIndex);
                resolvedKey.SetKey(key.MakeBinaryKey());
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

            TData::TScanFlags flags;
            if (item.GetIncludeBeginning()) {
                flags |= TData::EScanFlags::INCLUDE_BEGIN;
            }
            if (item.GetIncludeEnding()) {
                flags |= TData::EScanFlags::INCLUDE_END;
            }
            if (item.GetReverse()) {
                flags |= TData::EScanFlags::REVERSE;
            }

            Data->ScanRange(item.HasBeginningKey() ? &begin : nullptr, item.HasEndingKey() ? &end : nullptr, flags, addKey);

            ++itemIndex;
        }

        sendMessage(false);
    }

} // NKikimr::NBlobDepot
