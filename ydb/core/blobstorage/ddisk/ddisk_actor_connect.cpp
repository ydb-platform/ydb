#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr::NDDisk {

    void TDDiskActor::Handle(TEvConnect::TPtr ev) {
        const auto& record = ev->Get()->Record;

        STLOG(PRI_DEBUG, BS_DDISK, BSDD00, "TDDiskActor::Handle(TEvConnect)", (DDiskId, DDiskId),
            (Record, record));

        const TQueryCredentials creds(record.GetCredentials());
        const auto [it, inserted] = Connections.try_emplace(creds.TabletId);
        TConnectionInfo& connection = it->second;

        if (!inserted) {
            if (creds.Generation < connection.Generation) {
                // this is definitely obsolete tablet trying to reach us, reject
                SendReply(*ev, std::make_unique<TEvConnectResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::BLOCKED));
                return;
            } else if (connection.Generation < creds.Generation) {
                // drop connection with previous tablet
            }
        }

        connection.TabletId = creds.TabletId;
        connection.Generation = creds.Generation;
        connection.NodeId = ev->Sender.NodeId();
        connection.InterconnectSessionId = ev->InterconnectSession;

        SendReply(*ev, std::make_unique<TEvConnectResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt,
            DDiskInstanceGuid));

        if (ev->InterconnectSession) {
            // subscribe to session to check for disconnections (if not yet)
        }
    }

    void TDDiskActor::Handle(TEvDisconnect::TPtr ev) {
        if (!CheckQuery(*ev, nullptr)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        const TQueryCredentials creds(record.GetCredentials());
        Connections.erase(creds.TabletId);
        SendReply(*ev, std::make_unique<TEvDisconnectResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
    }

    bool TDDiskActor::ValidateConnection(const IEventHandle& ev, const TQueryCredentials& creds) const {
        const auto it = Connections.find(creds.TabletId);
        if (it == Connections.end()) {
            return false;
        }
        const TConnectionInfo& connection = it->second;
        return connection.TabletId == creds.TabletId &&
            connection.Generation == creds.Generation &&
            creds.DDiskInstanceGuid == DDiskInstanceGuid && (creds.FromPersistentBuffer || (
            connection.NodeId == ev.Sender.NodeId() &&
            connection.InterconnectSessionId == ev.InterconnectSession));
    }

    void TDDiskActor::SendReply(const IEventHandle& queryEv, std::unique_ptr<IEventBase> replyEv) const {
        auto h = std::make_unique<IEventHandle>(queryEv.Sender, SelfId(), replyEv.release(), 0, queryEv.Cookie);
        if (queryEv.InterconnectSession) {
            h->Rewrite(TEvInterconnect::EvForward, queryEv.InterconnectSession);
        }
        TActivationContext::Send(h.release());
    }

} // NKikimr::NDDisk
