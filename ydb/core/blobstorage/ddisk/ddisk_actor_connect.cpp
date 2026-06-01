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
            const bool obsoleteSession =
                creds.Generation < connection.Generation ||
                (creds.RequiresDDiskSessionSeqNoCheck() &&
                 creds.Generation == connection.Generation &&
                 creds.DDiskSessionSeqNo < connection.DDiskSessionSeqNo);
            if (obsoleteSession) {
                // this is definitely obsolete tablet/session trying to reach us, reject
                SendReply(*ev, std::make_unique<TEvConnectResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::BLOCKED));
                return;
            }
        }

        connection.TabletId = creds.TabletId;
        connection.Generation = creds.Generation;
        connection.DDiskSessionSeqNo = creds.DDiskSessionSeqNo;
        connection.NodeId = ev->Sender.NodeId();
        connection.InterconnectSessionId = ev->InterconnectSession;

        STLOG(PRI_DEBUG, BS_DDISK, BSDD11, "TDDiskActor::Handle(TEvConnect) sending OK",
            (DDiskId, DDiskId), (Recipient, ev->Sender), (ICSession, ev->InterconnectSession));
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
            return creds.IsInternal();
        }
        const TConnectionInfo& connection = it->second;
        return connection.TabletId == creds.TabletId &&
            connection.Generation == creds.Generation &&
            (!creds.RequiresDDiskSessionSeqNoCheck() || connection.DDiskSessionSeqNo == creds.DDiskSessionSeqNo) &&
            (!creds.DDiskInstanceGuid || creds.DDiskInstanceGuid == DDiskInstanceGuid) &&
            (!creds.RequiresSenderCheck() || connection.NodeId == ev.Sender.NodeId()) &&
            (!creds.RequiresInterconnectSessionCheck() || connection.InterconnectSessionId == ev.InterconnectSession);
    }

    void TDDiskActor::SendReply(const IEventHandle& queryEv, std::unique_ptr<IEventBase> replyEv) const {
        auto h = std::make_unique<IEventHandle>(queryEv.Sender, SelfId(), replyEv.release(), 0, queryEv.Cookie);
        if (queryEv.InterconnectSession) {
            h->Rewrite(TEvInterconnect::EvForward, queryEv.InterconnectSession);
        }
        TActivationContext::Send(h.release());
    }

} // NKikimr::NDDisk
