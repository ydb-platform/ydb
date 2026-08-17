#include "ddisk_actor.h"

#include <ydb/core/util/stlog.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_DDISK

namespace NKikimr::NDDisk {

    TConnectionToken TDDiskActor::IssueConnectionToken(ui32 connectionIndex, TConnectionInfo& connection) {
        if (++connection.TokenSequenceNo == 0) {
            ++connection.TokenSequenceNo;
        }

        return TConnectionToken::Make(
            connectionIndex,
            connection.TokenSequenceNo,
            static_cast<ui32>(connection.TabletId),
            static_cast<ui16>(BaseInfo.PDiskActorID.NodeId()),
            static_cast<ui16>(BaseInfo.PDiskId),
            static_cast<ui16>(BaseInfo.VDiskSlotId),
            static_cast<ui8>(RandomNumber<ui32>())
        );
    }

    void TDDiskActor::RememberConnectionToken(TConnectionInfo& connection, EConnectionTokenInvalidationReason reason) {
        if (!connection.Token) {
            return;
        }

        ui8 nextIdx = connection.NextPreviousTokenIndex;
        auto& prevTokens = connection.PreviousTokens;
        TPreviousConnectionTokenInfo& previous = prevTokens[nextIdx];
        previous.Token = connection.Token;
        previous.TabletId = connection.TabletId;
        previous.DirectBlockGroupIndex = connection.DirectBlockGroupIndex;
        previous.Generation = connection.Generation;
        previous.DDiskSessionSeqNo = connection.DDiskSessionSeqNo;
        previous.InvalidationReason = reason;
        previous.Valid = true;
        connection.NextPreviousTokenIndex = (nextIdx + 1) % prevTokens.size();
    }

    void TDDiskActor::Handle(TEvConnect::TPtr ev) {
        const auto& record = ev->Get()->Record;
        YDB_LOG_DEBUG("TDDiskActor::Handle(TEvConnect)",
            {"marker", "BSDD00"},
            {"DDiskId", DDiskId},
            {"record", record});

        const TQueryCredentials creds(record.GetCredentials());

        using TCreds = NKikimrBlobStorage::NDDisk::TQueryCredentials;
        auto expectedRequestKind = IsPersistentBufferActor
            ? TCreds::REQUEST_KIND_TO_PERSISTENT_BUFFER
            : TCreds::REQUEST_KIND_TO_DDISK;
        if (creds.RequestKind != expectedRequestKind) {
            SendReply(*ev, std::make_unique<TEvConnectResult>(
                NKikimrBlobStorage::NDDisk::TReplyStatus::INCORRECT_REQUEST,
                TString("connection kind does not match recipient")));
            return;
        }

        TConnectionKey connectionKey{
            creds.TabletId,
            creds.DirectBlockGroupIndex
        };
        auto [it, inserted] = ConnectionIndexBySession.try_emplace(connectionKey);

        if (inserted) {
            if (!FreeConnectionIndices.empty()) {
                it->second = FreeConnectionIndices.back();
                FreeConnectionIndices.pop_back();
            } else {
                Y_ABORT_UNLESS(Connections.size() < Max<ui32>());
                it->second = Connections.size();
                Connections.emplace_back();
            }
        }

        ui32 connectionIndex = it->second;
        TConnectionInfo& connection = Connections[connectionIndex];
        bool sameSession = false;

        if (!inserted && connection.Active) {
            bool obsoleteSession =
                creds.Generation < connection.Generation ||
                (!IsPersistentBufferActor &&
                 creds.Generation == connection.Generation &&
                 creds.DDiskSessionSeqNo < connection.DDiskSessionSeqNo);
            if (obsoleteSession) {
                // this is definitely obsolete tablet/session trying to reach us, reject
                SendReply(*ev, std::make_unique<TEvConnectResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::BLOCKED));
                return;
            }
            sameSession =
                creds.Generation == connection.Generation &&
                (IsPersistentBufferActor || creds.DDiskSessionSeqNo == connection.DDiskSessionSeqNo);
        }

        if (!sameSession && connection.Active) {
            RememberConnectionToken( connection, EConnectionTokenInvalidationReason::Reconnect);
        }

        connection.TabletId = creds.TabletId;
        connection.DirectBlockGroupIndex = creds.DirectBlockGroupIndex;
        connection.Generation = creds.Generation;
        connection.DDiskSessionSeqNo = creds.DDiskSessionSeqNo;
        connection.NodeId = ev->Sender.NodeId();
        connection.InterconnectSessionId = ev->InterconnectSession;
        connection.Active = true;
        if (!sameSession) {
            connection.Token = IssueConnectionToken(connectionIndex, connection);
        }

        YDB_LOG_DEBUG("TDDiskActor::Handle(TEvConnect) sending OK",
            {"marker", "BSDD11"},
            {"DDiskId", DDiskId},
            {"recipient", ev->Sender},
            {"ICSession", ev->InterconnectSession});
        auto result = std::make_unique<TEvConnectResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK, std::nullopt,
                DDiskInstanceGuid, connection.Token);
        SendReply(*ev, std::move(result));

        if (ev->InterconnectSession) {
            // subscribe to session to check for disconnections (if not yet)
        }
    }

    void TDDiskActor::Handle(TEvDisconnect::TPtr ev) {
        if (!CheckQuery(*ev, nullptr)) {
            return;
        }

        const auto& record = ev->Get()->Record;
        TQueryCredentials creds(record.GetCredentials());
        TConnectionKey connectionKey{
            creds.TabletId,
            creds.DirectBlockGroupIndex
        };
        auto &connectionDict = ConnectionIndexBySession;

        if (auto it = connectionDict.find(connectionKey); it != connectionDict.end()) {
            TConnectionInfo& connection = Connections[it->second];
            RememberConnectionToken( connection, EConnectionTokenInvalidationReason::Disconnect);
            connection.Active = false;
            connection.Token = {};
            FreeConnectionIndices.push_back(it->second);
            connectionDict.erase(it);
        }
        SendReply(*ev, std::make_unique<TEvDisconnectResult>(NKikimrBlobStorage::NDDisk::TReplyStatus::OK));
    }

    TDDiskActor::EConnectionResolution TDDiskActor::ResolveConnection(const TQueryCredentials& requestCreds, TQueryCredentials* resolvedCreds) const {
        if (requestCreds.ConnectionToken) {
            ui32 index = requestCreds.ConnectionToken->GetConnectionIndex();
            if (index >= Connections.size()) {
                return EConnectionResolution::InvalidToken;
            }

            const TConnectionInfo* connection = &Connections[index];
            if (!connection->Active || connection->Token != *requestCreds.ConnectionToken) {
                for (const auto& previous : connection->PreviousTokens) {
                    if (previous.Valid && previous.Token == *requestCreds.ConnectionToken) {
                        return EConnectionResolution::StaleToken;
                    }
                }

                return EConnectionResolution::InvalidToken;
            }

            using TCreds = NKikimrBlobStorage::NDDisk::TQueryCredentials;
            auto requestKind = IsPersistentBufferActor
                ? TCreds::REQUEST_KIND_TO_PERSISTENT_BUFFER
                : TCreds::REQUEST_KIND_TO_DDISK;

            *resolvedCreds = TQueryCredentials(
                connection->TabletId,
                connection->Generation,
                connection->DDiskSessionSeqNo,
                DDiskInstanceGuid,
                requestKind,
                connection->DirectBlockGroupIndex
            );
            resolvedCreds->ConnectionToken = requestCreds.ConnectionToken;
            return EConnectionResolution::Resolved;
        }

        if (requestCreds.HasServerContext()) {
            TConnectionKey connectionKey{
                requestCreds.TabletId,
                requestCreds.DirectBlockGroupIndex
            };
            auto it = ConnectionIndexBySession.find(connectionKey);

            if (requestCreds.IsInternal()) {
                // Cross-DDisk fanout may target an actor without a client
                // connection. If the slot exists, preserve the old generation
                // and instance checks.
                if (it != ConnectionIndexBySession.end()) {
                    const TConnectionInfo& connection = Connections[it->second];
                    bool isValid = connection.Active &&
                        connection.Generation == requestCreds.Generation &&
                        (!requestCreds.DDiskInstanceGuid || requestCreds.DDiskInstanceGuid == DDiskInstanceGuid);

                    if (!isValid) {
                        return EConnectionResolution::InvalidToken;
                    }
                }
            } else {
                // A token-resolved request may re-enter the production
                // executor. Accept its server context only for the exact slot
                // from which it was restored.

                using TCreds = NKikimrBlobStorage::NDDisk::TQueryCredentials;
                auto expectedRequestKind = IsPersistentBufferActor
                    ? TCreds::REQUEST_KIND_TO_PERSISTENT_BUFFER
                    : TCreds::REQUEST_KIND_TO_DDISK;

                if (requestCreds.RequestKind != expectedRequestKind || it == ConnectionIndexBySession.end()) {
                    return EConnectionResolution::InvalidToken;
                }

                const TConnectionInfo& connection = Connections[it->second];
                bool isValid = connection.Active &&
                    connection.Generation == requestCreds.Generation &&
                    connection.DDiskSessionSeqNo == requestCreds.DDiskSessionSeqNo &&
                    (!requestCreds.DDiskInstanceGuid || requestCreds.DDiskInstanceGuid == DDiskInstanceGuid);

                if (!isValid) {
                    return EConnectionResolution::InvalidToken;
                }
            }

            *resolvedCreds = requestCreds;
            return EConnectionResolution::Resolved;
        }

        return EConnectionResolution::InvalidToken;
    }

    TStringBuf TDDiskActor::ConnectionErrorReason(EConnectionResolution resolution) {
        switch (resolution) {
            case EConnectionResolution::Resolved:
                return {};
            case EConnectionResolution::StaleToken:
                return "stale connection token";
            case EConnectionResolution::InvalidToken:
                return "invalid connection token";
        }
        Y_UNREACHABLE();
    }

    TStringBuf TDDiskActor::ConnectionInvalidationReason(EConnectionTokenInvalidationReason reason) {
        switch (reason) {
            case EConnectionTokenInvalidationReason::Reconnect:
                return "reconnect";
            case EConnectionTokenInvalidationReason::Disconnect:
                return "disconnect";
        }
        Y_UNREACHABLE();
    }

    TString TDDiskActor::DescribeConnectionFailure(const TQueryCredentials& requestCreds, EConnectionResolution resolution) const {
        TStringBuilder reason;
        reason << ConnectionErrorReason(resolution);

        if (!requestCreds.ConnectionToken) {
            return reason;
        }

        const TConnectionToken& token = *requestCreds.ConnectionToken;
        const ui32 index = token.GetConnectionIndex();
        reason
            << " claimedConnectionIndex# " << index
            << " claimedSequenceNo# " << static_cast<ui32>(token.GetSequenceNo())
            << " claimedTabletIdSuffix# " << token.GetTabletIdSuffix()
            << " claimedNodeId# " << token.GetNodeId()
            << " claimedPDiskId# " << token.GetPDiskId()
            << " claimedVSlotId# " << token.GetVSlotId();
        if (index >= Connections.size()) {
            reason << " slot# out-of-range";
            return reason;
        }

        const TConnectionInfo& connection = Connections[index];
        reason
            << " slotActive# " << connection.Active
            << " slotTabletId# " << connection.TabletId
            << " slotDirectBlockGroupIndex# " << connection.DirectBlockGroupIndex
            << " slotGeneration# " << connection.Generation
            << " slotDDiskSessionSeqNo# " << connection.DDiskSessionSeqNo
            << " slotTokenSequenceNo# " << static_cast<ui32>(connection.TokenSequenceNo)
            << " slotNodeId# " << connection.NodeId
            << " slotICSession# " << connection.InterconnectSessionId;

        if (resolution == EConnectionResolution::StaleToken) {
            for (const TPreviousConnectionTokenInfo& previous : connection.PreviousTokens) {
                if (previous.Valid && previous.Token == token) {
                    reason
                        << " previousTabletId# " << previous.TabletId
                        << " previousDirectBlockGroupIndex# " << previous.DirectBlockGroupIndex
                        << " previousGeneration# " << previous.Generation
                        << " previousDDiskSessionSeqNo# " << previous.DDiskSessionSeqNo
                        << " invalidatedBy# " << ConnectionInvalidationReason(previous.InvalidationReason);

                    break;
                }
            }
        }

        return reason;
    }

    void TDDiskActor::SendReply(const IEventHandle& queryEv, std::unique_ptr<IEventBase> replyEv) const {
        auto h = std::make_unique<IEventHandle>(queryEv.Sender, SelfId(), replyEv.release(), 0, queryEv.Cookie);
        if (queryEv.InterconnectSession) {
            h->Rewrite(TEvInterconnect::EvForward, queryEv.InterconnectSession);
        }
        TActivationContext::Send(h.release());
    }

} // NKikimr::NDDisk
