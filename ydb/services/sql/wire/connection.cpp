#include "connection.h"

#include "catalog.h"
#include "protocol.h"

#include <ydb/services/sql/ddl/events.h>
#include <ydb/services/sql/parse/parser.h>

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NSql {

namespace {

class TSqlConnection: public NActors::TActorBootstrapped<TSqlConnection> {
public:
    TSqlConnection(const NActors::TActorId& listenerActorId,
                   TIntrusivePtr<NRawSocket::TSocketDescriptor> socket,
                   NRawSocket::TNetworkConfig::TSocketAddressType address)
        : ListenerActorId(listenerActorId)
        , Socket(std::move(socket))
        , Address(std::move(address))
    {
    }

    void Bootstrap() {
        LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::PGWIRE,
            "sql connection accepted from " << Address << " by " << ListenerActorId);
        Socket->SetNonBlock();
        Become(&TSqlConnection::StateStartup);
    }

private:
    void RequestRead() {
        if (PollerToken) {
            PollerToken->Request(true, !Outbox.empty());
        }
    }

    bool Write(TStringBuf data) {
        while (!data.empty()) {
            const ssize_t written = Socket->Send(data.data(), data.size());
            if (written > 0) {
                data.Skip(written);
                continue;
            }
            if (written == -EAGAIN || written == -EWOULDBLOCK) {
                Outbox.append(data);
                return true;
            }
            PassAway();
            return false;
        }
        return true;
    }

    void Flush() {
        TStringBuf data = Outbox;
        while (!data.empty()) {
            const ssize_t written = Socket->Send(data.data(), data.size());
            if (written > 0) {
                data.Skip(written);
                continue;
            }
            if (written == -EAGAIN || written == -EWOULDBLOCK) {
                Outbox = TString(data);
                RequestRead();
                return;
            }
            PassAway();
            return;
        }
        Outbox.clear();
    }

    void Fail(TStringBuf sqlState, TStringBuf message) {
        Write(MakeErrorResponse(sqlState, message));
        CloseAfterWrite = true;
        if (Outbox.empty()) {
            PassAway();
        }
    }

    void ReadSocket() {
        char chunk[4096];
        for (;;) {
            const ssize_t received = Socket->Receive(chunk, sizeof(chunk));
            if (received > 0) {
                Inbox.append(chunk, received);
                continue;
            }
            if (received == 0) {
                PassAway();
                return;
            }
            if (received == -EAGAIN || received == -EWOULDBLOCK) {
                break;
            }
            PassAway();
            return;
        }
        if (CurrentStateFunc() == &TSqlConnection::StateStartup) {
            if (HandleInbox()) {
                RequestRead();
            }
            return;
        }
        if (CurrentStateFunc() == &TSqlConnection::StateScheme) {
            RequestRead();
            return;
        }
        if (CurrentStateFunc() == &TSqlConnection::StateQuery && HandleQueries()) {
            RequestRead();
        }
    }

    bool HandleInbox() {
        TString error;
        TStartupMessage message;
        if (!TryParseStartup(Inbox, message, error)) {
            return true;
        }
        if (error) {
            Fail("08P01", error);
            return false;
        }
        if (message.SslRequest) {
            return Write(TStringBuf("N", 1)) && HandleInbox();
        }
        if (message.CancelRequest) {
            PassAway();
            return false;
        }

        const auto database = message.Parameters.FindPtr("database");
        if (!database || database->empty()) {
            Fail("3D000", "database is required");
            return false;
        }
        DatabasePath = *database;
        ResolveDatabase();
        return false;
    }

    void ResolveDatabase() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(DatabasePath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        request->ResultSet.emplace_back(std::move(entry));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TSqlConnection::StateResolve);
    }

    void HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto* request = ev->Get()->Request.Get();
        const bool known = request
            && request->ResultSet.size() == 1
            && request->ResultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        if (!known) {
            Fail("3D000", "unknown database");
            return;
        }
        const ui32 backendPid = static_cast<ui32>(SelfId().LocalId());
        const ui32 secret = SelfId().Hint() ^ 0x53514c;
        Write(MakeHandshake(backendPid, secret));
        Become(&TSqlConnection::StateQuery);
        RequestRead();
    }

    bool HandleQueries() {
        for (;;) {
            TString error;
            TClientMessage message;
            if (!TryParseMessage(Inbox, message, error)) {
                return true;
            }
            if (error) {
                Fail("08P01", error);
                return false;
            }
            if (message.Type == 'X') {
                PassAway();
                return false;
            }
            if (message.Type == 'Q') {
                TStringBuf sql(message.Payload);
                if (sql.EndsWith('\0')) {
                    sql.Chop(1);
                }
                TString catalogReply;
                if (TryAnswerCatalog(sql, catalogReply)) {
                    if (!Write(catalogReply)) {
                        return false;
                    }
                    continue;
                }
                const TParseResult parsed = Parse(sql);
                if (parsed.CreateTable) {
                    Register(CreateTableActor(SelfId(), DatabasePath, *parsed.CreateTable));
                    Become(&TSqlConnection::StateScheme);
                    RequestRead();
                    return false;
                }
                if (parsed.AlterTable) {
                    Register(CreateAlterTableActor(SelfId(), DatabasePath, *parsed.AlterTable));
                    Become(&TSqlConnection::StateScheme);
                    RequestRead();
                    return false;
                }
                if (parsed.CreateSequence) {
                    Register(CreateSequenceActor(SelfId(), DatabasePath, *parsed.CreateSequence));
                    Become(&TSqlConnection::StateScheme);
                    RequestRead();
                    return false;
                }
            }
            TString reply = MakeErrorResponse("0A000", "not supported");
            reply.append(MakeReadyForQuery());
            if (!Write(reply)) {
                return false;
            }
        }
    }

    void HandleRegister(NActors::TEvPollerRegisterResult::TPtr& ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        RequestRead();
    }

    void HandleReady(NActors::TEvPollerReady::TPtr& ev) {
        if (ev->Get()->Write) {
            Flush();
            if (CloseAfterWrite && Outbox.empty()) {
                PassAway();
                return;
            }
        }
        const auto state = CurrentStateFunc();
        if (ev->Get()->Read && (state == &TSqlConnection::StateStartup || state == &TSqlConnection::StateQuery || state == &TSqlConnection::StateScheme)) {
            ReadSocket();
        }
    }

    void HandleDdl(TEvDdlResult::TPtr& ev) {
        const TEvDdlResult& result = *ev->Get();
        TString reply;
        if (result.Ok) {
            reply = MakeCommandComplete(result.CommandTag);
        } else {
            const TString sqlState = result.SqlState.empty() ? TString("0A000") : result.SqlState;
            const TString message = result.Message.empty() ? TString("not supported") : result.Message;
            reply = MakeErrorResponse(sqlState, message);
        }
        reply.append(MakeReadyForQuery());
        const bool written = Write(reply);
        Become(&TSqlConnection::StateQuery);
        if (!written) {
            return;
        }
        if (HandleQueries()) {
            RequestRead();
        }
    }

    STATEFN(StateStartup) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvPollerRegisterResult, HandleRegister);
            hFunc(NActors::TEvPollerReady, HandleReady);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigate);
            hFunc(NActors::TEvPollerReady, HandleReady);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateQuery) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvPollerReady, HandleReady);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    STATEFN(StateScheme) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDdlResult, HandleDdl);
            hFunc(NActors::TEvPollerReady, HandleReady);
            cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
        }
    }

    const NActors::TActorId ListenerActorId;
    TIntrusivePtr<NRawSocket::TSocketDescriptor> Socket;
    NRawSocket::TNetworkConfig::TSocketAddressType Address;
    NActors::TPollerToken::TPtr PollerToken;
    TString Inbox;
    TString Outbox;
    TString DatabasePath;
    bool CloseAfterWrite = false;
};

} // namespace

NActors::IActor* CreateSqlConnection(const NActors::TActorId& listenerActorId,
                                     TIntrusivePtr<NRawSocket::TSocketDescriptor> socket,
                                     NRawSocket::TNetworkConfig::TSocketAddressType address) {
    return new TSqlConnection(listenerActorId, std::move(socket), std::move(address));
}

} // namespace NKikimr::NSql
