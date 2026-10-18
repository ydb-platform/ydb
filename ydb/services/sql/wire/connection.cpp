#include "connection.h"

#include "catalog.h"
#include "protocol.h"

#include <ydb/services/sql/parse/parser.h>

#include <ydb/services/sql/session/events.h>

#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NSql {

namespace {

class TSqlConnection: public NActors::TActorBootstrapped<TSqlConnection> {
public:
    TSqlConnection(const NActors::TActorId& listenerActorId,
                   TIntrusivePtr<NRawSocket::TSocketDescriptor> socket,
                   NRawSocket::TNetworkConfig::TSocketAddressType address,
                   ui32 internalErrorRetries)
        : ListenerActorId(listenerActorId)
        , Socket(std::move(socket))
        , Address(std::move(address))
        , InternalErrorRetries(internalErrorRetries)
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
        if (CurrentStateFunc() == &TSqlConnection::StateExecute) {
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
        Session = Register(CreateSqlSession(DatabasePath, InternalErrorRetries));
        Write(MakeHandshake(backendPid, secret));
        Become(&TSqlConnection::StateQuery);
        RequestRead();
    }

    bool ReadCString(TStringBuf& payload, TString& value) const {
        const size_t end = payload.find('\0');
        if (end == TStringBuf::npos) {
            return false;
        }
        value = TString(payload.substr(0, end));
        payload.Skip(end + 1);
        return true;
    }

    bool ReadInt16(TStringBuf& payload, i16& value) const {
        if (payload.size() < 2) {
            return false;
        }
        const auto* bytes = reinterpret_cast<const unsigned char*>(payload.data());
        value = static_cast<i16>((ui16(bytes[0]) << 8) | ui16(bytes[1]));
        payload.Skip(2);
        return true;
    }

    bool ReadInt32(TStringBuf& payload, i32& value) const {
        if (payload.size() < 4) {
            return false;
        }
        const auto* bytes = reinterpret_cast<const unsigned char*>(payload.data());
        value = static_cast<i32>((ui32(bytes[0]) << 24) | (ui32(bytes[1]) << 16) | (ui32(bytes[2]) << 8) | ui32(bytes[3]));
        payload.Skip(4);
        return true;
    }

    ui16 CountParams(TStringBuf sql) const {
        ui16 highest = 0;
        bool quoted = false;
        for (size_t i = 0; i < sql.size(); ++i) {
            if (sql[i] == '\'') {
                quoted = !quoted;
                continue;
            }
            if (quoted || sql[i] != '$') {
                continue;
            }
            ui32 number = 0;
            size_t j = i + 1;
            while (j < sql.size() && sql[j] >= '0' && sql[j] <= '9') {
                number = number * 10 + static_cast<ui32>(sql[j] - '0');
                ++j;
            }
            if (j > i + 1 && number > highest && number <= Max<ui16>()) {
                highest = static_cast<ui16>(number);
            }
            if (j > i + 1) {
                i = j - 1;
            }
        }
        return highest;
    }

    TString OnParse(TStringBuf payload) {
        TString name;
        TString sql;
        if (!ReadCString(payload, name) || !ReadCString(payload, sql)) {
            return MakeErrorResponse("08P01", "parse");
        }
        const TParseResult parsed = Parse(sql);
        if (parsed.SqlState) {
            return MakeErrorResponse(parsed.SqlState, "not supported");
        }
        Statements[name] = {sql, CountParams(sql)};
        return MakeParseComplete();
    }

    TString OnBind(TStringBuf payload) {
        TString portal;
        TString statement;
        if (!ReadCString(payload, portal) || !ReadCString(payload, statement)) {
            return MakeErrorResponse("08P01", "bind");
        }
        const TPrepared* prepared = Statements.FindPtr(statement);
        if (!prepared) {
            return MakeErrorResponse("26000", statement);
        }
        i16 formats = 0;
        if (!ReadInt16(payload, formats) || formats < 0) {
            return MakeErrorResponse("08P01", "bind");
        }
        TVector<i16> formatCodes;
        for (i16 i = 0; i < formats; ++i) {
            i16 code = 0;
            if (!ReadInt16(payload, code)) {
                return MakeErrorResponse("08P01", "bind");
            }
            formatCodes.push_back(code);
        }
        i16 count = 0;
        if (!ReadInt16(payload, count) || count < 0) {
            return MakeErrorResponse("08P01", "bind");
        }
        TVector<TBoundParam> params;
        for (i16 i = 0; i < count; ++i) {
            i32 length = 0;
            if (!ReadInt32(payload, length)) {
                return MakeErrorResponse("08P01", "bind");
            }
            TBoundParam param;
            if (length < 0) {
                param.Null = true;
            } else {
                if (payload.size() < static_cast<size_t>(length)) {
                    return MakeErrorResponse("08P01", "bind");
                }
                const TStringBuf bytes = payload.substr(0, length);
                payload.Skip(length);
                const i16 format = formatCodes.empty() ? 0 : formatCodes[formatCodes.size() == 1 ? 0 : i];
                if (format == 1 && (length == 2 || length == 4 || length == 8)) {
                    i64 number = 0;
                    for (char byte : bytes) {
                        number = (number << 8) | static_cast<unsigned char>(byte);
                    }
                    if (length == 2) {
                        number = static_cast<i16>(number);
                    } else if (length == 4) {
                        number = static_cast<i32>(number);
                    }
                    param.Number = true;
                    param.Text = ToString(number);
                } else {
                    param.Text = TString(bytes);
                    bool digits = !param.Text.empty();
                    for (char ch : param.Text) {
                        if (ch < '0' || ch > '9') {
                            digits = false;
                        }
                    }
                    param.Number = digits;
                }
            }
            params.push_back(std::move(param));
        }
        Portals[portal] = {prepared->Sql, std::move(params)};
        return MakeBindComplete();
    }

    TString OnDescribe(TStringBuf payload) {
        if (payload.empty()) {
            return MakeErrorResponse("08P01", "describe");
        }
        const char kind = payload[0];
        payload.Skip(1);
        TString name;
        if (!ReadCString(payload, name)) {
            return MakeErrorResponse("08P01", "describe");
        }
        const TString* sql = nullptr;
        ui16 params = 0;
        if (kind == 'S') {
            const TPrepared* prepared = Statements.FindPtr(name);
            if (!prepared) {
                return MakeErrorResponse("26000", name);
            }
            sql = &prepared->Sql;
            params = prepared->Params;
        } else if (kind == 'P') {
            const TPortal* portal = Portals.FindPtr(name);
            if (!portal) {
                return MakeErrorResponse("34000", name);
            }
            sql = &portal->Sql;
            params = static_cast<ui16>(portal->Params.size());
        } else {
            return MakeErrorResponse("08P01", "describe");
        }
        TString reply = MakeParameterDescription(params);
        const TParseResult parsed = Parse(*sql);
        if (parsed.Select && !parsed.Select->Star && !parsed.Select->Columns.empty()) {
            TVector<TResultColumn> columns;
            for (const TIdentifier& column : parsed.Select->Columns) {
                columns.push_back({column.Name, 25, -1});
            }
            reply.append(MakeRowDescription(columns));
            return reply;
        }
        reply.append(MakeNoData());
        return reply;
    }

    TString OnClose(TStringBuf payload) {
        if (payload.empty()) {
            return MakeErrorResponse("08P01", "close");
        }
        const char kind = payload[0];
        payload.Skip(1);
        TString name;
        if (!ReadCString(payload, name)) {
            return MakeErrorResponse("08P01", "close");
        }
        if (kind == 'S') {
            Statements.erase(name);
        } else if (kind == 'P') {
            Portals.erase(name);
        } else {
            return MakeErrorResponse("08P01", "close");
        }
        return MakeCloseComplete();
    }

    bool BeginExecute(TStringBuf payload) {
        TString name;
        if (!ReadCString(payload, name)) {
            Write(MakeErrorResponse("08P01", "execute"));
            return true;
        }
        const TPortal* portal = Portals.FindPtr(name);
        if (!portal) {
            Write(MakeErrorResponse("34000", name));
            return true;
        }
        auto request = MakeHolder<TEvExecute>();
        request->Sql = portal->Sql;
        request->Params = portal->Params;
        Extended = true;
        Send(Session, request.Release());
        Become(&TSqlConnection::StateExecute);
        RequestRead();
        return false;
    }

    bool HandleExtended(const TClientMessage& message) {
        if (message.Type == 'E') {
            return BeginExecute(message.Payload);
        }
        TString reply;
        if (message.Type == 'P') {
            reply = OnParse(message.Payload);
        } else if (message.Type == 'B') {
            reply = OnBind(message.Payload);
        } else if (message.Type == 'D') {
            reply = OnDescribe(message.Payload);
        } else {
            reply = OnClose(message.Payload);
        }
        return Write(reply);
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
            if (message.Type == 'H') {
                continue;
            }
            if (message.Type == 'S') {
                if (!Write(MakeReadyForQuery(TxState))) {
                    return false;
                }
                continue;
            }
            if (message.Type == 'P' || message.Type == 'B' || message.Type == 'D' || message.Type == 'E' || message.Type == 'C') {
                if (!HandleExtended(message)) {
                    return false;
                }
                continue;
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
                auto request = MakeHolder<TEvExecute>();
                request->Sql = TString(sql);
                Send(Session, request.Release());
                Become(&TSqlConnection::StateExecute);
                RequestRead();
                return false;
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
        if (ev->Get()->Read && (state == &TSqlConnection::StateStartup || state == &TSqlConnection::StateQuery || state == &TSqlConnection::StateExecute)) {
            ReadSocket();
        }
    }

    void HandleExecute(TEvExecuteResult::TPtr& ev) {
        const TEvExecuteResult& result = *ev->Get();
        TString reply;
        TxState = result.TxStatus == 'T' || result.TxStatus == 'E' ? result.TxStatus : 'I';
        if (!result.Ok) {
            const TString sqlState = result.SqlState.empty() ? TString("0A000") : result.SqlState;
            const TString message = result.Message.empty() ? TString("not supported") : result.Message;
            reply = MakeErrorResponse(sqlState, message);
            if (!Extended) {
                reply.append(MakeReadyForQuery(TxState));
            }
        } else if (result.Columns.empty() && !result.CommandTag.StartsWith("SELECT")) {
            reply = MakeCommandComplete(result.CommandTag);
            if (!Extended) {
                reply.append(MakeReadyForQuery(TxState));
            }
        } else {
            TVector<TResultColumn> columns;
            columns.reserve(result.Columns.size());
            for (const TQueryColumn& column : result.Columns) {
                columns.push_back({column.Name, column.TypeOid, column.TypeSize});
            }
            TVector<TVector<TTextCell>> rows;
            rows.reserve(result.Rows.size());
            for (const TVector<TQueryCell>& row : result.Rows) {
                TVector<TTextCell> line;
                line.reserve(row.size());
                for (const TQueryCell& cell : row) {
                    line.push_back({cell.Null, cell.Text});
                }
                rows.push_back(std::move(line));
            }
            reply = MakeTextResult(columns, rows, result.CommandTag, !Extended, TxState);
        }
        Extended = false;
        const bool written = Write(reply);
        Become(&TSqlConnection::StateQuery);
        if (!written) {
            return;
        }
        if (HandleQueries()) {
            RequestRead();
        }
    }

    void PassAway() override {
        if (Session) {
            Send(Session, new NActors::TEvents::TEvPoison);
            Session = {};
        }
        TActorBootstrapped<TSqlConnection>::PassAway();
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

    STATEFN(StateExecute) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExecuteResult, HandleExecute);
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
    struct TPrepared {
        TString Sql;
        ui16 Params = 0;
    };

    struct TPortal {
        TString Sql;
        TVector<TBoundParam> Params;
    };

    NActors::TActorId Session;
    THashMap<TString, TPrepared> Statements;
    THashMap<TString, TPortal> Portals;
    ui32 InternalErrorRetries = 3;
    bool Extended = false;
    char TxState = 'I';
    bool CloseAfterWrite = false;
};

} // namespace

NActors::IActor* CreateSqlConnection(const NActors::TActorId& listenerActorId,
                                     TIntrusivePtr<NRawSocket::TSocketDescriptor> socket,
                                     NRawSocket::TNetworkConfig::TSocketAddressType address,
                                     ui32 internalErrorRetries) {
    return new TSqlConnection(listenerActorId, std::move(socket), std::move(address), internalErrorRetries);
}

} // namespace NKikimr::NSql
