#pragma once

#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NTestHelpers {

class TFeatureFlags: public TTestFeatureFlagsHolder<TFeatureFlags> {
};

template <bool UseDatabase = true>
class TEnv {
    static constexpr char DomainName[] = "Root";

    static NKikimrPQ::TPQConfig MakePqConfig() {
        NKikimrPQ::TPQConfig config;
        config.SetRequireCredentialsInNewProtocol(false);
        return config;
    }

    template <typename... Args>
    void Init(Args&&... args) {
        auto grpcPort = PortManager.GetPort();

        Server.EnableGRpc(grpcPort);
        Server.SetupDefaultProfiles();
        Client.InitRootScheme(DomainName);

        Endpoint = "localhost:" + ToString(grpcPort);
        Database = "/" + ToString(DomainName);

        YdbProxy = Server.GetRuntime()->Register(CreateYdbProxy(
            Endpoint, UseDatabase ? Database : "", false /* ssl */, std::forward<Args>(args)...));
        Sender = Server.GetRuntime()->AllocateEdgeActor();
    }

    void Login(ui64 schemeShardId, const TString& user, const TString& password) {
        auto req = MakeHolder<NSchemeShard::TEvSchemeShard::TEvLogin>();
        req->Record.SetUser(user);
        req->Record.SetPassword(password);
        ForwardToTablet(*Server.GetRuntime(), schemeShardId, Sender, req.Release());

        auto resp = Server.GetRuntime()->GrabEdgeEvent<NSchemeShard::TEvSchemeShard::TEvLoginResult>(Sender);
        UNIT_ASSERT(resp->Get()->Record.GetError().empty());
        UNIT_ASSERT(!resp->Get()->Record.GetToken().empty());
    }

public:
    TEnv(bool init = true)
        : Settings(Tests::TServerSettings(PortManager.GetPort(), {}, MakePqConfig())
            .SetDomainName(DomainName)
        )
        , Server(Settings)
        , Client(Settings)
    {
        if (init) {
            Init();
        }
    }

    TEnv(const TFeatureFlags& featureFlags, bool init = true)
        : Settings(Tests::TServerSettings(PortManager.GetPort(), {}, MakePqConfig())
            .SetDomainName(DomainName)
            .SetFeatureFlags(featureFlags.FeatureFlags)
        )
        , Server(Settings)
        , Client(Settings)
    {
        if (init) {
            Init();
        }
    }

    explicit TEnv(const TString& builtin)
        : TEnv(false)
    {
        UNIT_ASSERT_STRING_CONTAINS(builtin, "@builtin");
        Init(builtin);
        Client.ModifyOwner("/", DomainName, builtin);
    }

    explicit TEnv(const TString& user, const TString& password)
        : TEnv(false)
    {
        NKikimrReplication::TStaticCredentials staticCreds;
        staticCreds.SetUser(user);
        staticCreds.SetPassword(password);
        Init(staticCreds);

        const auto db = "/" + ToString(DomainName);
        // create user & set owner
        {
            auto st = Client.CreateUser(db, user, password);
            UNIT_ASSERT_VALUES_EQUAL(st, NMsgBusProxy::EResponseStatus::MSTATUS_OK);

            Client.ModifyOwner("/", DomainName, user);
        }
        // init security state
        {
            auto resp = Client.Ls(db);

            const auto& desc = resp->Record;
            UNIT_ASSERT(desc.HasPathDescription());
            UNIT_ASSERT(desc.GetPathDescription().HasDomainDescription());
            UNIT_ASSERT(desc.GetPathDescription().GetDomainDescription().HasDomainKey());

            Login(desc.GetPathDescription().GetDomainDescription().GetDomainKey().GetSchemeShard(), user, password);
        }
        // update security state
        {
            auto resp = Client.Ls(db);

            const auto& desc = resp->Record;
            UNIT_ASSERT(desc.HasPathDescription());
            UNIT_ASSERT(desc.GetPathDescription().HasDomainDescription());
            UNIT_ASSERT(desc.GetPathDescription().GetDomainDescription().HasSecurityState());

            const auto& secState = desc.GetPathDescription().GetDomainDescription().GetSecurityState();
            Server.GetRuntime()->Send(new IEventHandle(MakeTicketParserID(), Sender,
                new TEvTicketParser::TEvUpdateLoginSecurityState(secState)));
        }
    }

    template <typename... Args>
    auto ModifyOwner(Args&&... args) {
        return Client.ModifyOwner(std::forward<Args>(args)...);
    }

    template <typename... Args>
    auto Describe(Args&&... args) {
        return Client.Ls(std::forward<Args>(args)...);
    }

    auto GetDescription(const TString& path) {
        auto resp = Describe(path);
        return resp->Record;
    }

    TPathId GetPathId(const TString& path) {
        const auto& desc = GetDescription(path);
        UNIT_ASSERT(desc.HasPathDescription());
        UNIT_ASSERT(desc.GetPathDescription().HasSelf());

        const auto& self = desc.GetPathDescription().GetSelf();
        return TPathId(self.GetSchemeshardId(), self.GetPathId());
    }

    ui64 GetSchemeshardId(const TString& path) {
        return GetPathId(path).OwnerId;
    }

    template <typename... Args>
    auto CreateTable(Args&&... args) {
        return Client.CreateTable(std::forward<Args>(args)...);
    }

    template <typename... Args>
    auto CreateTableWithIndex(Args&&... args) {
        return Client.CreateTableWithUniformShardedIndex(std::forward<Args>(args)...);
    }
    
    void SendAsync(const TActorId& recipient, IEventBase* ev) {
        Server.GetRuntime()->Send(new IEventHandle(recipient, Sender, ev));
    }

    template <typename TEvResponse>
    auto Send(const TActorId& recipient, IEventBase* ev) {
        SendAsync(recipient, ev);
        return Server.GetRuntime()->GrabEdgeEvent<TEvResponse>(Sender);
    }

    auto& GetRuntime() {
        return *Server.GetRuntime();
    }

    const NYdb::TDriver& GetDriver() const {
        return Server.GetDriver();
    }

    const TString& GetEndpoint() const {
        return Endpoint;
    }

    const TString& GetDatabase() const {
        return Database;
    }

    const TActorId& GetSender() const {
        return Sender;
    }

    const TActorId& GetYdbProxy() const {
        return YdbProxy;
    }

private:
    TPortManager PortManager;
    Tests::TServerSettings Settings;
    Tests::TServer Server;
    Tests::TClient Client;
    TString Endpoint;
    TString Database;
    TActorId YdbProxy;
    TActorId Sender;
};

}
