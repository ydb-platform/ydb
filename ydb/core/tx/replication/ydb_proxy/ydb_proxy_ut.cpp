#include "ydb_proxy.h"

#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/ticket_parser.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NKikimr::NReplication {

Y_UNIT_TEST_SUITE(YdbProxyTests) {
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
                Endpoint, UseDatabase ? Database : "", std::forward<Args>(args)...));
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

        void SendAsync(const TActorId& recipient, IEventBase* ev) {
            Server.GetRuntime()->Send(new IEventHandle(recipient, Sender, ev));
        }

        template <typename TEvResponse>
        auto Wait(bool rethrow = false) {
            if (rethrow) {
                return Server.GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(Sender);
            } else {
                return Server.GetRuntime()->GrabEdgeEvent<TEvResponse>(Sender);
            }
        }

        template <typename TEvResponse>
        auto Send(const TActorId& recipient, IEventBase* ev) {
            SendAsync(recipient, ev);
            return Wait<TEvResponse>();
        }

        template <typename TEvResponse>
        auto Send(IEventBase* ev) {
            return Send<TEvResponse>(YdbProxy, ev);
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

    Y_UNIT_TEST(MakeDirectory) {
        TEnv env;
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(RemoveDirectory) {
        TEnv env;
        // make
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvRemoveDirectoryResponse>(
                new TEvYdbProxy::TEvRemoveDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvRemoveDirectoryResponse>(
                new TEvYdbProxy::TEvRemoveDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(DescribePath) {
        TEnv env;
        // describe root
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribePathResponse>(
                new TEvYdbProxy::TEvDescribePathRequest("/Root", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& entry = ev->Get()->Result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "Root");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, NYdb::NScheme::ESchemeEntryType::Directory);
        }
        // make dir
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // describe dir
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribePathResponse>(
                new TEvYdbProxy::TEvDescribePathRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& entry = ev->Get()->Result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "dir");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, NYdb::NScheme::ESchemeEntryType::Directory);
        }
    }

    Y_UNIT_TEST(ListDirectory) {
        TEnv env;
        // describe empty root
        {
            auto ev = env.Send<TEvYdbProxy::TEvListDirectoryResponse>(
                new TEvYdbProxy::TEvListDirectoryRequest("/Root", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& self = ev->Get()->Result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(self.Name, "Root");
            UNIT_ASSERT_VALUES_EQUAL(self.Type, NYdb::NScheme::ESchemeEntryType::Directory);

            const auto& children = ev->Get()->Result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(children[0].Name, ".sys");
            UNIT_ASSERT_VALUES_EQUAL(children[0].Type, NYdb::NScheme::ESchemeEntryType::Directory);
        }
        // make dir
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // describe non-empty root
        {
            auto ev = env.Send<TEvYdbProxy::TEvListDirectoryResponse>(
                new TEvYdbProxy::TEvListDirectoryRequest("/Root", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& self = ev->Get()->Result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(self.Name, "Root");
            UNIT_ASSERT_VALUES_EQUAL(self.Type, NYdb::NScheme::ESchemeEntryType::Directory);

            const auto& children = ev->Get()->Result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 2);
        }
    }

    Y_UNIT_TEST(StaticCreds) {
        TEnv env("user1", "password1");
        // make dir
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
    }

    Y_UNIT_TEST(CreateTable) {
        TEnv<false> env;
        // invalid key
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Float) // cannot be key
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // ok, created
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok, exists
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
    }

    Y_UNIT_TEST(DropTable) {
        TEnv<false> env;
        // create
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvDropTableResponse>(
                new TEvYdbProxy::TEvDropTableRequest("/Root/table", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvDropTableResponse>(
                new TEvYdbProxy::TEvDropTableRequest("/Root/table", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(AlterTable) {
        TEnv<false> env;
        // fail
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendDropColumns("extra");

            auto ev = env.Send<TEvYdbProxy::TEvAlterTableResponse>(
                new TEvYdbProxy::TEvAlterTableRequest("/Root/table", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // create
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .AddNullableColumn("extra", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendDropColumns("extra");

            auto ev = env.Send<TEvYdbProxy::TEvAlterTableResponse>(
                new TEvYdbProxy::TEvAlterTableRequest("/Root/table", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // invalid column
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendDropColumns("extra"); // not exist

            auto ev = env.Send<TEvYdbProxy::TEvAlterTableResponse>(
                new TEvYdbProxy::TEvAlterTableRequest("/Root/table", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(CopyTable) {
        TEnv<false> env;
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvCopyTableResponse>(
                new TEvYdbProxy::TEvCopyTableRequest("/Root/table", "/Root/copy", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // create
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvCopyTableResponse>(
                new TEvYdbProxy::TEvCopyTableRequest("/Root/table", "/Root/copy", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
    }

    Y_UNIT_TEST(CopyTables) {
        TEnv<false> env;

        TVector<NYdb::NTable::TCopyItem> items{
            {"/Root/table1", "/Root/copy1"},
            {"/Root/table2", "/Root/copy2"},
        };

        // create
        for (const auto& item : items) {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest(item.SourcePath(), std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // copy
        {

            auto ev = env.Send<TEvYdbProxy::TEvCopyTablesResponse>(
                new TEvYdbProxy::TEvCopyTablesRequest(std::move(items), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
    }

    Y_UNIT_TEST(DescribeTable) {
        TEnv<false> env;
        // create
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // describe
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeTableResponse>(
                new TEvYdbProxy::TEvDescribeTableRequest("/Root/table", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& schema = ev->Get()->Result.GetTableDescription();
            // same pk
            UNIT_ASSERT_EQUAL(schema.GetPrimaryKeyColumns().size(), 1);
            UNIT_ASSERT_EQUAL(schema.GetPrimaryKeyColumns().at(0), "key");
            // same columns
            UNIT_ASSERT_EQUAL(schema.GetColumns().size(), 2);
            UNIT_ASSERT_EQUAL(schema.GetColumns().at(0).Name, "key");
            UNIT_ASSERT_EQUAL(schema.GetColumns().at(1).Name, "value");
        }
    }

    Y_UNIT_TEST(CreateCdcStream) {
        TEnv<false> env;
        // create table
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }

        const auto feed = NYdb::NTable::TChangefeedDescription("updates",
            NYdb::NTable::EChangefeedMode::Updates, NYdb::NTable::EChangefeedFormat::Json
        );

        // two attempts: create, check, retry, check
        for (int i = 1; i <= 2; ++i) {
            // create cdc stream
            {
                auto settings = NYdb::NTable::TAlterTableSettings()
                    .AppendAddChangefeeds(feed);

                auto ev = env.Send<TEvYdbProxy::TEvAlterTableResponse>(
                    new TEvYdbProxy::TEvAlterTableRequest("/Root/table", settings));
                UNIT_ASSERT(ev);
                UNIT_ASSERT(ev->Get()->Result.IsSuccess());
            }
            // describe
            {
                auto ev = env.Send<TEvYdbProxy::TEvDescribeTableResponse>(
                    new TEvYdbProxy::TEvDescribeTableRequest("/Root/table", {}));
                UNIT_ASSERT(ev);
                UNIT_ASSERT(ev->Get()->Result.IsSuccess());

                const auto& schema = ev->Get()->Result.GetTableDescription();
                UNIT_ASSERT_EQUAL(schema.GetChangefeedDescriptions().size(), 1);
                UNIT_ASSERT_EQUAL(schema.GetChangefeedDescriptions().at(0), feed);
            }
        }
    }

    Y_UNIT_TEST(CreateTopic) {
        TEnv env;
        // invalid retention period
        {
            auto settings = NYdb::NTopic::TCreateTopicSettings()
                .RetentionPeriod(TDuration::Days(365));

            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
    }

    Y_UNIT_TEST(AlterTopic) {
        TEnv env;
        // fail
        {
            auto settings = NYdb::NTopic::TAlterTopicSettings()
                .SetRetentionPeriod(TDuration::Days(2));

            auto ev = env.Send<TEvYdbProxy::TEvAlterTopicResponse>(
                new TEvYdbProxy::TEvAlterTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // create
        {
            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto settings = NYdb::NTopic::TAlterTopicSettings()
                .SetRetentionPeriod(TDuration::Days(2));

            auto ev = env.Send<TEvYdbProxy::TEvAlterTopicResponse>(
                new TEvYdbProxy::TEvAlterTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // invalid retention period
        {
            auto settings = NYdb::NTopic::TAlterTopicSettings()
                .SetRetentionPeriod(TDuration::Days(365));

            auto ev = env.Send<TEvYdbProxy::TEvAlterTopicResponse>(
                new TEvYdbProxy::TEvAlterTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(DropTopic) {
        TEnv env;
        // create
        {
            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvDropTopicResponse>(
                new TEvYdbProxy::TEvDropTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvDropTopicResponse>(
                new TEvYdbProxy::TEvDropTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(DescribeTopic) {
        TEnv env;
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeTopicResponse>(
                new TEvYdbProxy::TEvDescribeTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // create
        {
            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeTopicResponse>(
                new TEvYdbProxy::TEvDescribeTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& schema = ev->Get()->Result.GetTopicDescription();
            UNIT_ASSERT_VALUES_EQUAL(schema.GetTotalPartitionsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(schema.GetRetentionPeriod(), TDuration::Days(1));
        }
    }

    Y_UNIT_TEST(DescribeConsumer) {
        TEnv env;
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeConsumerResponse>(
                new TEvYdbProxy::TEvDescribeConsumerRequest("/Root/topic", "consumer", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // create
        {
            auto settings = NYdb::NTopic::TCreateTopicSettings()
                .BeginAddConsumer()
                    .ConsumerName("consumer")
                .EndAddConsumer();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeConsumerResponse>(
                new TEvYdbProxy::TEvDescribeConsumerRequest("/Root/topic", "consumer", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeConsumerResponse>(
                new TEvYdbProxy::TEvDescribeConsumerRequest("/Root/topic", "consumer2", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    template <typename Env>
    TActorId CreateTopicReader(Env& env, const TString& topicPath) {
        auto settings = NYdb::NTopic::TReadSessionSettings()
            .ConsumerName("consumer")
            .AppendTopics(NYdb::NTopic::TTopicReadSettings(topicPath));

        auto ev = env.template Send<TEvYdbProxy::TEvCreateTopicReaderResponse>(
            new TEvYdbProxy::TEvCreateTopicReaderRequest(settings));
        UNIT_ASSERT(ev);
        UNIT_ASSERT(ev->Get()->Result);

        return ev->Get()->Result;
    }

    template <typename Env>
    bool WriteTopic(const Env& env, const TString& topicPath, const TString& data) {
        NYdb::NTopic::TTopicClient client(env.GetDriver(), NYdb::NTopic::TTopicClientSettings()
            .DiscoveryEndpoint(env.GetEndpoint())
            .Database(env.GetDatabase())
        );

        auto session = client.CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
            .Path(topicPath)
            .ProducerId("producer")
            .MessageGroupId("producer")
        );

        const auto result = session->Write(data);
        session->Close();

        return result;
    }

    template <typename TEvent>
    TEvent ReadTopicAsync(TEvYdbProxy::TEvReadTopicResponse::TPtr& ev) {
        const auto* event = std::get_if<TEvent>(&ev->Get()->Result);
        UNIT_ASSERT(event);

        return *event;
    }

    template <typename TEvent, typename Env>
    TEvent ReadTopic(Env& env, const TActorId& reader) {
        auto ev = env.template Send<TEvYdbProxy::TEvReadTopicResponse>(reader,
            new TEvYdbProxy::TEvReadTopicRequest());
        UNIT_ASSERT(ev);

        return ReadTopicAsync<TEvent>(ev);
    }

    Y_UNIT_TEST(ReadTopic) {
        using TReadSessionEvent = NYdb::NTopic::TReadSessionEvent;
        TEnv env;

        // create topic
        {
            auto settings = NYdb::NTopic::TCreateTopicSettings()
                .BeginAddConsumer()
                    .ConsumerName("consumer")
                .EndAddConsumer();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }

        TActorId reader = CreateTopicReader(env, "/Root/topic");

        UNIT_ASSERT(WriteTopic(env, "/Root/topic", "message-1"));
        {
            ReadTopic<TReadSessionEvent::TStartPartitionSessionEvent>(env, reader).Confirm();

            auto data = ReadTopic<TReadSessionEvent::TDataReceivedEvent>(env, reader);
            UNIT_ASSERT_VALUES_EQUAL(data.GetMessages().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(data.GetMessages().at(0).GetData(), "message-1");
            data.Commit();

            auto ack = ReadTopic<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(env, reader);
            UNIT_ASSERT_VALUES_EQUAL(ack.GetCommittedOffset(), 1);
        }

        // wait next event
        env.SendAsync(reader, new TEvYdbProxy::TEvReadTopicRequest());

        TActorId newReader = CreateTopicReader(env, "/Root/topic");
        // wait next event
        env.SendAsync(newReader, new TEvYdbProxy::TEvReadTopicRequest());

        bool stopped = false;
        bool closed = false;
        bool started = false;
        while (!stopped || !closed || !started) {
            // wait response from any reader
            TEvYdbProxy::TEvReadTopicResponse::TPtr ev;
            try {
                ev = env.Wait<TEvYdbProxy::TEvReadTopicResponse>(true);
            } catch (yexception&) {
                // bad luck, previous session was not closed, close it manually
                env.SendAsync(reader, new TEvents::TEvPoison());
                stopped = closed = true;
                continue;
            }

            if (ev->Sender == reader) {
                if (!stopped) {
                    ReadTopicAsync<TReadSessionEvent::TStopPartitionSessionEvent>(ev).Confirm();
                    env.SendAsync(reader, new TEvYdbProxy::TEvReadTopicRequest());
                    stopped = true;
                } else if (!closed) {
                    ReadTopicAsync<TReadSessionEvent::TPartitionSessionClosedEvent>(ev);
                    closed = true;
                } else {
                    UNIT_ASSERT_C(false, "Unexpected event from previous reader");
                }
            } else if (ev->Sender == newReader) {
                if (!started) {
                    ReadTopicAsync<TReadSessionEvent::TStartPartitionSessionEvent>(ev).Confirm();
                    started = true;
                } else {
                    UNIT_ASSERT_C(false, "Unexpected event from new reader");
                }
            } else {
                UNIT_ASSERT_C(false, "Unknown reader");
            }
        }

        UNIT_ASSERT(WriteTopic(env, "/Root/topic", "message-2"));
        {
            auto data = ReadTopic<TReadSessionEvent::TDataReceivedEvent>(env, newReader);
            UNIT_ASSERT_VALUES_EQUAL(data.GetMessages().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(data.GetMessages().at(0).GetData(), "message-2");
            data.Commit();

            auto ack = ReadTopic<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(env, newReader);
            UNIT_ASSERT_VALUES_EQUAL(ack.GetCommittedOffset(), 2);
        }
    }

} // YdbProxyTests

}
