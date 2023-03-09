#include "ydb_proxy.h"

#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/base/ticket_parser.h>

namespace NKikimr::NReplication {

Y_UNIT_TEST_SUITE(YdbProxyTests) {
    template <bool UseDatabase = true>
    class TEnv {
        static constexpr char DomainName[] = "Root";

        template <typename... Args>
        void Init(Args&&... args) {
            auto grpcPort = PortManager.GetPort();

            Server.EnableGRpc(grpcPort);
            Server.SetupDefaultProfiles();
            Client.InitRootScheme(DomainName);

            const auto endpoint = "localhost:" + ToString(grpcPort);
            const auto database = "/" + ToString(DomainName);

            YdbProxy = Server.GetRuntime()->Register(CreateYdbProxy(
                endpoint, UseDatabase ? database : "", std::forward<Args>(args)...));
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
            : Settings(Tests::TServerSettings(PortManager.GetPort())
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
                Server.GetRuntime()->Send(new IEventHandleFat(MakeTicketParserID(), Sender,
                    new TEvTicketParser::TEvUpdateLoginSecurityState(secState)));
            }
        }

        template <typename TEvResponse>
        auto Send(IEventBase* ev) {
            Server.GetRuntime()->Send(new IEventHandleFat(YdbProxy, Sender, ev));
            return Server.GetRuntime()->GrabEdgeEvent<TEvResponse>(Sender);
        }

    private:
        TPortManager PortManager;
        Tests::TServerSettings Settings;
        Tests::TServer Server;
        Tests::TClient Client;
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

} // YdbProxyTests

}
