#include "ut_helpers.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/build_index/vector_resolve_actor.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
using namespace Tests;
using namespace NDataShard::NBuildIndex;
using namespace NTableIndex::NKMeans;
using Ydb::Table::VectorIndexSettings;

namespace {

const TString kDatabase = "/Root";
const TString kLevelTable = "/Root/table-level";
const TString kPrefixTable = "/Root/table-prefix";

struct TResolveTestEnv {
    TPortManager PortManager;
    TServer::TPtr Server;
    TTestActorRuntime& Runtime;
    TActorId Sender;

    TResolveTestEnv()
        : Server(new TServer(MakeSettings()))
        , Runtime(*Server->GetRuntime())
    {
        Runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_DEBUG);
        Sender = Runtime.AllocateEdgeActor();
        InitRoot(Server, Sender);
    }

    TServerSettings MakeSettings() {
        TServerSettings settings(PortManager.GetPort(2134));
        settings.SetDomainName("Root");
        return settings;
    }

    void CreateTables(bool prefixed) {
        TShardedTableOptions options;
        options.EnableOutOfOrder(true);
        options.Shards(1);
        CreateLevelTable(Server, Sender, options);
        if (prefixed) {
            CreatePrefixTable(Server, Sender, options);
        }
    }

    void CreatePrefixSequence() {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto* tx = request->Record.MutableTransaction()->MutableModifyScheme();
        tx->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
        tx->SetWorkingDir(kPrefixTable);
        auto* sequence = tx->MutableSequence();
        sequence->SetName(IdColumnSequence);
        sequence->SetMinValue(-0x7FFFFFFFFFFFFFFFLL);
        sequence->SetMaxValue(-1);
        sequence->SetStartValue(static_cast<i64>(SetPostingParentFlag(1)));
        sequence->SetRestart(true);

        Runtime.Send(new IEventHandle(MakeTxProxyID(), Sender, request.Release()));
        auto response = Runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(Sender);
        const auto status = static_cast<TEvTxUserProxy::TResultStatus::EStatus>(response->Get()->Record.GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(status, TEvTxUserProxy::TResultStatus::ExecInProgress);
        WaitTxNotification(Server, Sender, response->Get()->Record.GetTxId());
    }

    void Upsert(const TString& sql) {
        ExecSQL(Server, Sender, sql);
    }

    TActorId CreateResolver(TVectorResolveSettings settings) {
        return Runtime.Register(CreateVectorResolveActor(std::move(settings)));
    }

    THolder<TEvVectorResolveResponse> Resolve(const TActorId& resolver, ui64 requestId,
        TVector<TString> vectors, TVector<TSerializedCellVec> prefixes = {})
    {
        auto request = MakeHolder<TEvVectorResolveRequest>();
        request->RequestId = requestId;
        request->Vectors = std::move(vectors);
        request->Prefixes = std::move(prefixes);
        Runtime.Send(new IEventHandle(resolver, Sender, request.Release()));

        auto response = Runtime.GrabEdgeEventRethrow<TEvVectorResolveResponse>(Sender);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->RequestId, requestId);
        UNIT_ASSERT_C(response->Get()->Success, response->Get()->Issues.ToOneLineString());
        return response->Release();
    }
};

TVectorResolveSettings MakeSettings(bool prefixed) {
    TVectorResolveSettings settings;
    settings.DatabaseName = kDatabase;
    settings.LevelTablePath = kLevelTable;
    if (prefixed) {
        settings.PrefixTablePath = kPrefixTable;
        settings.PrefixColumns = {"user"};
    }
    settings.IndexSettings.set_vector_dimension(2);
    settings.IndexSettings.set_vector_type(VectorIndexSettings::VECTOR_TYPE_UINT8);
    settings.IndexSettings.set_metric(VectorIndexSettings::DISTANCE_COSINE);
    return settings;
}

TString ClusterLiteral(ui64 id) {
    return TStringBuilder() << "Unwrap(CAST(\"" << id << "\" AS Uint64))";
}

TString LevelUpsertSql(const TVector<std::tuple<ui64, ui64, TString>>& rows) {
    TStringBuilder sql;
    sql << "UPSERT INTO `" << kLevelTable << "` (`" << ParentColumn << "`, `" << IdColumn << "`, `"
        << CentroidColumn << "`) VALUES ";
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i) {
            sql << ", ";
        }
        sql << "(" << ClusterLiteral(std::get<0>(rows[i])) << ", "
            << ClusterLiteral(std::get<1>(rows[i])) << ", \""
            << std::get<2>(rows[i]) << "\")";
    }
    sql << ";";
    return sql;
}

TVector<TCell> StringCells(const TString& value, TString& storage) {
    storage = value;
    TVector<TCell> cells;
    cells.push_back(TCell(storage.data(), storage.size()));
    return cells;
}

TString CentroidA() {
    return "\\x10\\x00\\x02";
}

TString CentroidB() {
    return "\\x00\\x10\\x02";
}

} // namespace

Y_UNIT_TEST_SUITE(TVectorResolveActor) {

    Y_UNIT_TEST(NonPrefixed) {
        TResolveTestEnv env;
        env.CreateTables(false);

        env.Upsert(LevelUpsertSql({
            {0, 1, CentroidA()},
            {0, 2, CentroidB()},
            {1, SetPostingParentFlag(11), CentroidA()},
            {1, SetPostingParentFlag(12), CentroidB()},
            {2, SetPostingParentFlag(21), CentroidA()},
            {2, SetPostingParentFlag(22), CentroidB()},
        }));

        auto resolver = env.CreateResolver(MakeSettings(false));
        auto response = env.Resolve(resolver, 1, {
            "\x18\x01\x02", // close to A
            "\x01\x18\x02", // close to B
        });

        UNIT_ASSERT_VALUES_EQUAL(response->Clusters.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[0].size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[0][0], SetPostingParentFlag(11));
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[1].size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[1][0], SetPostingParentFlag(22));
    }

    Y_UNIT_TEST(Prefixed) {
        TResolveTestEnv env;
        env.CreateTables(true);
        env.CreatePrefixSequence();

        env.Upsert(LevelUpsertSql({
            {10, SetPostingParentFlag(31), CentroidA()},
            {10, SetPostingParentFlag(32), CentroidB()},
        }));
        env.Upsert(TStringBuilder()
            << "UPSERT INTO `" << kPrefixTable << "` (`user`, `" << IdColumn << "`) VALUES "
            << "(\"a\", " << ClusterLiteral(10) << "), "
            << "(\"b\", " << ClusterLiteral(SetPostingParentFlag(20)) << ");");

        auto resolver = env.CreateResolver(MakeSettings(true));

        TVector<TSerializedCellVec> prefixes;
        auto addPrefix = [&](const TString& value) {
            TString storage;
            prefixes.emplace_back(StringCells(value, storage));
        };

        addPrefix("a");
        addPrefix("b");
        addPrefix("c");
        addPrefix("c");

        auto response = env.Resolve(resolver, 2, {
            "\x18\x01\x02", // prefix a, close to A
            "\x01\x18\x02", // prefix b, direct posting parent
            "\x18\x01\x02", // prefix c, new prefix
            "\x01\x18\x02", // prefix c again, must reuse the same new ID
        }, prefixes);

        UNIT_ASSERT_VALUES_EQUAL(response->Clusters.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[0].size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[0][0], SetPostingParentFlag(31));
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[1].size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[1][0], SetPostingParentFlag(20));
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[2].size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[2][0], static_cast<ui64>(SetPostingParentFlag(1)));
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[3].size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[3][0], response->Clusters[2][0]);
    }

    Y_UNIT_TEST(PrefixedExistingPrefixIsNotChanged) {
        TResolveTestEnv env;
        env.CreateTables(true);
        env.CreatePrefixSequence();

        env.Upsert(LevelUpsertSql({
            {10, SetPostingParentFlag(31), CentroidA()},
        }));
        env.Upsert(TStringBuilder()
            << "UPSERT INTO `" << kPrefixTable << "` (`user`, `" << IdColumn << "`) VALUES "
            << "(\"a\", " << ClusterLiteral(10) << ");");

        auto resolver = env.CreateResolver(MakeSettings(true));

        TString prefix = "a";
        TVector<TSerializedCellVec> prefixes;
        prefixes.emplace_back(TVector<TCell>{TCell(prefix.data(), prefix.size())});

        auto response = env.Resolve(resolver, 3, {"\x18\x01\x02"}, prefixes);
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[0].size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Clusters[0][0], SetPostingParentFlag(31));
    }
}

} // namespace NKikimr
