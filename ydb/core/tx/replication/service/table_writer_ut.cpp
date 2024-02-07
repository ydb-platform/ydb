#include "table_writer.h"
#include "worker.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NService {

Y_UNIT_TEST_SUITE(LocalTableWriter) {
    struct TTestTableDescription {
        struct TColumn {
            TString Name;
            TString Type;

            void SerializeTo(NKikimrSchemeOp::TColumnDescription& proto) const {
                proto.SetName(Name);
                proto.SetType(Type);
            }
        };

        TString Name;
        TVector<TString> KeyColumns;
        TVector<TColumn> Columns;

        void SerializeTo(NKikimrSchemeOp::TTableDescription& proto) const {
            proto.SetName("Table");
            proto.MutableReplicationConfig()->SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
            proto.MutableReplicationConfig()->SetConsistency(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_WEAK);

            for (const auto& keyColumn : KeyColumns) {
                proto.AddKeyColumnNames(keyColumn);
            }

            for (const auto& column : Columns) {
                column.SerializeTo(*proto.AddColumns());
            }
        }
    };

    NKikimrSchemeOp::TTableDescription MakeTableDescription(const TTestTableDescription& desc) {
        NKikimrSchemeOp::TTableDescription proto;
        desc.SerializeTo(proto);
        return proto;
    }

    template <typename Env>
    auto GetDescription(Env& env, const TString& path) {
        auto resp = env.Describe(path);
        return resp->Record;
    }

    template <typename Env>
    TPathId GetPathId(Env& env, const TString& path) {
        const auto& desc = GetDescription(env, path);
        UNIT_ASSERT(desc.HasPathDescription());
        UNIT_ASSERT(desc.GetPathDescription().HasSelf());

        const auto& self = desc.GetPathDescription().GetSelf();
        return TPathId(self.GetSchemeshardId(), self.GetPathId());
    }

    Y_UNIT_TEST(WriteTable) {
        TEnv env;
        env.GetRuntime().SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);

        env.CreateTable("/Root", MakeTableDescription(TTestTableDescription{
            .Name = "Test",
            .KeyColumns = {"key"},
            .Columns = {
                {.Name = "key", .Type = "Uint32"},
                {.Name = "value", .Type = "Utf8"},
            },
        }));

        auto writer = env.GetRuntime().Register(CreateLocalTableWriter(GetPathId(env, "/Root/Table")));
        env.Send<TEvWorker::TEvHandshake>(writer, new TEvWorker::TEvHandshake());

        using TRecord = TEvWorker::TEvData::TRecord;
        env.Send<TEvWorker::TEvPoll>(writer, new TEvWorker::TEvData({
            TRecord(1, R"({"key":[1], "update":{"value":"10"}})"),
            TRecord(2, R"({"key":[2], "update":{"value":"20"}})"),
            TRecord(3, R"({"key":[3], "update":{"value":"30"}})"),
        }));
    }
}

}
