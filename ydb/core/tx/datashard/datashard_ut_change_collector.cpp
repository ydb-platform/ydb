#include "change_record.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/library/uuid/uuid.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

namespace NKikimr {

using namespace NDataShard;
using namespace Tests;

namespace {

auto GetValueFromLocalDb(TTestActorRuntime& runtime, const TActorId& sender, ui64 tabletId, const TString& query) {
    auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
    request->Record.MutableProgram()->MutableProgram()->SetText(query);
    runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NKikimrProto::OK);

    return response->Record.GetExecutionEngineEvaluatedResponse();
}

auto GetChangeRecords(TTestActorRuntime& runtime, const TActorId& sender, ui64 tabletId) {
    auto protoValue = GetValueFromLocalDb(runtime, sender, tabletId, R"((
        (let range '( '('Order (Uint64 '0) (Void) )))
        (let columns '('Order 'Group 'PlanStep 'TxId 'PathOwnerId 'LocalPathId 'SchemaVersion) )
        (let result (SelectRange 'ChangeRecords range columns '()))
        (return (AsList (SetResult 'Result result) ))
    ))");
    auto value = NClient::TValue::Create(protoValue);
    const auto& result = value["Result"]["List"];

    TVector<std::tuple<ui64, ui64, ui64, ui64, TPathId, ui64>> records;
    for (size_t i = 0; i < result.Size(); ++i) {
        const auto& item = result[i];
        records.emplace_back(
            item["Order"],
            item["Group"],
            item["PlanStep"],
            item["TxId"],
            TPathId(item["PathOwnerId"], item["LocalPathId"]),
            item["SchemaVersion"]
        );
    }

    return records;
}

auto GetChangeRecordDetails(TTestActorRuntime& runtime, const TActorId& sender, ui64 tabletId) {
    auto protoValue = GetValueFromLocalDb(runtime, sender, tabletId, R"((
        (let range '( '('Order (Uint64 '0) (Void) )))
        (let columns '('Order 'Kind 'Body) )
        (let result (SelectRange 'ChangeRecordDetails range columns '()))
        (return (AsList (SetResult 'Result result) ))
    ))");
    auto value = NClient::TValue::Create(protoValue);
    const auto& result = value["Result"]["List"];

    TVector<std::tuple<ui64, TChangeRecord::EKind, TString>> records;
    for (size_t i = 0; i < result.Size(); ++i) {
        const auto& item = result[i];
        records.emplace_back(
            item["Order"],
            static_cast<TChangeRecord::EKind>(ui8(item["Kind"])),
            item["Body"]
        );
    }

    return records;
}

auto GetChangeRecordsWithDetails(TTestActorRuntime& runtime, const TActorId& sender, ui64 tabletId) {
    const auto records = GetChangeRecords(runtime, sender, tabletId);
    const auto details = GetChangeRecordDetails(runtime, sender, tabletId);
    UNIT_ASSERT_VALUES_EQUAL(records.size(), details.size());

    THashMap<TPathId, TVector<TChangeRecord::TPtr>> result;
    for (size_t i = 0; i < records.size(); ++i) {
        const auto& record = records.at(i);
        const auto& detail = details.at(i);
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(record), std::get<0>(detail));

        const auto& pathId = std::get<4>(record);
        auto it = result.find(pathId);
        if (it == result.end()) {
            it = result.emplace(pathId, TVector<TChangeRecord::TPtr>()).first;
        }

        it->second.push_back(
            TChangeRecordBuilder(std::get<1>(detail))
                .WithOrder(std::get<0>(record))
                .WithGroup(std::get<1>(record))
                .WithStep(std::get<2>(record))
                .WithTxId(std::get<3>(record))
                .WithPathId(std::get<4>(record))
                .WithSchemaVersion(std::get<5>(record))
                .WithBody(std::get<2>(detail))
                .Build()
        );
    }

    return result;
}

struct TUuidHolder {
    TString Uuid;

    TUuidHolder(const TString& uuid)
        : Uuid(uuid)
    {}

    bool operator==(const TUuidHolder& rhs) const {
        return Uuid == rhs.Uuid;
    }

    IOutputStream& operator<<(IOutputStream& os) const {
        os << Uuid;
        return os;
    }

    void Out(IOutputStream& out) const {
        out << Uuid;
    }
};

template <typename V>
using TStructKey = TVector<std::pair<TString, V>>;
using TStructValue = THashMap<TString, ui32>;
constexpr ui32 Null = 0;

template <typename C>
static void OutKvContainer(IOutputStream& out, const C& c) {
    out << "{";
    for (const auto& [k, v] : c) {
        out << " (" << k << ": ";

        bool isUintNull = false;

        if constexpr (std::is_same_v<C, ui32>) {
            if (v == Null) {
                isUintNull = true;
                out << "null";
            }
        }

        if (!isUintNull) {
            out << v;
        }

        out << ")";
    }
    out << " }";
}

template <typename SK>
struct TStructRecordBase {
    NTable::ERowOp Rop;
    TStructKey<SK> Key;
    TStructValue Update;
    TStructValue OldImage;
    TStructValue NewImage;

    TStructRecordBase() = default;

    TStructRecordBase(NTable::ERowOp rop, const TStructKey<SK>& key,
            const TStructValue& update = {},
            const TStructValue& oldImage = {},
            const TStructValue& newImage = {})
        : Rop(rop)
        , Key(key)
        , Update(update)
        , OldImage(oldImage)
        , NewImage(newImage)
    {
    }

    bool operator==(const TStructRecordBase<SK>& rhs) const {
        return Rop == rhs.Rop
            && Key == rhs.Key
            && Update == rhs.Update
            && OldImage == rhs.OldImage
            && NewImage == rhs.NewImage;
    }

    void Out(IOutputStream& out) const {
        out << "{"
            << " Rop: " << Rop
            << " Key: " << Key
            << " Update: " << Update
            << " OldImage: " << OldImage
            << " NewImage: " << NewImage
        << " }";
    }

    static TStructRecordBase<SK> Parse(const NKikimrChangeExchange::TDataChange& proto,
            const THashMap<NTable::TTag, TString>& tagToName)
    {
        TStructRecordBase<SK> record;

        Parse<SK>(proto.GetKey(), tagToName, [&record](const TString& name, SK value) {
            record.Key.emplace_back(name, value);
        });

        switch (proto.GetRowOperationCase()) {
        case NKikimrChangeExchange::TDataChange::kUpsert:
            record.Rop = NTable::ERowOp::Upsert;
            Parse<ui32>(proto.GetUpsert(), tagToName, [&record](const TString& name, ui32 value) {
                record.Update.emplace(name, value);
            });
            break;
        case NKikimrChangeExchange::TDataChange::kErase:
            record.Rop = NTable::ERowOp::Erase;
            break;
        default:
            record.Rop = NTable::ERowOp::Absent;
            break;
        }

        if (proto.HasOldImage()) {
            Parse<ui32>(proto.GetOldImage(), tagToName, [&record](const TString& name, ui32 value) {
                record.OldImage.emplace(name, value);
            });
        }

        if (proto.HasNewImage()) {
            Parse<ui32>(proto.GetNewImage(), tagToName, [&record](const TString& name, ui32 value) {
                record.NewImage.emplace(name, value);
            });
        }

        return record;
    }

    static TStructRecordBase<SK> Parse(const TString& serializedProto, const THashMap<NTable::TTag, TString>& tagToName) {
        NKikimrChangeExchange::TDataChange proto;
        Y_PROTOBUF_SUPPRESS_NODISCARD proto.ParseFromArray(serializedProto.data(), serializedProto.size());
        return Parse(proto, tagToName);
    }

private:
    template <typename T>
    using TInserter = std::function<void(const TString&, T)>;

    template <typename T>
    static void Parse(const NKikimrChangeExchange::TDataChange::TSerializedCells& proto,
            const THashMap<NTable::TTag, TString>& tagToName, TInserter<T> inserter)
    {
        TSerializedCellVec serialized;
        UNIT_ASSERT(TSerializedCellVec::TryParse(proto.GetData(), serialized));

        const auto& cells = serialized.GetCells();
        UNIT_ASSERT_VALUES_EQUAL(cells.size(), proto.TagsSize());

        for (ui32 i = 0; i < proto.TagsSize(); ++i) {
            const auto tag = proto.GetTags(i);
            if (!tagToName.contains(tag)) {
                continue;
            }

            const auto& name = tagToName.at(tag);
            const auto& cell = cells.at(i);

            if (cell.IsNull()) {
                if constexpr (std::is_same_v<T, ui32>) {
                    inserter(name, Null);
                } else if constexpr (std::is_same_v<T, TUuidHolder>) {
                    inserter(name, TUuidHolder("null"));
                }
            } else {
                if constexpr (std::is_same_v<T, ui32>) {
                    inserter(name, cell.AsValue<ui32>());
                } else if constexpr (std::is_same_v<T, TUuidHolder>) {
                    TStringStream ss;
                    NUuid::UuidBytesToString(cell.Data(), ss);
                    inserter(name, TUuidHolder(ss.Str()));
                }
            }
        }
    }
};

using TStructRecord = TStructRecordBase<ui32>;

template <typename SK>
using TStructRecords = THashMap<TString, TVector<TStructRecordBase<SK>>>;

} // anonymous

Y_UNIT_TEST_SUITE(AsyncIndexChangeCollector) {
    template <typename SK = ui32>
    void Run(const TString& path, const TShardedTableOptions& opts, const TVector<TString>& queries, const TStructRecords<SK>& expectedRecords) {
        const auto pathParts = SplitPath(path);
        UNIT_ASSERT(pathParts.size() > 1);

        const auto domainName = pathParts.at(0);
        const auto workingDir = CombinePath(pathParts.begin(), pathParts.begin() + pathParts.size() - 1);
        const auto tableName = pathParts.at(pathParts.size() - 1);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName(domainName)
            .SetUseRealThreads(false)
            .SetEnableDataColumnForIndexTable(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        // prevent change sending
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvChangeExchange::TEvActivateSender::EventType:
                return TTestActorRuntime::EEventAction::DROP;
            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        CreateShardedTable(server, sender, workingDir, tableName, opts);
        for (const auto& query : queries) {
            ExecSQL(server, sender, query);
        }

        auto desc = Navigate(runtime, sender, path);
        const auto& entry = desc->ResultSet.at(0);

        THashMap<NTable::TTag, TString> tagToName;
        for (const auto& [tag, column] : entry.Columns) {
            tagToName.emplace(tag, column.Name);
        }

        THashMap<TString, TPathId> indexNameToPathId;
        for (const auto& index : entry.Indexes) {
            const auto& name = index.GetName();
            const auto pathId = TPathId(index.GetPathOwnerId(), index.GetLocalPathId());
            indexNameToPathId.emplace(name, pathId);
        }

        const auto tabletIds = GetTableShards(server, sender, path);
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);

        const auto actualRecords = GetChangeRecordsWithDetails(runtime, sender, tabletIds[0]);
        for (const auto& [name, expected] : expectedRecords) {
            UNIT_ASSERT(indexNameToPathId.contains(name));
            const auto& pathId = indexNameToPathId.at(name);

            UNIT_ASSERT(actualRecords.contains(pathId));
            const auto& actual = actualRecords.at(pathId);

            UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());
            for (size_t i = 0; i < expected.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(expected.at(i), TStructRecordBase<SK>::Parse(actual.at(i)->GetBody(), tagToName));
                UNIT_ASSERT_VALUES_EQUAL(actual.at(i)->GetSchemaVersion(), entry.TableId.SchemaVersion);
            }
        }
    }

    template <typename SK = ui32>
    void Run(const TString& path, const TShardedTableOptions& opts, const TString& query, const TStructRecords<SK>& expectedRecords) {
        Run(path, opts, TVector<TString>(1, query), expectedRecords);
    }

    TShardedTableOptions SimpleTable() {
        return TShardedTableOptions()
            .Columns({
                {"pkey", "Uint32", true, false},
                {"ikey", "Uint32", false, false},
            })
            .Indexes({
                {"by_ikey", {"ikey"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
            });
    }

    Y_UNIT_TEST(InsertSingleRow) {
        Run("/Root/path", SimpleTable(), "INSERT INTO `/Root/path` (pkey, ikey) VALUES (1, 10);", {
            {"by_ikey", {TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}})}},
        });
    }

    Y_UNIT_TEST(InsertManyRows) {
        Run("/Root/path", SimpleTable(), "INSERT INTO `/Root/path` (pkey, ikey) VALUES (1, 10), (2, 20);", {
            {"by_ikey", {
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 20}, {"pkey", 2}}),
            }},
        });
    }

    Y_UNIT_TEST(UpsertSingleRow) {
        Run("/Root/path", SimpleTable(), "UPSERT INTO `/Root/path` (pkey, ikey) VALUES (1, 10);", {
            {"by_ikey", {TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}})}},
        });
    }

    Y_UNIT_TEST(UpsertManyRows) {
        Run("/Root/path", SimpleTable(), "UPSERT INTO `/Root/path` (pkey, ikey) VALUES (1, 10), (2, 20);", {
            {"by_ikey", {
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 20}, {"pkey", 2}}),
            }},
        });
    }

    Y_UNIT_TEST(UpsertToSameKey) {
        Run("/Root/path", SimpleTable(), "UPSERT INTO `/Root/path` (pkey, ikey) VALUES (1, 10), (1, 20);", {
            {"by_ikey", {
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Erase,  {{"ikey", 10}, {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 20}, {"pkey", 1}}),
            }},
        });
    }

    Y_UNIT_TEST(DeleteNothing) {
        Run("/Root/path", SimpleTable(), "DELETE FROM `/Root/path` WHERE pkey = 1;", {});
    }

    Y_UNIT_TEST(DeleteSingleRow) {
        Run("/Root/path", SimpleTable(), TVector<TString>{
            "UPSERT INTO `/Root/path` (pkey, ikey) VALUES (1, 10);",
            "DELETE FROM `/Root/path` WHERE pkey = 1;",
        }, {
            {"by_ikey", {
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Erase,  {{"ikey", 10}, {"pkey", 1}}),
            }},
        });
    }

    TShardedTableOptions MultiIndexedTable() {
        return TShardedTableOptions()
            .Columns({
                {"pkey", "Uint32", true, false},
                {"ikey1", "Uint32", false, false},
                {"ikey2", "Uint32", false, false},
            })
            .Indexes({
                {"by_ikey1", {"ikey1"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
                {"by_ikey2", {"ikey2"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
            });
    }

    Y_UNIT_TEST(MultiIndexedTableInsertSingleRow) {
        Run("/Root/path", MultiIndexedTable(), "INSERT INTO `/Root/path` (pkey, ikey1, ikey2) VALUES (1, 10, 100);", {
            {"by_ikey1", {TStructRecord(NTable::ERowOp::Upsert, {{"ikey1", 10},  {"pkey", 1}})}},
            {"by_ikey2", {TStructRecord(NTable::ERowOp::Upsert, {{"ikey2", 100}, {"pkey", 1}})}},
        });
    }

    Y_UNIT_TEST(MultiIndexedTableUpdateOneIndexedColumn) {
        Run("/Root/path", MultiIndexedTable(), TVector<TString>{
            "INSERT INTO `/Root/path` (pkey, ikey1, ikey2) VALUES (1, 10, 100);",
            "UPDATE `/Root/path` SET ikey1 = 20 WHERE pkey = 1;",
        }, {
            {"by_ikey1", {
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey1", 10},  {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Erase,  {{"ikey1", 10},  {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey1", 20},  {"pkey", 1}}),
            }},
            {"by_ikey2", {
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey2", 100}, {"pkey", 1}}),
            }},
        });
    }

    Y_UNIT_TEST(MultiIndexedTableReplaceSingleRow) {
        Run("/Root/path", MultiIndexedTable(), TVector<TString>{
            "INSERT INTO `/Root/path` (pkey, ikey1, ikey2) VALUES (1, 10, 100);",
            "REPLACE INTO `/Root/path` (pkey, ikey1) VALUES (1, 20);",
        }, {
            {"by_ikey1", {
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey1", 10},  {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Erase,  {{"ikey1", 10},  {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey1", 20},  {"pkey", 1}}),
            }},
            {"by_ikey2", {
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey2", 100}, {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Erase,  {{"ikey2", 100}, {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey2", Null}, {"pkey", 1}}),
            }},
        });
    }

    TShardedTableOptions IndexedPrimaryKey() {
        return TShardedTableOptions()
            .Columns({
                {"pkey", "Uint32", true, false},
                {"ikey", "Uint32", false, false},
            })
            .Indexes({
                {"by_ikey_pkey", {"ikey", "pkey"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
            });
    }

    Y_UNIT_TEST(IndexedPrimaryKeyInsertSingleRow) {
        Run("/Root/path", IndexedPrimaryKey(), "INSERT INTO `/Root/path` (pkey, ikey) VALUES (1, 10);", {
            {"by_ikey_pkey", {TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}})}},
        });
    }

    Y_UNIT_TEST(IndexedPrimaryKeyDeleteSingleRow) {
        Run("/Root/path", IndexedPrimaryKey(), TVector<TString>{
            "UPSERT INTO `/Root/path` (pkey, ikey) VALUES (1, 10);",
            "DELETE FROM `/Root/path` WHERE pkey = 1;",
        }, {
            {"by_ikey_pkey", {
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Erase,  {{"ikey", 10}, {"pkey", 1}}),
            }},
        });
    }

    TShardedTableOptions CoveredIndex() {
        return TShardedTableOptions()
            .Columns({
                {"pkey", "Uint32", true, false},
                {"ikey", "Uint32", false, false},
                {"value", "Uint32", false, false},
            })
            .Indexes({
                {"by_ikey", {"ikey"}, {"value"}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
            });
    }

    Y_UNIT_TEST(CoveredIndexUpdateCoveredColumn) {
        Run("/Root/path", CoveredIndex(), TVector<TString>{
            "INSERT INTO `/Root/path` (pkey, ikey, value) VALUES (1, 10, 100);",
            "UPDATE `/Root/path` SET value = 200 WHERE pkey = 1;",
        }, {
            {"by_ikey", {
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}}, {{"value", 100}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}}, {{"value", 200}}),
            }},
        });
    }

    Y_UNIT_TEST(CoveredIndexUpsert) {
        Run("/Root/path", CoveredIndex(), TVector<TString>{
            "INSERT INTO `/Root/path` (pkey, ikey, value) VALUES (1, 10, 100);",
            "UPSERT INTO `/Root/path` (pkey, ikey, value) VALUES (1, 10, 200);",
        }, {
            {"by_ikey", {
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}}, {{"value", 100}}),
                TStructRecord(NTable::ERowOp::Erase,  {{"ikey", 10}, {"pkey", 1}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}}, {{"value", 200}}),
            }},
        });
    }

    Y_UNIT_TEST(AllColumnsInPk) {
        const auto schema = TShardedTableOptions()
            .Columns({
                {"a", "Uint32", true, false},
                {"b", "Uint32", true, false},
            })
            .Indexes({
                {"by_b", {"b"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
            });

        Run("/Root/path", schema, TVector<TString>{
            "UPSERT INTO `/Root/path` (a, b) VALUES (1, 10);",
            "UPSERT INTO `/Root/path` (a, b) VALUES (1, 20);",
            "UPSERT INTO `/Root/path` (a, b) VALUES (1, 10);",
            "UPSERT INTO `/Root/path` (a, b) VALUES (2, 10);",
        }, {
            {"by_b", {
                TStructRecord(NTable::ERowOp::Upsert, {{"b", 10}, {"a", 1}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"b", 20}, {"a", 1}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"b", 10}, {"a", 2}}),
            }},
        });
    }

    Y_UNIT_TEST(UpsertWithoutIndexedValue) {
        const auto schema = TShardedTableOptions()
            .Columns({
                {"a", "Uint32", true, false},
                {"b", "Uint32", true, false},
                {"c", "Uint32", false, false},
                {"d", "Uint32", false, false},
            })
            .Indexes({
                {"by_c", {"c"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
            });

        Run("/Root/path", schema, TVector<TString>{
            "UPSERT INTO `/Root/path` (a, b, d) VALUES (1, 10, 10000);",
            "UPSERT INTO `/Root/path` (a, b, c) VALUES (1, 10, 1000);",
        }, {
            {"by_c", {
                TStructRecord(NTable::ERowOp::Upsert, {{"c", Null}, {"a", 1}, {"b", 10}}),
                TStructRecord(NTable::ERowOp::Erase,  {{"c", Null}, {"a", 1}, {"b", 10}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"c", 1000}, {"a", 1}, {"b", 10}}),
            }},
        });
    }

    Y_UNIT_TEST(CoverIndexedColumn) {
        const auto schema = TShardedTableOptions()
            .Columns({
                {"a", "Uint32", true, false},
                {"b", "Uint32", false, false},
                {"c", "Uint32", false, false},
                {"d", "Uint32", false, false},
            })
            .Indexes({
                {"by_bc", {"b", "c"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
                {"by_d", {"d"}, {"c"}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
            });

        Run("/Root/path", schema, TVector<TString>{
            "UPSERT INTO `/Root/path` (a, b, c, d) VALUES (1, 10, 100, 1000);",
        }, {
            {"by_bc", {
                TStructRecord(NTable::ERowOp::Upsert, {{"b", 10}, {"c", 100}, {"a", 1}}),
            }},
            {"by_d", {
                TStructRecord(NTable::ERowOp::Upsert, {{"d", 1000}, {"a", 1}}, {{"c", 100}}),
            }},
        });
    }

    Y_UNIT_TEST(ImplicitlyUpdateCoveredColumn) {
        const auto schema = TShardedTableOptions()
            .Columns({
                {"a", "Uint32", true, false},
                {"b", "Uint32", false, false},
                {"c", "Uint32", false, false},
            })
            .Indexes({
                {"by_b", {"b"}, {"c"}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
            });

        Run("/Root/path", schema, TVector<TString>{
            "UPSERT INTO `/Root/path` (a, b, c) VALUES (1, 10, 100);",
            "UPSERT INTO `/Root/path` (a, b) VALUES (1, 20);",
        }, {
            {"by_b", {
                TStructRecord(NTable::ERowOp::Upsert, {{"b", 10}, {"a", 1}}, {{"c", 100}}),
                TStructRecord(NTable::ERowOp::Erase, {{"b", 10}, {"a", 1}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"b", 20}, {"a", 1}}, {{"c", 100}}),
            }},
        });
    }

} // AsyncIndexChangeCollector

Y_UNIT_TEST_SUITE(CdcStreamChangeCollector) {
    using TCdcStream = TShardedTableOptions::TCdcStream;

    NKikimrPQ::TPQConfig WithProtoSourceIdInfo() {
        NKikimrPQ::TPQConfig pqConfig;
        pqConfig.SetEnableProtoSourceIdInfo(true);
        pqConfig.SetTopicsAreFirstClassCitizen(true);
        // NOTE(shmel1k@): KIKIMR-14221
        pqConfig.SetCheckACL(false);
        pqConfig.SetRequireCredentialsInNewProtocol(false);
        pqConfig.MutableQuotingConfig()->SetEnableQuoting(false);
        return pqConfig;
    }

    template <typename SK = ui32>
    void Run(const NFake::TCaches& cacheParams, const TString& path,
            const TShardedTableOptions& opts, const TVector<TCdcStream>& streams,
            const TVector<TString>& queries, const TStructRecords<SK>& expectedRecords)
    {
        const auto pathParts = SplitPath(path);
        UNIT_ASSERT(pathParts.size() > 1);

        const auto domainName = pathParts.at(0);
        const auto workingDir = CombinePath(pathParts.begin(), pathParts.begin() + pathParts.size() - 1);
        const auto tableName = pathParts.at(pathParts.size() - 1);

        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134), {}, WithProtoSourceIdInfo());
        serverSettings
            .SetDomainName(domainName)
            .SetUseRealThreads(false)
            .SetEnableDataColumnForIndexTable(true)
            .SetCacheParams(cacheParams)
            .SetEnableUuidAsPrimaryKey(true);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        // prevent change sending
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvChangeExchange::TEvActivateSender::EventType:
                return TTestActorRuntime::EEventAction::DROP;
            default:
                return TTestActorRuntime::EEventAction::PROCESS;
            }
        });

        CreateShardedTable(server, sender, workingDir, tableName, opts);

        for (const auto& stream : streams) {
            WaitTxNotification(server, sender, AsyncAlterAddStream(server, workingDir, tableName, stream));
        }

        auto desc = Navigate(runtime, sender, path);
        const auto& entry = desc->ResultSet.at(0);

        const auto tabletIds = GetTableShards(server, sender, path);
        UNIT_ASSERT_VALUES_EQUAL(tabletIds.size(), 1);

        for (const auto& query : queries) {
            if (query.StartsWith("COMPACT TABLE")) {
                const auto result = CompactTable(runtime, tabletIds[0], entry.TableId);
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
                RebootTablet(runtime, tabletIds[0], sender);
            } else {
                ExecSQL(server, sender, query);
            }
        }

        THashMap<NTable::TTag, TString> tagToName;
        for (const auto& [tag, column] : entry.Columns) {
            tagToName.emplace(tag, column.Name);
        }

        THashMap<TPathId, TString> indexPathIdToName;
        for (const auto& index : entry.Indexes) {
            const auto& name = index.GetName();
            const auto pathId = TPathId(index.GetPathOwnerId(), index.GetLocalPathId());
            indexPathIdToName.emplace(pathId, name);
        }

        THashMap<TPathId, TString> streamPathIdToName;
        for (const auto& stream : entry.CdcStreams) {
            const auto& name = stream.GetName();
            const auto pathId = PathIdFromPathId(stream.GetPathId());
            streamPathIdToName.emplace(pathId, name);
        }

        for (const auto& [pathId, actual] : GetChangeRecordsWithDetails(runtime, sender, tabletIds[0])) {
            TString name;
            if (streamPathIdToName.contains(pathId)) {
                name = streamPathIdToName.at(pathId);
            } else if (indexPathIdToName.contains(pathId)) {
                name = indexPathIdToName.at(pathId);
            } else {
                UNIT_ASSERT_C(false, "Unexpected path id: " << pathId);
            }

            UNIT_ASSERT(expectedRecords.contains(name));
            const auto& expected = expectedRecords.at(name);

            UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());
            for (size_t i = 0; i < expected.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(expected.at(i), TStructRecordBase<SK>::Parse(actual.at(i)->GetBody(), tagToName));
                UNIT_ASSERT_VALUES_EQUAL(actual.at(i)->GetSchemaVersion(), entry.TableId.SchemaVersion);
            }
        }
    }

    NFake::TCaches DefaultCacheParams() {
        return {};
    }

    NFake::TCaches TinyCacheParams() {
        auto params = DefaultCacheParams();
        params.Shared = 1; // byte
        return params;
    }

    template <typename SK = ui32>
    void Run(const TString& path, const TShardedTableOptions& opts, const TVector<TCdcStream>& streams,
            const TVector<TString>& queries, const TStructRecords<SK>& expectedRecords)
    {
        Run(DefaultCacheParams(), path, opts, streams, queries, expectedRecords);
    }

    template <typename SK = ui32>
    void Run(const TString& path, const TShardedTableOptions& opts, const TCdcStream& stream,
            const TString& query, const TStructRecords<SK>& expectedRecords)
    {
        Run(path, opts, TVector<TCdcStream>(1, stream), TVector<TString>(1, query), expectedRecords);
    }

    TShardedTableOptions SimpleTable() {
        return TShardedTableOptions();
    }

    TShardedTableOptions UuidTable() {
        return TShardedTableOptions()
            .Columns({
                {"key", "Uuid", true, false},
                {"value", "Uint32", false, false},
            });
    }

    TCdcStream KeysOnly() {
        return TCdcStream{
            .Name = "keys_stream",
            .Mode = NKikimrSchemeOp::ECdcStreamModeKeysOnly,
            .Format = NKikimrSchemeOp::ECdcStreamFormatProto,
        };
    }

    TCdcStream Updates() {
        return TCdcStream{
            .Name = "updates_stream",
            .Mode = NKikimrSchemeOp::ECdcStreamModeUpdate,
            .Format = NKikimrSchemeOp::ECdcStreamFormatProto,
        };
    }

    TCdcStream NewImage() {
        return TCdcStream{
            .Name = "new_image",
            .Mode = NKikimrSchemeOp::ECdcStreamModeNewImage,
            .Format = NKikimrSchemeOp::ECdcStreamFormatProto,
        };
    }

    TCdcStream OldImage() {
        return TCdcStream{
            .Name = "old_image",
            .Mode = NKikimrSchemeOp::ECdcStreamModeOldImage,
            .Format = NKikimrSchemeOp::ECdcStreamFormatProto,
        };
    }

    TCdcStream NewAndOldImages() {
        return TCdcStream{
            .Name = "new_and_old_images",
            .Mode = NKikimrSchemeOp::ECdcStreamModeNewAndOldImages,
            .Format = NKikimrSchemeOp::ECdcStreamFormatProto,
        };
    }

    Y_UNIT_TEST(InsertSingleRow) {
        Run("/Root/path", SimpleTable(), KeysOnly(), "INSERT INTO `/Root/path` (key, value) VALUES (1, 10);", {
            {"keys_stream", {TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}})}},
        });
    }

    Y_UNIT_TEST(InsertSingleUuidRow) {
        Run<TUuidHolder>("/Root/path", UuidTable(), KeysOnly(), "INSERT INTO `/Root/path` (key, value) VALUES (Uuid(\"65df1ec1-a97d-47b2-ae56-3c023da6ee8c\"), 10);", {
            {"keys_stream", {TStructRecordBase<TUuidHolder>(NTable::ERowOp::Upsert, {{"key", TUuidHolder("65df1ec1-a97d-47b2-ae56-3c023da6ee8c")}})}},
        });
    }

    Y_UNIT_TEST(DeleteNothing) {
        Run("/Root/path", SimpleTable(), KeysOnly(), "DELETE FROM `/Root/path` WHERE key = 1;", {
            {"keys_stream", {TStructRecord(NTable::ERowOp::Erase, {{"key", 1}})}},
        });
    }

    Y_UNIT_TEST(UpsertManyRows) {
        Run("/Root/path", SimpleTable(), Updates(), "UPSERT INTO `/Root/path` (key, value) VALUES (1, 10), (2, 20);", {
            {"updates_stream", {
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {{"value", 10}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 2}}, {{"value", 20}}),
            }},
        });
    }

    Y_UNIT_TEST(UpsertToSameKey) {
        Run("/Root/path", SimpleTable(), Updates(), "UPSERT INTO `/Root/path` (key, value) VALUES (1, 10), (1, 20);", {
            {"updates_stream", {
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {{"value", 10}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {{"value", 20}}),
            }},
        });
    }

    Y_UNIT_TEST(UpsertToSameKeyWithImages) {
        Run("/Root/path", SimpleTable(), NewAndOldImages(), "UPSERT INTO `/Root/path` (key, value) VALUES (1, 10), (1, 20);", {
            {"new_and_old_images", {
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {}, {}, {{"value", 10}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {}, {{"value", 10}}, {{"value", 20}}),
            }},
        });
    }

    Y_UNIT_TEST(UpsertIntoTwoStreams) {
        Run("/Root/path", SimpleTable(), TVector<TCdcStream>{Updates(), NewAndOldImages()}, TVector<TString>{
            "UPSERT INTO `/Root/path` (key, value) VALUES (1, 10), (1, 20);",
        }, {
            {"updates_stream", {
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {{"value", 10}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {{"value", 20}}),
            }},
            {"new_and_old_images", {
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {}, {}, {{"value", 10}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {}, {{"value", 10}}, {{"value", 20}}),
            }},
        });
    }

    Y_UNIT_TEST(DeleteSingleRow) {
        Run("/Root/path", SimpleTable(), TVector<TCdcStream>{NewAndOldImages()}, TVector<TString>{
            "UPSERT INTO `/Root/path` (key, value) VALUES (1, 10);",
            "DELETE FROM `/Root/path` WHERE key = 1;",
        }, {
            {"new_and_old_images", {
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {}, {}, {{"value", 10}}),
                TStructRecord(NTable::ERowOp::Erase,  {{"key", 1}}, {}, {{"value", 10}}, {}),
            }},
        });
    }

    Y_UNIT_TEST(UpsertModifyDelete) {
        Run("/Root/path", SimpleTable(), TVector<TCdcStream>{NewAndOldImages()}, TVector<TString>{
            "UPSERT INTO `/Root/path` (key, value) VALUES (1, 10);",
            "UPSERT INTO `/Root/path` (key, value) VALUES (1, 20);",
            "DELETE FROM `/Root/path` WHERE key = 1;",
        }, {
            {"new_and_old_images", {
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {}, {}, {{"value", 10}}),
                TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {}, {{"value", 10}}, {{"value", 20}}),
                TStructRecord(NTable::ERowOp::Erase,  {{"key", 1}}, {}, {{"value", 20}}, {}),
            }},
        });
    }

    TShardedTableOptions IndexedTable() {
        return TShardedTableOptions()
            .Columns({
                {"pkey", "Uint32", true, false},
                {"ikey", "Uint32", false, false},
            })
            .Indexes({
                {"by_ikey", {"ikey"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
            });
    }

    Y_UNIT_TEST(IndexAndStreamUpsert) {
        Run("/Root/path", IndexedTable(), Updates(), "INSERT INTO `/Root/path` (pkey, ikey) VALUES (1, 10);", {
            {"by_ikey", {TStructRecord(NTable::ERowOp::Upsert, {{"ikey", 10}, {"pkey", 1}})}},
            {"updates_stream", {TStructRecord(NTable::ERowOp::Upsert, {{"pkey", 1}}, {{"ikey", 10}})}},
        });
    }

    TShardedTableOptions TinyCacheTable() {
        return TShardedTableOptions()
            .ExecutorCacheSize(1);
    }

    Y_UNIT_TEST(PageFaults) {
        constexpr ui32 count = 1000;
        TVector<TStructRecord> expectedRecords(Reserve(count + 2));
        auto bigUpsert = TStringBuilder() << "UPSERT INTO `/Root/path` (key, value) VALUES ";

        for (ui32 i = 1; i <= count; ++i) {
            expectedRecords.push_back(TStructRecord(NTable::ERowOp::Upsert, {{"key", i}}, {}, {}, {{"value", i}}));

            if (i > 1) {
                bigUpsert << ",";
            }

            bigUpsert << "(" << i << "," << i << ")";
        }

        bigUpsert << ";";
        expectedRecords.push_back(TStructRecord(NTable::ERowOp::Upsert, {{"key", 1}}, {}, {{"value", 1}}, {{"value", 10}}));
        expectedRecords.push_back(TStructRecord(NTable::ERowOp::Upsert, {{"key", 1000}}, {}, {{"value", 1000}}, {{"value", 10000}}));

        Run(TinyCacheParams(), "/Root/path", TinyCacheTable(), TVector<TCdcStream>{NewAndOldImages()}, TVector<TString>{
            bigUpsert,
            "COMPACT TABLE `/Root/path`;",
            "SELECT * FROM `/Root/path` WHERE key = 1;",
            "UPSERT INTO `/Root/path` (key, value) VALUES (1, 10), (1000, 10000);",
        }, {
            {"new_and_old_images", expectedRecords},
        });
    }

    Y_UNIT_TEST(NewImage) {
        const auto schema = TShardedTableOptions()
            .Columns({
                {"a", "Uint32", true, false},
                {"b", "Uint32", false, false},
                {"c", "Uint32", false, false},
            });

        Run("/Root/path", schema, TVector<TCdcStream>{NewImage()}, TVector<TString>{
            "INSERT INTO `/Root/path` (a, b) values (1, 2)",
            "DELETE FROM `/Root/path` WHERE a = 1;",
        }, {
            {"new_image", {
                TStructRecord(NTable::ERowOp::Upsert, {{"a", 1}}, {}, {}, {{"b", 2}, {"c", Null}}),
                TStructRecord(NTable::ERowOp::Erase,  {{"a", 1}}, {}, {}, {}),
            }},
        });
    }

    Y_UNIT_TEST(OldImage) {
        const auto schema = TShardedTableOptions()
            .Columns({
                {"a", "Uint32", true, false},
                {"b", "Uint32", false, false},
                {"c", "Uint32", false, false},
            });

        Run("/Root/path", schema, TVector<TCdcStream>{OldImage()}, TVector<TString>{
            "INSERT INTO `/Root/path` (a, b) values (1, 2)",
            "DELETE FROM `/Root/path` WHERE a = 1;",
        }, {
            {"old_image", {
                TStructRecord(NTable::ERowOp::Upsert, {{"a", 1}}, {}, {}, {}),
                TStructRecord(NTable::ERowOp::Erase,  {{"a", 1}}, {}, {{"b", 2}, {"c", Null}}, {}),
            }},
        });
    }

} // CdcStreamChangeCollector

} // NKikimr

Y_DECLARE_OUT_SPEC(inline, NKikimr::TStructRecord, out, value) {
    return value.Out(out);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::TStructRecordBase<NKikimr::TUuidHolder>, out, value) {
    return value.Out(out);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::TUuidHolder, out, value) {
    return value.Out(out);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::TStructKey<ui32>, out, value) {
    return NKikimr::OutKvContainer<NKikimr::TStructKey<ui32>>(out, value);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::TStructKey<NKikimr::TUuidHolder>, out, value) {
    return NKikimr::OutKvContainer<NKikimr::TStructKey<NKikimr::TUuidHolder>>(out, value);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::TStructValue, out, value) {
    return NKikimr::OutKvContainer(out, value);
}
