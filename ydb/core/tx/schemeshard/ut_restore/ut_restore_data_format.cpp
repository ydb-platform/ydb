#include "restore_data_format_enums.h"
#include "ut_helpers/ut_backup_restore_common.h"

#include <ydb/public/api/protos/ydb_import.pb.h>

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/base/localdb.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <library/cpp/testing/unittest/registar.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/writer.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr::NSchemeShard;
using namespace NKikimr::NWrappers::NTestHelpers;
using namespace NKikimr;
using namespace NKikimrSchemeOp;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::Tests;

namespace {

using ERestoreDataFormat = NSchemeShardUT_Private::ERestoreDataFormat;

struct TParquetUtf8Row {
    TString Key;
    TString Value;
};

TString BuildParquetUtf8Data(const TVector<TParquetUtf8Row>& rows) {
    arrow::StringBuilder keyBuilder;
    arrow::StringBuilder valueBuilder;
    for (auto&& r : rows) {
        UNIT_ASSERT(keyBuilder.Append(r.Key.data(), r.Key.size()).ok());
        UNIT_ASSERT(valueBuilder.Append(r.Value.data(), r.Value.size()).ok());
    }

    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    UNIT_ASSERT(keyBuilder.Finish(&keyArray).ok());
    UNIT_ASSERT(valueBuilder.Finish(&valueArray).ok());

    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("key", arrow::utf8()),
        arrow::field("value", arrow::utf8()),
    });

    auto table = arrow::Table::Make(schema, {keyArray, valueArray});

    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();
    UNIT_ASSERT(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size*/ 16).ok());
    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

TString BuildParquetInt32KeyData(const TVector<std::pair<i32, TString>>& rows) {
    arrow::Int32Builder keyBuilder;
    arrow::StringBuilder valueBuilder;
    for (auto&& [k, v] : rows) {
        UNIT_ASSERT(keyBuilder.Append(k).ok());
        UNIT_ASSERT(valueBuilder.Append(v.data(), v.size()).ok());
    }

    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    UNIT_ASSERT(keyBuilder.Finish(&keyArray).ok());
    UNIT_ASSERT(valueBuilder.Finish(&valueArray).ok());

    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("key", arrow::int32()),
        arrow::field("value", arrow::utf8()),
    });

    auto table = arrow::Table::Make(schema, {keyArray, valueArray});

    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();
    UNIT_ASSERT(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size*/ 16).ok());
    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

TString BuildParquetUtf8WithNullValue() {
    arrow::StringBuilder keyBuilder;
    arrow::StringBuilder valueBuilder;
    UNIT_ASSERT(keyBuilder.Append("k1", 2).ok());
    UNIT_ASSERT(valueBuilder.AppendNull().ok());
    UNIT_ASSERT(keyBuilder.Append("k2", 2).ok());
    UNIT_ASSERT(valueBuilder.Append("v2", 2).ok());

    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    UNIT_ASSERT(keyBuilder.Finish(&keyArray).ok());
    UNIT_ASSERT(valueBuilder.Finish(&valueArray).ok());

    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("key", arrow::utf8()),
        arrow::field("value", arrow::utf8()),
    });

    auto table = arrow::Table::Make(schema, {keyArray, valueArray});
    auto sink = arrow::io::BufferOutputStream::Create(0).ValueOrDie();
    UNIT_ASSERT(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, /*chunk_size*/ 16).ok());
    auto buffer = sink->Finish().ValueOrDie();
    return TString(reinterpret_cast<const char*>(buffer->data()), buffer->size());
}

struct TDataWithChecksum {
    TString Data;
    TString Checksum;

    TDataWithChecksum() = default;

    TDataWithChecksum(TString&& data)
        : Data(std::move(data))
        , Checksum(NBackup::ComputeChecksum(Data))
    {}

    TDataWithChecksum(const char* data)
        : TDataWithChecksum(TString(data))
    {}

    operator TString() const {
        return Data;
    }
};

struct TTestData {
    TDataWithChecksum RawData;
    TString Data;
    TString YsonStr;
    EDataFormat DataFormat = EDataFormat::Csv;

    TTestData(TString csvData, TString ysonStr)
        : RawData(std::move(csvData))
        , Data(RawData)
        , YsonStr(std::move(ysonStr))
    {}

    TTestData(TString payload, TString ysonStr, EDataFormat format)
        : RawData(std::move(payload))
        , Data(static_cast<TString>(RawData))
        , YsonStr(std::move(ysonStr))
        , DataFormat(format)
    {}

    TString Ext() const {
        switch (DataFormat) {
        case EDataFormat::Csv:
            return ".csv";
        case EDataFormat::Parquet:
            return ".parquet";
        case EDataFormat::Invalid:
            UNIT_ASSERT_C(false, "Invalid data format");
            break;
        }

        return {};
    }
};

TTestData GenerateCsvTestData(const TString& keyPrefix, ui32 count) {
    TStringBuilder csv;
    TStringBuilder yson;

    for (ui32 i = 1; i <= count; ++i) {
        if (keyPrefix) {
            csv << "\"" << keyPrefix << i << "\",";
        } else {
            csv << i << ",";
        }

        csv << "\"" << "value" << i << "\"" << Endl;

        if (i == 1) {
            yson << "[[[[";
        } else {
            yson << ";";
        }

        yson << "["
            << "[\"" << (keyPrefix ? keyPrefix + ToString(i) : ToString(i)) << "\"];"
            << "[\"" << "value" << i << "\"]"
            << "]";

        if (i == count) {
            yson << "];\%false]]]";
        }
    }

    return TTestData(std::move(csv), std::move(yson));
}

TTestData GenerateCsvUtf8Rows(const TVector<TParquetUtf8Row>& rows) {
    TStringBuilder csv;
    TStringBuilder yson;

    for (size_t i = 0; i < rows.size(); ++i) {
        csv << "\"" << rows[i].Key << "\",\"" << rows[i].Value << "\"" << Endl;

        if (i == 0) {
            yson << "[[[[";
        } else {
            yson << ";";
        }

        yson << "[[\"" << rows[i].Key << "\"];[\"" << rows[i].Value << "\"]]";

        if (i + 1 == rows.size()) {
            yson << "];\%false]]]";
        }
    }

    return TTestData(std::move(csv), std::move(yson));
}

struct TTestDataWithScheme {
    TDataWithChecksum Metadata = R"({"version": 0})";
    EPathType Type = EPathTypeTable;
    TDataWithChecksum Scheme;
    TVector<TTestData> Data;

    TTestDataWithScheme(TString&& scheme, TVector<TTestData>&& data)
        : Scheme(std::move(scheme))
        , Data(std::move(data))
    {}
};

THashMap<TString, TString> ConvertTableTestData(const TTestDataWithScheme& item) {
    THashMap<TString, TString> result;
    const TString prefix;

    result.emplace(prefix + "/metadata.json", item.Metadata);

    const auto schemeKey = prefix + "/scheme.pb";
    result.emplace(schemeKey, item.Scheme);

    for (ui32 i = 0; i < item.Data.size(); ++i) {
        const auto& data = item.Data.at(i);
        result.emplace(Sprintf("%s/data_%02d%s", prefix.data(), i, data.Ext().c_str()), data.Data);
    }

    return result;
}

THashMap<TString, TString> MakeParquetS3Data(const TString& scheme, const TVector<TString>& parts) {
    THashMap<TString, TString> data;
    data.emplace("/metadata.json", R"({"version": 0})");
    data.emplace("/scheme.pb", scheme);
    for (ui32 i = 0; i < parts.size(); ++i) {
        data.emplace(Sprintf("/data_%02d.parquet", i), parts[i]);
    }

    return data;
}

TString Utf8KeySchemePb() {
    return R"(
        columns {
          name: "key"
          type { optional_type { item { type_id: UTF8 } } }
        }
        columns {
          name: "value"
          type { optional_type { item { type_id: UTF8 } } }
        }
        primary_key: "key"
    )";
}

TString Int32KeySchemePb() {
    return R"(
        columns {
          name: "key"
          type { optional_type { item { type_id: INT32 } } }
        }
        columns {
          name: "value"
          type { optional_type { item { type_id: UTF8 } } }
        }
        primary_key: "key"
    )";
}

void ApplyParquetFeatureFlag(TTestBasicRuntime& runtime, ERestoreDataFormat format, bool enable = true) {
    if (format == ERestoreDataFormat::Parquet && enable) {
        runtime.GetAppData().FeatureFlags.SetEnableParquetForS3Import(true);
    }
}

TString RestoreDataFormatField(ERestoreDataFormat format) {
    return format == ERestoreDataFormat::Parquet ? "DataFormat: PARQUET\n" : TString();
}

TString ImportDataFormatField(ERestoreDataFormat format) {
    return format == ERestoreDataFormat::Parquet ? "data_format: PARQUET\n" : TString();
}

void DoRestore(
    TTestBasicRuntime& runtime,
    TTestEnv& env,
    const TString& tableScheme,
    const THashMap<TString, TString>& s3Data,
    ERestoreDataFormat format,
    ui32 readBatchSize = 4194304)
{
    ApplyParquetFeatureFlag(runtime, format);

    ui64 txId = 100;
    TestCreateTable(runtime, ++txId, "/MyRoot", tableScheme);
    env.TestWaitNotification(runtime, txId);

    TPortManager portManager;
    const ui16 port = portManager.GetPort();

    TS3Mock s3Mock(s3Data, TS3Mock::TSettings(port));
    UNIT_ASSERT(s3Mock.Start());

    runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);

    const auto desc = DescribePath(runtime, "/MyRoot/Table", true, true);
    UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

    NKikimrSchemeOp::TTableDescription tableDescription;
    tableDescription.MutableColumns()->CopyFrom(desc.GetPathDescription().GetTable().GetColumns());
    tableDescription.MutableKeyColumnNames()->CopyFrom(desc.GetPathDescription().GetTable().GetKeyColumnNames());

    TString tableDescriptionStr;
    UNIT_ASSERT(google::protobuf::TextFormat::PrintToString(tableDescription, &tableDescriptionStr));

    TestRestore(runtime, ++txId, "/MyRoot", Sprintf(R"(
        TableName: "Table"
        TableDescription {
            %s
        }
        S3Settings {
            Endpoint: "localhost:%d"
            Scheme: HTTP
            %s
            Limits {
                ReadBatchSize: %u
            }
        }
    )", tableDescriptionStr.data(), port, RestoreDataFormatField(format).data(), readBatchSize), {NKikimrScheme::StatusAccepted});
    env.TestWaitNotification(runtime, txId);
}

void DoImport(
    TTestBasicRuntime& runtime,
    const THashMap<TString, TString>& s3Data,
    ERestoreDataFormat format,
    Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS,
    bool enableParquetFeatureFlag = true)
{
    TTestEnv env(runtime, TTestEnvOptions());
    ApplyParquetFeatureFlag(runtime, format, enableParquetFeatureFlag);

    ui64 id = 100;

    TPortManager portManager;
    const ui16 port = portManager.GetPort();

    TS3Mock s3Mock(s3Data, TS3Mock::TSettings(port));
    UNIT_ASSERT(s3Mock.Start());

    runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);

    const TString requestStr = Sprintf(R"(
        ImportFromS3Settings {
          endpoint: "localhost:%d"
          scheme: HTTP
          %s
          items {
            source_prefix: ""
            destination_path: "/MyRoot/Table"
          }
        }
    )", port, ImportDataFormatField(format).data());

    auto initialStatus = Ydb::StatusIds::SUCCESS;
    if (expectedStatus == Ydb::StatusIds::BAD_REQUEST ||
        expectedStatus == Ydb::StatusIds::PRECONDITION_FAILED) {
        initialStatus = expectedStatus;
    }

    TestImport(runtime, ++id, "/MyRoot", requestStr, "", "", initialStatus);
    env.TestWaitNotification(runtime, id);

    if (initialStatus == Ydb::StatusIds::SUCCESS) {
        TestGetImport(runtime, id, "/MyRoot", expectedStatus);
    }
}

THashMap<TString, TString> MakeSingleShardUtf8S3Data(ERestoreDataFormat format) {
    if (format == ERestoreDataFormat::Parquet) {
        const auto parquet = BuildParquetUtf8Data({
            {"a1", "value1"},
            {"a2", "value2"},
            {"a3", "value3"},
        });
        return MakeParquetS3Data(Utf8KeySchemePb(), {parquet});
    }

    return ConvertTableTestData(TTestDataWithScheme(
        Utf8KeySchemePb(),
        {GenerateCsvTestData("a", 3)}));
}

THashMap<TString, TString> MakeInt32KeyS3Data(ERestoreDataFormat format) {
    if (format == ERestoreDataFormat::Parquet) {
        const auto parquet = BuildParquetInt32KeyData({
            {1, "v1"},
            {2, "v2"},
            {3, "v3"},
        });

        return MakeParquetS3Data(Int32KeySchemePb(), {parquet});
    }

    TStringBuilder csv;
    TStringBuilder yson;
    for (i32 i = 1; i <= 3; ++i) {
        csv << i << ",\"v" << i << "\"" << Endl;
        if (i == 1) {
            yson << "[[[[";
        } else {
            yson << ";";
        }
        yson << "[[\"" << i << "\"];[\"v" << i << "\"]]";
        if (i == 3) {
            yson << "];\%false]]]";
        }
    }

    return ConvertTableTestData(TTestDataWithScheme(
        Int32KeySchemePb(),
        {TTestData(std::move(csv), std::move(yson))}));
}

THashMap<TString, TString> MakeMultiShardUtf8S3Data(ERestoreDataFormat format) {
    if (format == ERestoreDataFormat::Parquet) {
        return MakeParquetS3Data(Utf8KeySchemePb(), TVector<TString>{
            BuildParquetUtf8Data({{"a1", "v_a_1"}, {"a2", "v_a_2"}}),
            BuildParquetUtf8Data({{"b1", "v_b_1"}, {"b2", "v_b_2"}}),
        });
    }

    return ConvertTableTestData(TTestDataWithScheme(
        Utf8KeySchemePb(),
        {
            GenerateCsvUtf8Rows({{"a1", "v_a_1"}, {"a2", "v_a_2"}}),
            GenerateCsvUtf8Rows({{"b1", "v_b_1"}, {"b2", "v_b_2"}}),
        }));
}

THashMap<TString, TString> MakeImportSingleShardS3Data(ERestoreDataFormat format) {
    if (format == ERestoreDataFormat::Parquet) {
        const auto parquet = BuildParquetUtf8Data({
            {"a1", "value1"},
            {"a2", "value2"},
        });

        return MakeParquetS3Data(Utf8KeySchemePb(), {parquet});
    }

    return ConvertTableTestData(TTestDataWithScheme(
        Utf8KeySchemePb(),
        {GenerateCsvTestData("a", 2)}));
}

Y_UNIT_TEST_SUITE(TRestoreDataFormatTests) {
    Y_UNIT_TEST(ShouldSucceedOnSingleShardUtf8, ERestoreDataFormat) {
        const auto format = Arg<0>();

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());

        const auto s3Data = MakeSingleShardUtf8S3Data(format);

        DoRestore(runtime, env, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", s3Data, format);

        const TString expectedYson =
            R"([[[[[["a1"];["value1"]];[["a2"];["value2"]];[["a3"];["value3"]]];%false]]])";

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key"}, {"key", "value"});
        NKqp::CompareYson(expectedYson, content);
    }

    Y_UNIT_TEST(ShouldSucceedOnInt32Key, ERestoreDataFormat) {
        const auto format = Arg<0>();

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());

        DoRestore(runtime, env, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Int32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", MakeInt32KeyS3Data(format), format);

        const TString expectedYson =
            R"([[[[[["1"];["v1"]];[["2"];["v2"]];[["3"];["v3"]]];%false]]])";

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key"}, {"key", "value"});
        NKqp::CompareYson(expectedYson, content);
    }

    Y_UNIT_TEST(ShouldSucceedOnMultiShardTable, ERestoreDataFormat) {
        const auto format = Arg<0>();

        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());

        ApplyParquetFeatureFlag(runtime, format);

        ui64 txId = 100;
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Text: "b" } }
              }
            }
        )");

        env.TestWaitNotification(runtime, txId);

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock(MakeMultiShardUtf8S3Data(format), TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);

        const auto desc = DescribePath(runtime, "/MyRoot/Table", true, true);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

        NKikimrSchemeOp::TTableDescription tableDescription;
        tableDescription.MutableColumns()->CopyFrom(desc.GetPathDescription().GetTable().GetColumns());
        tableDescription.MutableKeyColumnNames()->CopyFrom(desc.GetPathDescription().GetTable().GetKeyColumnNames());

        TString tableDescriptionStr;
        UNIT_ASSERT(google::protobuf::TextFormat::PrintToString(tableDescription, &tableDescriptionStr));

        TestRestore(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableName: "Table"
            TableDescription {
                %s
            }
            S3Settings {
                Endpoint: "localhost:%d"
                Scheme: HTTP
                %s
            }
        )", tableDescriptionStr.data(), port, RestoreDataFormatField(format).data()));
        env.TestWaitNotification(runtime, txId);

        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 0, "Table", {"key"}, {"key", "value"});
            NKqp::CompareYson(
                R"([[[[[["a1"];["v_a_1"]];[["a2"];["v_a_2"]]];%false]]])", content);
        }
        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 1, "Table", {"key"}, {"key", "value"});
            NKqp::CompareYson(
                R"([[[[[["b1"];["v_b_1"]];[["b2"];["v_b_2"]]];%false]]])", content);
        }
    }

    // CSV lines cannot encode NULL (empty token is rejected by TYdbDump::ParseLine).
    Y_UNIT_TEST(ShouldHandleNullValues) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());

        DoRestore(runtime, env, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", MakeParquetS3Data(Utf8KeySchemePb(), {BuildParquetUtf8WithNullValue()}), ERestoreDataFormat::Parquet);

        const TString expectedYson =
            R"([[[[[["k1"];#];[["k2"];["v2"]]];%false]]])";

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key"}, {"key", "value"});
        NKqp::CompareYson(expectedYson, content);
    }
}

Y_UNIT_TEST_SUITE(TImportFromS3DataFormatTests) {
    Y_UNIT_TEST(ShouldSucceedOnSingleShardTable, ERestoreDataFormat) {
        const auto format = Arg<0>();

        TTestBasicRuntime runtime;

        DoImport(runtime, MakeImportSingleShardS3Data(format), format);

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key"}, {"key", "value"});
        NKqp::CompareYson(R"([[[[[["a1"];["value1"]];[["a2"];["value2"]]];%false]]])", content);
    }

    Y_UNIT_TEST(ShouldFailWhenFeatureFlagDisabled) {
        TTestBasicRuntime runtime;

        const auto parquet = BuildParquetUtf8Data({{"k", "v"}});

        DoImport(
            runtime,
            MakeParquetS3Data(Utf8KeySchemePb(), {parquet}),
            ERestoreDataFormat::Parquet,
            Ydb::StatusIds::CANCELLED,
            /*enableParquetFeatureFlag=*/false);
    }
}

} // anonymous namespace
