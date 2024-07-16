#include "ut_helpers/ut_backup_restore_common.h"

#include <contrib/libs/double-conversion/double-conversion/ieee.h>

#include <ydb/core/base/localdb.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/schemeshard_billing_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <ydb/library/binary_json/write.h>
#include <ydb/library/dynumber/dynumber.h>
#include <ydb/library/uuid/uuid.h>


#include <ydb/public/api/protos/ydb_import.pb.h>

#include <contrib/libs/zstd/include/zstd.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NSchemeShard;
using namespace NKikimr::NWrappers::NTestHelpers;

namespace {

    const TString EmptyYsonStr = R"([[[[];%false]]])";

    TString GenerateScheme(const NKikimrSchemeOp::TPathDescription& pathDesc) {
        UNIT_ASSERT(pathDesc.HasTable());
        const auto& tableDesc = pathDesc.GetTable();

        Ydb::Table::CreateTableRequest scheme;
        NKikimrMiniKQL::TType mkqlKeyType;

        scheme.mutable_primary_key()->CopyFrom(tableDesc.GetKeyColumnNames());
        FillColumnDescription(scheme, mkqlKeyType, tableDesc);
        FillIndexDescription(scheme, tableDesc);
        FillStorageSettings(scheme, tableDesc);
        FillColumnFamilies(scheme, tableDesc);
        FillAttributes(scheme, pathDesc);
        FillTableBoundary(scheme, tableDesc, mkqlKeyType);
        FillPartitioningSettings(scheme, tableDesc);
        FillKeyBloomFilter(scheme, tableDesc);
        FillReadReplicasSettings(scheme, tableDesc);

        TString result;
        UNIT_ASSERT(google::protobuf::TextFormat::PrintToString(scheme, &result));

        return result;
    }

    TString GenerateScheme(const NKikimrScheme::TEvDescribeSchemeResult& describeResult) {
        UNIT_ASSERT(describeResult.HasPathDescription());
        return GenerateScheme(describeResult.GetPathDescription());
    }

    TString GenerateTableDescription(const NKikimrScheme::TEvDescribeSchemeResult& describeResult) {
        UNIT_ASSERT(describeResult.HasPathDescription());
        UNIT_ASSERT(describeResult.GetPathDescription().HasTable());
        const auto& tableDesc = describeResult.GetPathDescription().GetTable();

        NKikimrSchemeOp::TTableDescription scheme;
        scheme.MutableColumns()->CopyFrom(tableDesc.GetColumns());
        scheme.MutableKeyColumnNames()->CopyFrom(tableDesc.GetKeyColumnNames());

        TString schemeStr;
        UNIT_ASSERT(google::protobuf::TextFormat::PrintToString(scheme, &schemeStr));

        return schemeStr;
    }

    struct TTestData {
        TString Data;
        TString YsonStr;
        EDataFormat DataFormat = EDataFormat::Csv;
        ECompressionCodec CompressionCodec;

        TTestData(TString data, TString ysonStr, ECompressionCodec codec = ECompressionCodec::None)
            : Data(std::move(data))
            , YsonStr(std::move(ysonStr))
            , CompressionCodec(codec)
        {
        }

        TString Ext() const {
            TStringBuilder result;

            switch (DataFormat) {
            case EDataFormat::Csv:
                result << ".csv";
                break;
            case EDataFormat::Invalid:
                UNIT_ASSERT_C(false, "Invalid data format");
                break;
            }

            switch (CompressionCodec) {
            case ECompressionCodec::None:
                break;
            case ECompressionCodec::Zstd:
                result << ".zst";
                break;
            case ECompressionCodec::Invalid:
                UNIT_ASSERT_C(false, "Invalid compression codec");
                break;
            }

            return result;
        }
    };

    struct TTestDataWithScheme {
        TString Scheme;
        TVector<TTestData> Data;

        TTestDataWithScheme() = default;

        TTestDataWithScheme(TString&& scheme, TVector<TTestData>&& data)
            : Scheme(std::move(scheme))
            , Data(std::move(data))
        {
        }
    };

    TTestData GenerateTestData(const TString& keyPrefix, ui32 count) {
        TStringBuilder csv;
        TStringBuilder yson;

        for (ui32 i = 1; i <= count; ++i) {
            // csv
            if (keyPrefix) {
                csv << "\"" << keyPrefix << i << "\",";
            } else {
                csv << i << ",";
            }

            csv << "\"" << "value" << i << "\"" << Endl;

            // yson
            if (i == 1) {
                yson << "[[[[";
            } else {
                yson << ";";
            }

            yson << "["
                << "[\"" << keyPrefix << i << "\"];"
                << "[\"" << "value" << i << "\"]"
            << "]";

            if (i == count) {
                yson << "];\%false]]]";
            }
        }

        return TTestData(std::move(csv), std::move(yson));
    }

    TString ZstdCompress(const TStringBuf src) {
        TString compressed;
        compressed.resize(ZSTD_compressBound(src.size()));

        const auto res = ZSTD_compress(compressed.Detach(), compressed.size(), src.data(), src.size(), ZSTD_CLEVEL_DEFAULT);
        UNIT_ASSERT_C(!ZSTD_isError(res), "Zstd error: " << ZSTD_getErrorName(res));
        compressed.resize(res);

        return compressed;
    }

    TTestData GenerateZstdTestData(const TString& keyPrefix, ui32 count, ui32 rowsPerFrame = 0) {
        auto data = GenerateTestData(keyPrefix, count);
        if (!rowsPerFrame) {
            rowsPerFrame = count;
        }

        TString compressed;
        ui32 start = 0;
        ui32 rowsInFrame = 0;

        for (ui32 i = 0; i < data.Data.size(); ++i) {
            const auto c = data.Data[i];
            const bool last = i == data.Data.size() - 1;

            if (last) {
                UNIT_ASSERT(c == '\n');
            }

            if (c == '\n') {
                if (++rowsInFrame == rowsPerFrame || last) {
                    compressed.append(ZstdCompress(TStringBuf(&data.Data[start], i + 1 - start)));

                    start = i + 1;
                    rowsInFrame = 0;
                }
            }
        }

        data.Data = std::move(compressed);
        data.CompressionCodec = ECompressionCodec::Zstd;

        return data;
    }

    TTestData GenerateTestData(ECompressionCodec codec, const TString& keyPrefix, ui32 count) {
        switch (codec) {
        case ECompressionCodec::None:
            return GenerateTestData(keyPrefix, count);
        case ECompressionCodec::Zstd:
            return GenerateZstdTestData(keyPrefix, count);
        case ECompressionCodec::Invalid:
            UNIT_ASSERT_C(false, "Invalid compression codec");
            Y_ABORT("unreachable");
        }
    }

    TTestDataWithScheme GenerateTestData(const TString& scheme, const TVector<std::pair<TString, ui64>>& shardsConfig) {
        TTestDataWithScheme result;
        result.Scheme = scheme;

        for (const auto& [keyPrefix, count] : shardsConfig) {
            result.Data.push_back(GenerateTestData(keyPrefix, count));
        }

        return result;
    }

    THashMap<TString, TString> ConvertTestData(const THashMap<TString, TTestDataWithScheme>& data) {
        THashMap<TString, TString> result;

        for (const auto& [prefix, item] : data) {
            result.emplace(prefix + "/scheme.pb", item.Scheme);
            for (ui32 i = 0; i < item.Data.size(); ++i) {
                const auto& data = item.Data.at(i);
                result.emplace(Sprintf("%s/data_%02d%s", prefix.data(), i, data.Ext().c_str()), data.Data);
            }
        }

        return result;
    }

    THashMap<TString, TString> ConvertTestData(const TTestDataWithScheme& data) {
        return ConvertTestData({{"", data}});
    }

    NKikimrMiniKQL::TResult ReadTableImpl(TTestActorRuntime& runtime, ui64 tabletId, const TString& query) {
        NKikimrMiniKQL::TResult result;

        TString error;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, query, result, error);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
        UNIT_ASSERT_VALUES_EQUAL(error, "");

        return result;
    }

    struct TReadKeyDesc {
        TString Name;
        TString Type;
        TString Atom;
    };

    NKikimrMiniKQL::TResult ReadTable(TTestActorRuntime& runtime, ui64 tabletId,
            const TString& table = "Table",
            const TReadKeyDesc& keyDesc = {"key", "Utf8", "\"\""},
            const TVector<TString>& columns = {"key", "value"},
            const TString& rangeFlags = "") {

        const auto rangeFmt = Sprintf("'%s (%s '%s)", keyDesc.Name.data(), keyDesc.Type.data(), keyDesc.Atom.data());
        const auto columnsFmt = "'" + JoinSeq(" '", columns);

        return ReadTableImpl(runtime, tabletId, Sprintf(R"(
            (
                (let range '(%s '(%s (Void) )))
                (let columns '(%s) )
                (let result (SelectRange '__user__%s range columns '()))
                (return (AsList (SetResult 'Result result) ))
            )
        )", rangeFlags.c_str(), rangeFmt.data(), columnsFmt.data(), table.data()));
    }

    using TDelayFunc = std::function<bool(TAutoPtr<IEventHandle>&)>;

    auto SetDelayObserver(TTestActorRuntime& runtime, THolder<IEventHandle>& delayed, TDelayFunc delayFunc) {
        return runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (delayFunc(ev)) {
                delayed.Reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    }

    void WaitForDelayed(TTestActorRuntime& runtime, THolder<IEventHandle>& delayed, TTestActorRuntime::TEventObserver prevObserver) {
        if (!delayed) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&delayed](IEventHandle&) -> bool {
                return bool(delayed);
            });
            runtime.DispatchEvents(opts);
        }

        runtime.SetObserverFunc(prevObserver);
    }


} // anonymous

Y_UNIT_TEST_SUITE(TRestoreTests) {
    void RestoreNoWait(TTestBasicRuntime& runtime, ui64& txId,
            ui16 port, THolder<TS3Mock>& s3Mock, TVector<TTestData>&& data, ui32 readBatchSize = 128) {

        const auto desc = DescribePath(runtime, "/MyRoot/Table", true, true);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

        s3Mock.Reset(new TS3Mock(ConvertTestData({GenerateScheme(desc), std::move(data)}), TS3Mock::TSettings(port)));
        UNIT_ASSERT(s3Mock->Start());

        runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);

        TestRestore(runtime, ++txId, "/MyRoot", Sprintf(R"(
            TableName: "Table"
            TableDescription {
                %s
            }
            S3Settings {
                Endpoint: "localhost:%d"
                Scheme: HTTP
                Limits {
                    ReadBatchSize: %d
                }
            }
        )", GenerateTableDescription(desc).data(), port, readBatchSize));
    }

    void Restore(TTestBasicRuntime& runtime, TTestEnv& env, const TString& creationScheme, TVector<TTestData>&& data, ui32 readBatchSize = 128) {
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", creationScheme);
        env.TestWaitNotification(runtime, txId);

        TPortManager portManager;
        THolder<TS3Mock> s3Mock;

        RestoreNoWait(runtime, txId, portManager.GetPort(), s3Mock, std::move(data), readBatchSize);
        env.TestWaitNotification(runtime, txId);
    }

    void Restore(TTestBasicRuntime& runtime, const TString& creationScheme, TVector<TTestData>&& data, ui32 readBatchSize = 128) {
        TTestEnv env(runtime);
        Restore(runtime, env, creationScheme, std::move(data), readBatchSize);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldSucceedOnSingleShardTable) {
        TTestBasicRuntime runtime;

        const auto data = GenerateTestData(Codec, "a", 1);

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {data});

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.YsonStr, content);
    }

    bool CheckDefaultFromSequence(const NKikimrSchemeOp::TTableDescription& desc) {
        for (const auto& column: desc.GetColumns()) {
            if (column.GetName() == "key") {
                switch (column.GetDefaultValueCase()) {
                    case NKikimrSchemeOp::TColumnDescription::kDefaultFromSequence: {
                        const auto& fromSequence = column.GetDefaultFromSequence();
                        return fromSequence == "myseq";
                    }
                    default: break;
                }
                break;
            }
        }
        return false;
    }

    bool CheckDefaultFromLiteral(const NKikimrSchemeOp::TTableDescription& desc) {
        for (const auto& column: desc.GetColumns()) {
            if (column.GetName() == "value") {
                switch (column.GetDefaultValueCase()) {
                    case NKikimrSchemeOp::TColumnDescription::kDefaultFromLiteral: {
                        const auto& fromLiteral = column.GetDefaultFromLiteral();

                        TString str;
                        google::protobuf::TextFormat::PrintToString(fromLiteral, &str);

                        TString result = R"(type {
  optional_type {
    item {
      type_id: UTF8
    }
  }
}
value {
  items {
    text_value: "value1"
  }
}
)";
                        return str == result;
                    }
                    default: break;
                }
                break;
            }
        }
        return false;
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldSucceedWithDefaultFromLiteral) {
        TTestBasicRuntime runtime;

        const auto data = GenerateTestData(Codec, "a", 1);

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns {
                Name: "value"
                Type: "Utf8"
                DefaultFromLiteral {
                    type {
                        optional_type {
                            item {
                                type_id: UTF8
                            }
                        }
                    }
                    value {
                        items {
                            text_value: "value1"
                        }
                    }
                }
            }
            KeyColumnNames: ["key"]
        )", {data});

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.YsonStr, content);

        const auto desc = DescribePath(runtime, "/MyRoot/Table", true, true);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

        const auto& table = desc.GetPathDescription().GetTable();

        UNIT_ASSERT_C(CheckDefaultFromLiteral(table), "Invalid default value");
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldSucceedOnMultiShardTable) {
        TTestBasicRuntime runtime;

        const auto a = GenerateTestData(Codec, "a", 1);
        const auto b = GenerateTestData(Codec, "b", 1);

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Text: "b" } }
              }
            }
        )", {a, b});

        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 0);
            NKqp::CompareYson(a.YsonStr, content);
        }
        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 1);
            NKqp::CompareYson(b.YsonStr, content);
        }
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldSucceedOnLargeData) {
        TTestBasicRuntime runtime;

        const auto data = GenerateTestData(Codec, "", 100);
        UNIT_ASSERT(data.Data.size() > 128);

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {data});

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key", "Uint32", "0"});
        NKqp::CompareYson(data.YsonStr, content);
    }

    void ShouldSucceedOnMultipleFrames(ui32 batchSize) {
        TTestBasicRuntime runtime;

        const auto data = GenerateZstdTestData("a", 3, 2);

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {data}, batchSize);

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.YsonStr, content);
    }

    Y_UNIT_TEST(ShouldSucceedOnMultipleFramesStandardBatch) {
        ShouldSucceedOnMultipleFrames(128);
    }

    Y_UNIT_TEST(ShouldSucceedOnMultipleFramesSmallBatch) {
        ShouldSucceedOnMultipleFrames(7);
    }

    Y_UNIT_TEST(ShouldSucceedOnMultipleFramesTinyBatch) {
        ShouldSucceedOnMultipleFrames(1);
    }

    Y_UNIT_TEST(ShouldSucceedOnSmallBuffer) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.GetAppData().ZstdBlockSizeForTest = 16;
        runtime.GetAppData().DataShardConfig.SetRestoreReadBufferSizeLimit(16);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        bool uploadResponseDropped = false;
        runtime.SetObserverFunc([&uploadResponseDropped](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvS3UploadRowsResponse) {
                uploadResponseDropped = true;
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TPortManager portManager;
        THolder<TS3Mock> s3Mock;
        const auto data = GenerateZstdTestData("a", 2);
        const ui32 batchSize = 1;
        RestoreNoWait(runtime, txId, portManager.GetPort(), s3Mock, {data}, batchSize);

        if (!uploadResponseDropped) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&uploadResponseDropped](IEventHandle&) -> bool {
                return uploadResponseDropped;
            });
            runtime.DispatchEvents(opts);
        }

        TMaybe<NKikimrTxDataShard::TShardOpResult> result;
        runtime.SetObserverFunc([&result](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::EvSchemaChanged) {
                const auto& record = ev->Get<TEvDataShard::TEvSchemaChanged>()->Record;
                if (record.HasOpResult()) {
                    result = record.GetOpResult();
                }
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        });

        RebootTablet(runtime, TTestTxConfig::FakeHiveTablets, runtime.AllocateEdgeActor());

        if (!result) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&result](IEventHandle&) -> bool {
                return result.Defined();
            });
            runtime.DispatchEvents(opts);
        }

        UNIT_ASSERT_VALUES_EQUAL(result->GetBytesProcessed(), 16);
        UNIT_ASSERT_VALUES_EQUAL(result->GetRowsProcessed(), 2);

        env.TestWaitNotification(runtime, txId);
        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.YsonStr, content);
    }

    Y_UNIT_TEST(ShouldNotDecompressEntirePortionAtOnce) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().ZstdBlockSizeForTest = 113; // one row

        ui32 uploadRowsCount = 0;
        runtime.SetObserverFunc([&uploadRowsCount](TAutoPtr<IEventHandle>& ev) {
            uploadRowsCount += ui32(ev->GetTypeRewrite() == TEvDataShard::EvS3UploadRowsResponse);
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const auto data = GenerateZstdTestData(TString(100, 'a'), 2); // 2 rows, 1 row = 113b
        // ensure that one decompressed row is bigger than entire compressed file
        UNIT_ASSERT(data.Data.size() < *runtime.GetAppData().ZstdBlockSizeForTest);

        Restore(runtime, env, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {data}, data.Data.size());

        UNIT_ASSERT_VALUES_EQUAL(uploadRowsCount, 2);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldExpandBuffer) {
        TTestBasicRuntime runtime;

        const auto data = GenerateTestData(Codec, "a", 2);
        const ui32 batchSize = 1;

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {data}, batchSize);

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.YsonStr, content);
    }

    Y_UNIT_TEST(ShouldSucceedOnSupportedDatatypes) {
        TTestBasicRuntime runtime;

        TString csv = TStringBuilder()
            << "1," // key
            << "-100500," // int32
            << "100500," // uint32
            << "-200500," // int64
            << "200500," // uint64
            << "255," // uint8
            << "1," // bool
            << "1.1234," // double
            << "-1.123," // float
            << "2020-08-12T00:00:00.000000Z," // date
            << "2020-08-12T12:34:56.000000Z," // datetime
            << "2020-08-12T12:34:56.123456Z," // timestamp
            << "-300500," // interval
            << "-18486," // negative date32
            << "-1597235696," // negative datetime64
            << "-1597235696123456," // negative timestamp64
            << "-300500," // negative interval64
            << "3.321," // decimal
            << ".3321e1," // dynumber
            << "\"" << CGIEscapeRet("lorem ipsum") << "\"," // string
            << "\"" << CGIEscapeRet("lorem ipsum dolor sit amet") << "\"," // utf8
            << "\"" << CGIEscapeRet(R"({"key": "value"})") << "\"," // json
            << "\"" << CGIEscapeRet(R"({"key": "value"})") << "\"," // jsondoc
            << "65df1ec1-a97d-47b2-ae56-3c023da6ee8c"
        << Endl;

        TString yson = TStringBuilder() << "[[[[["
            << "[%true];" // bool
            << "[\"" << -18486 << "\"];" // date32
            << "[\"" << TInstant::ParseIso8601("2020-08-12T00:00:00.000000Z").Days() << "\"];" // date
            << "[\"" << -1597235696 << "\"];" // datetime64
            << "[\"" << TInstant::ParseIso8601("2020-08-12T12:34:56.000000Z").Seconds() << "\"];" // datetime
            << "[\"" << "3.321" << "\"];" // decimal
            << "[\"" << 1.1234 << "\"];" // double
            << "[\"" << ".3321e1" << "\"];" // dynumber
            << "[\"" << -1.123f << "\"];" // float
            << "[\"" << -100500 << "\"];" // int32
            << "[\"" << -200500 << "\"];" // int64
            << "[\"" << -300500 << "\"];" // interval64
            << "[\"" << -300500 << "\"];" // interval
            << "[\"" << "{\\\"key\\\": \\\"value\\\"}" << "\"];" // json
            << "[\"" << "{\\\"key\\\":\\\"value\\\"}" << "\"];" // jsondoc
            << "[\"" << 1 << "\"];" // key
            << "[\"" << "lorem ipsum" << "\"];" // string
            << "[\"" << -1597235696123456 << "\"];" // timestamp64
            << "[\"" << TInstant::ParseIso8601("2020-08-12T12:34:56.123456Z").MicroSeconds() << "\"];" // timestamp
            << "[\"" << 100500 << "\"];" // uint32
            << "[\"" << 200500 << "\"];" // uint64
            << "[\"" << 255 << "\"];" // uint8
            << "[\"" << "lorem ipsum dolor sit amet" << "\"];" // utf8
            << "[[\"" << "wR7fZX2pskeuVjwCPabujA==" << "\"]]" // uuid
        << "]];\%false]]]";

        const auto data = TTestData(std::move(csv), std::move(yson));

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "int32_value" Type: "Int32" }
            Columns { Name: "uint32_value" Type: "Uint32" }
            Columns { Name: "int64_value" Type: "Int64" }
            Columns { Name: "uint64_value" Type: "Uint64" }
            Columns { Name: "uint8_value" Type: "Uint8" }
            Columns { Name: "bool_value" Type: "Bool" }
            Columns { Name: "double_value" Type: "Double" }
            Columns { Name: "float_value" Type: "Float" }
            Columns { Name: "date_value" Type: "Date" }
            Columns { Name: "datetime_value" Type: "Datetime" }
            Columns { Name: "timestamp_value" Type: "Timestamp" }
            Columns { Name: "interval_value" Type: "Interval" }
            Columns { Name: "date32_value" Type: "Date32" }
            Columns { Name: "datetime64_value" Type: "Datetime64" }
            Columns { Name: "timestamp64_value" Type: "Timestamp64" }
            Columns { Name: "interval64_value" Type: "Interval64" }
            Columns { Name: "decimal_value" Type: "Decimal" }
            Columns { Name: "dynumber_value" Type: "DyNumber" }
            Columns { Name: "string_value" Type: "String" }
            Columns { Name: "utf8_value" Type: "Utf8" }
            Columns { Name: "json_value" Type: "Json" }
            Columns { Name: "jsondoc_value" Type: "JsonDocument" }
            Columns { Name: "uuid_value" Type: "Uuid" }
            KeyColumnNames: ["key"]
        )", {data}, data.Data.size() + 1);

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key", "Uint64", "0"}, {
            "key",
            "int32_value",
            "uint32_value",
            "int64_value",
            "uint64_value",
            "uint8_value",
            "bool_value",
            "double_value",
            "float_value",
            "date_value",
            "datetime_value",
            "timestamp_value",
            "interval_value",
            "date32_value",
            "datetime64_value",
            "timestamp64_value",
            "interval64_value",
            "decimal_value",
            "dynumber_value",
            "string_value",
            "utf8_value",
            "json_value",
            "jsondoc_value",
            "uuid_value",
        });
        NKqp::CompareYson(data.YsonStr, content);
    }

    Y_UNIT_TEST(ShouldRestoreSpecialFpValues) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Original"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "double_value" Type: "Double" }
            Columns { Name: "float_value" Type: "Float" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        auto writeRow = [&](ui64 key, double doubleValue, float floatValue) {
            NKikimrMiniKQL::TResult result;
            TString error;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, TTestTxConfig::FakeHiveTablets, Sprintf(R"(
                (
                    (let key '( '('key (Uint32 '%lu) ) ) )
                    (let row '( '('double_value (Double '%lf ) ) '('float_value (Float '%f) ) ) )
                    (return (AsList (UpdateRow '__user__Original key row) ))
                )
            )", key, doubleValue, floatValue), result, error);

            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
            UNIT_ASSERT_VALUES_EQUAL(error, "");
        };

        using double_conversion::Double;

        writeRow(1, Double::NaN(), static_cast<float>(Double::NaN()));
        writeRow(2, -Double::NaN(), static_cast<float>(-Double::NaN()));
        writeRow(3, Double::Infinity(), static_cast<float>(Double::Infinity()));
        writeRow(4, -Double::Infinity(), static_cast<float>(-Double::Infinity()));

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Original"
                destination_prefix: ""
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetExport(runtime, txId, "/MyRoot");

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Restored"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetImport(runtime, txId, "/MyRoot");
    }

    Y_UNIT_TEST(ShouldRestoreDefaultValuesFromLiteral) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Original"
            Columns { Name: "key" Type: "Utf8" }
            Columns {
                Name: "value"
                Type: "Utf8"
                DefaultFromLiteral {
                    type {
                        optional_type {
                            item {
                                type_id: UTF8
                            }
                        }
                    }
                    value {
                        items {
                            text_value: "value1"
                        }
                    }
                }
            }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Original"
                destination_prefix: ""
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetExport(runtime, txId, "/MyRoot");

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Restored"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetImport(runtime, txId, "/MyRoot");

        const auto desc = DescribePath(runtime, "/MyRoot/Restored", true, true);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

        const auto& table = desc.GetPathDescription().GetTable();

        UNIT_ASSERT_C(CheckDefaultFromLiteral(table), "Invalid default value");
    }

    Y_UNIT_TEST(ShouldRestoreDefaultValuesFromSequence) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Original"
                Columns { Name: "key" Type: "Uint64" DefaultFromSequence: "myseq" }
                Columns { Name: "value" Type: "Uint64" }
                KeyColumnNames: ["key"]
            }
            SequenceDescription {
                Name: "myseq"
            }
        )");

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Original"
                destination_prefix: ""
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetExport(runtime, txId, "/MyRoot");

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Restored"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetImport(runtime, txId, "/MyRoot");

        const auto desc = DescribePath(runtime, "/MyRoot/Restored", true, true);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

        const auto& table = desc.GetPathDescription().GetTable();

        UNIT_ASSERT_C(CheckDefaultFromSequence(table), "Invalid default value");
    }

    Y_UNIT_TEST(ShouldRestoreSequence) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCEPROXY, NActors::NLog::PRI_TRACE);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Original"
                Columns { Name: "key" Type: "Uint64" DefaultFromSequence: "myseq" }
                Columns { Name: "value" Type: "Uint64" }
                KeyColumnNames: ["key"]
            }
            SequenceDescription {
                Name: "myseq"
            }
        )");
        env.TestWaitNotification(runtime, txId);

        i64 value = DoNextVal(runtime, "/MyRoot/Original/myseq");
        UNIT_ASSERT_VALUES_EQUAL(value, 1);

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Original"
                destination_prefix: ""
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetExport(runtime, txId, "/MyRoot");

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Restored"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetImport(runtime, txId, "/MyRoot");

        const auto desc = DescribePath(runtime, "/MyRoot/Restored", true, true);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

        const auto& table = desc.GetPathDescription().GetTable();

        value = DoNextVal(runtime, "/MyRoot/Restored/myseq");
        UNIT_ASSERT_VALUES_EQUAL(value, 2);

        UNIT_ASSERT_C(CheckDefaultFromSequence(table), "Invalid default value");
    }

    Y_UNIT_TEST(ShouldRestoreSequenceWithOverflow) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        ui64 txId = 100;

        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SEQUENCEPROXY, NActors::NLog::PRI_TRACE);

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
                Name: "Original"
                Columns { Name: "key" Type: "Uint64" DefaultFromSequence: "myseq" }
                Columns { Name: "value" Type: "Uint64" }
                KeyColumnNames: ["key"]
            }
            SequenceDescription {
                Name: "myseq"
                MinValue: 1
                MaxValue: 2
            }
        )");
        env.TestWaitNotification(runtime, txId);

        i64 value = DoNextVal(runtime, "/MyRoot/Original/myseq");
        UNIT_ASSERT_VALUES_EQUAL(value, 1);

        value = DoNextVal(runtime, "/MyRoot/Original/myseq");
        UNIT_ASSERT_VALUES_EQUAL(value, 2);

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Original"
                destination_prefix: ""
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetExport(runtime, txId, "/MyRoot");

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Restored"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetImport(runtime, txId, "/MyRoot");

        const auto desc = DescribePath(runtime, "/MyRoot/Restored", true, true);
        UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

        const auto& table = desc.GetPathDescription().GetTable();

        value = DoNextVal(runtime, "/MyRoot/Restored/myseq", Ydb::StatusIds::SCHEME_ERROR);

        UNIT_ASSERT_C(CheckDefaultFromSequence(table), "Invalid default value");
    }

    Y_UNIT_TEST(ExportImportOnSupportedDatatypes) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "int32_value" Type: "Int32" }
            Columns { Name: "uint32_value" Type: "Uint32" }
            Columns { Name: "int64_value" Type: "Int64" }
            Columns { Name: "uint64_value" Type: "Uint64" }
            Columns { Name: "uint8_value" Type: "Uint8" }
            Columns { Name: "bool_value" Type: "Bool" }
            Columns { Name: "double_value" Type: "Double" }
            Columns { Name: "float_value" Type: "Float" }
            Columns { Name: "date_value" Type: "Date" }
            Columns { Name: "datetime_value" Type: "Datetime" }
            Columns { Name: "timestamp_value" Type: "Timestamp" }
            Columns { Name: "interval_value" Type: "Interval" }
            Columns { Name: "date32_value" Type: "Date32" }
            Columns { Name: "datetime64_value" Type: "Datetime64" }
            Columns { Name: "timestamp64_value" Type: "Timestamp64" }
            Columns { Name: "interval64_value" Type: "Interval64" }
            Columns { Name: "decimal_value" Type: "Decimal" }
            Columns { Name: "dynumber_value" Type: "DyNumber" }
            Columns { Name: "string_value" Type: "String" }
            Columns { Name: "utf8_value" Type: "Utf8" }
            Columns { Name: "json_value" Type: "Json" }
            Columns { Name: "jsondoc_value" Type: "JsonDocument" }
            Columns { Name: "uuid_value" Type: "Uuid" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        const int partitionIdx = 0;

        const TVector<TCell> keys = {TCell::Make(1ull)};

        const TString string = "test string";
        const TString json = R"({"key": "value"})";
        auto binaryJson = NBinaryJson::SerializeToBinaryJson(json);
        Y_ABORT_UNLESS(binaryJson.Defined());

        const std::pair<ui64, ui64> decimal = NYql::NDecimal::MakePair(NYql::NDecimal::FromString("16.17", NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE));
        const TString dynumber = *NDyNumber::ParseDyNumberString("18");

        char uuid[16];
        NUuid::ParseUuidToArray(TString("65df1ec1-a97d-47b2-ae56-3c023da6ee8c"), reinterpret_cast<ui16*>(uuid), false);

        const TVector<TCell> values = {
            TCell::Make<i32>(-1), // Int32
            TCell::Make<ui32>(2), // Uint32
            TCell::Make<i64>(-3), // Int64
            TCell::Make<ui64>(4), // Uint64
            TCell::Make<ui8>(5), // Uint8
            TCell::Make<bool>(true), // Bool
            TCell::Make<double>(6.66), // Double
            TCell::Make<float>(7.77), // Float
            TCell::Make<ui16>(8), // Date
            TCell::Make<ui32>(9), // Datetime
            TCell::Make<ui64>(10), // Timestamp
            TCell::Make<i64>(-11), // Interval
            TCell::Make<i32>(-12), // Date32
            TCell::Make<i64>(-13), // Datetime64
            TCell::Make<i64>(-14), // Timestamp64
            TCell::Make<i64>(-15), // Interval64
            TCell::Make<std::pair<ui64, ui64>>(decimal), // Decimal
            TCell(dynumber.data(), dynumber.size()), // Dynumber
            TCell(string.data(), string.size()), // String
            TCell(string.data(), string.size()), // Utf8
            TCell(json.data(), json.size()), // Json
            TCell(binaryJson->Data(), binaryJson->Size()), // JsonDocument
            TCell(uuid, sizeof(uuid)), // Uuid
        };

        const TVector<ui32> keyTags = {1};
        TVector<ui32> valueTags(values.size());
        std::iota(valueTags.begin(), valueTags.end(), 2);            

        UploadRow(runtime, "/MyRoot/Table", partitionIdx, keyTags, valueTags, keys, values);

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: "Backup1"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetExport(runtime, txId, "/MyRoot");

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: "Backup1"
                destination_path: "/MyRoot/Restored"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetImport(runtime, txId, "/MyRoot");


        TString expectedJson = TStringBuilder() << "[[[[["
            << "[%true];" // bool
            << "[\"" << -12 << "\"];" // date32
            << "[\"" << 8 << "\"];" // date
            << "[\"" << -13 << "\"];" // datetime64
            << "[\"" << 9 << "\"];" // datetime
            << "[\"" << "16.17" << "\"];" // decimal
            << "[\"" << 6.66 << "\"];" // double
            << "[\"" << ".18e2" << "\"];" // dynumber
            << "[\"" << 7.77f << "\"];" // float
            << "[\"" << -1 << "\"];" // int32
            << "[\"" << -3 << "\"];" // int64
            << "[\"" << -15 << "\"];" // interval64
            << "[\"" << -11 << "\"];" // interval
            << "[\"" << "{\\\"key\\\": \\\"value\\\"}" << "\"];" // json
            << "[\"" << "{\\\"key\\\":\\\"value\\\"}" << "\"];" // jsondoc
            << "[\"" << 1 << "\"];" // key
            << "[\"" << "test string" << "\"];" // string
            << "[\"" << -14 << "\"];" // timestamp64
            << "[\"" << 10 << "\"];" // timestamp
            << "[\"" << 2 << "\"];" // uint32
            << "[\"" << 4 << "\"];" // uint64
            << "[\"" << 5 << "\"];" // uint8
            << "[\"" << "test string" << "\"];" // utf8
            << "[[\"" << "wR7fZX2pskeuVjwCPabujA==" << "\"]]" // uuid
        << "]];\%false]]]";

        const TReadKeyDesc readKeyDesc = {"key", "Uint64", "0"};

        const TVector<TString> readColumns = {
            "key",
            "int32_value",
            "uint32_value",
            "int64_value",
            "uint64_value",
            "uint8_value",
            "bool_value",
            "double_value",
            "float_value",
            "date_value",
            "datetime_value",
            "timestamp_value",
            "interval_value",
            "date32_value",
            "datetime64_value",
            "timestamp64_value",
            "interval64_value",
            "decimal_value",
            "dynumber_value",
            "string_value",
            "utf8_value",
            "json_value",
            "jsondoc_value",
            "uuid_value",
        };
        
        auto contentOriginalTable = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", readKeyDesc, readColumns);
        NKqp::CompareYson(expectedJson, contentOriginalTable);

        auto contentRestoredTable = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 2, "Restored", readKeyDesc, readColumns);
        NKqp::CompareYson(expectedJson, contentRestoredTable);
    }

    Y_UNIT_TEST(ExportImportPg) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableTablePgTypes(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "pgint4" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        UploadRow(runtime, "/MyRoot/Table", 0, {1}, {2}, {TCell::Make(55555u)}, {TCell::Make(55555u)});

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: "Backup1"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetExport(runtime, txId, "/MyRoot");

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: "Backup1"
                destination_path: "/MyRoot/Restored"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetImport(runtime, txId, "/MyRoot");
    }

    Y_UNIT_TEST(ExportImportUuid) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableTablePgTypes(true));
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Uuid" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        {
            TString tablePath = "/MyRoot/Table";
            int partitionIdx = 0;

            auto tableDesc = DescribePath(runtime, tablePath, true, true);
            const auto& tablePartitions = tableDesc.GetPathDescription().GetTablePartitions();
            UNIT_ASSERT(partitionIdx < tablePartitions.size());
            const ui64 datashardTabletId = tablePartitions[partitionIdx].GetDatashardId();

            NKikimrMiniKQL::TResult result;
            TString error;
            NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, datashardTabletId, Sprintf(R"(
                (
                    (let key '( '('key (Uint32 '%d) ) ) )
                    (let row '( '('value (Uuid '"%s") ) ) )
                    (return (AsList (UpdateRow '__user__%s key row) ))
                )
            )", 1, "0123456789012345", "Table"), result, error);

            UNIT_ASSERT_VALUES_EQUAL_C(status, NKikimrProto::EReplyStatus::OK, error);
            UNIT_ASSERT_VALUES_EQUAL(error, "");
        }

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock({}, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TestExport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ExportToS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_path: "/MyRoot/Table"
                destination_prefix: "Backup1"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetExport(runtime, txId, "/MyRoot");

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: "Backup1"
                destination_path: "/MyRoot/Restored"
              }
            }
        )", port));
        env.TestWaitNotification(runtime, txId);
        TestGetImport(runtime, txId, "/MyRoot");
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldCountWrittenBytesAndRows) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto data = GenerateTestData(Codec, "a", 2);

        TMaybe<NKikimrTxDataShard::TShardOpResult> result;
        runtime.SetObserverFunc([&result](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvDataShard::EvSchemaChanged) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            const auto& record = ev->Get<TEvDataShard::TEvSchemaChanged>()->Record;
            if (!record.HasOpResult()) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            result = record.GetOpResult();
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Restore(runtime, env, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {data});

        if (!result) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&result](IEventHandle&) -> bool {
                return result.Defined();
            });
            runtime.DispatchEvents(opts);
        }

        UNIT_ASSERT_VALUES_EQUAL(result->GetBytesProcessed(), 16);
        UNIT_ASSERT_VALUES_EQUAL(result->GetRowsProcessed(), 2);
    }

    Y_UNIT_TEST(ShouldHandleOverloadedShard) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        // prepare table schema with special policy
        NKikimrSchemeOp::TTableDescription desc;
        desc.SetName("Table");
        desc.AddKeyColumnNames("key");
        {
            auto& column = *desc.AddColumns();
            column.SetName("key");
            column.SetType("Uint32");
        }
        {
            auto& column = *desc.AddColumns();
            column.SetName("value");
            column.SetType("Utf8");
        }

        auto policy = NLocalDb::CreateDefaultUserTablePolicy();
        policy->InMemForceSizeToSnapshot = 1;
        policy->Serialize(*desc.MutablePartitionConfig()->MutableCompactionPolicy());

        // serialize schema
        TString scheme;
        UNIT_ASSERT(google::protobuf::TextFormat::PrintToString(desc, &scheme));
        TestCreateTable(runtime, ++txId, "/MyRoot", scheme);
        env.TestWaitNotification(runtime, txId);

        ui32 requests = 0;
        ui32 responses = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            requests += ui32(ev->GetTypeRewrite() == TEvDataShard::EvS3UploadRowsRequest);
            responses += ui32(ev->GetTypeRewrite() == TEvDataShard::EvS3UploadRowsResponse);
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        TPortManager portManager;
        THolder<TS3Mock> s3Mock;

        const auto data = GenerateTestData("", 1000);
        const ui32 batchSize = 32;
        RestoreNoWait(runtime, ++txId, portManager.GetPort(), s3Mock, {data}, batchSize);
        env.TestWaitNotification(runtime, txId);

        const ui32 expected = data.Data.size() / batchSize + ui32(bool(data.Data.size() % batchSize));
        UNIT_ASSERT(requests > expected);
        UNIT_ASSERT_VALUES_EQUAL(responses, expected);

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key", "Uint32", "0"});
        NKqp::CompareYson(data.YsonStr, content);
    }

    template <ECompressionCodec Codec>
    void ShouldFailOnFileWithoutNewLines(ui32 batchSize) {
        TTestBasicRuntime runtime;

        const TString v = "\"a1\",\"value1\"";
        const auto d = Codec == ECompressionCodec::Zstd ? ZstdCompress(v) : v;
        const auto data = TTestData(d, EmptyYsonStr, Codec);

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {data}, batchSize);

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.YsonStr, content);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldFailOnFileWithoutNewLinesStandardBatch) {
        ShouldFailOnFileWithoutNewLines<Codec>(128);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldFailOnFileWithoutNewLinesSmallBatch) {
        ShouldFailOnFileWithoutNewLines<Codec>(1);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldFailOnEmptyToken) {
        TTestBasicRuntime runtime;

        const TString v = "\"a1\",\n";
        const auto d = Codec == ECompressionCodec::Zstd ? ZstdCompress(v) : v;
        const auto data = TTestData(d, EmptyYsonStr, Codec);

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {data});

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.YsonStr, content);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldFailOnInvalidValue) {
        TTestBasicRuntime runtime;

        const TString v = "\"a1\",\"value1\"\n";
        const auto d = Codec == ECompressionCodec::Zstd ? ZstdCompress(v) : v;
        const auto data = TTestData(d, EmptyYsonStr, Codec);

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {data});

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key", "Uint64", "0"});
        NKqp::CompareYson(data.YsonStr, content);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldFailOnOutboundKey) {
        TTestBasicRuntime runtime;

        const auto a = GenerateTestData(Codec, "a", 1);
        const auto b = TTestData(a.Data, EmptyYsonStr);

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            SplitBoundary {
              KeyPrefix {
                Tuple { Optional { Text: "b" } }
              }
            }
        )", {a, b});

        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 0);
            NKqp::CompareYson(a.YsonStr, content);
        }
        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 1);
            NKqp::CompareYson(b.YsonStr, content);
        }
    }

    Y_UNIT_TEST(ShouldFailOnInvalidFrame) {
        TTestBasicRuntime runtime;

        const TString garbage = "\"a1\",\"value1\""; // not valid zstd data
        const auto data = TTestData(garbage, EmptyYsonStr, ECompressionCodec::Zstd);

        Restore(runtime, R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )", {data});

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.YsonStr, content);
    }

    void TestRestoreNegative(TTestActorRuntime& runtime, ui64 txId, const TString& parentPath, const TString& name,
            const TVector<TExpectedResult>& expectedResults) {

        TestRestore(runtime, ++txId, parentPath, Sprintf(R"(
            TableName: "%s"
            S3Settings {
                Endpoint: "localhost"
                Scheme: HTTP
            }
        )", name.data()), expectedResults);
    }

    Y_UNIT_TEST(ShouldFailOnVariousErrors) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        TestRestoreNegative(runtime, ++txId, "/MyRoot", "Table", {NKikimrScheme::StatusMultipleModifications});
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestMkDir(runtime, ++txId, "/MyRoot", "Dir");
        TestRestoreNegative(runtime, ++txId, "/MyRoot", "Dir", {NKikimrScheme::StatusNameConflict});
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestRestoreNegative(runtime, ++txId, "/MyRoot", "Dir", {NKikimrScheme::StatusNameConflict});
        TestRestoreNegative(runtime, ++txId, "/MyRoot", "NotExist", {NKikimrScheme::StatusPathDoesNotExist});

        TestAlterTable(runtime, ++txId, "/MyRoot",R"(
            Name: "Table"
            Columns { Name: "extra"  Type: "Utf8"}
        )");
        TestRestoreNegative(runtime, ++txId, "/MyRoot", "Table", {NKikimrScheme::StatusMultipleModifications});
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestDropTable(runtime, ++txId, "/MyRoot", "Table");
        TestRestoreNegative(runtime, ++txId, "/MyRoot", "Table", {NKikimrScheme::StatusMultipleModifications});
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestCreateIndexedTable(runtime, ++txId, "/MyRoot", R"(
            TableDescription {
              Name: "IndexedTable"
              Columns { Name: "key" Type: "Utf8" }
              Columns { Name: "value" Type: "Utf8" }
              KeyColumnNames: ["key"]
            }
            IndexDescription {
              Name: "ByValue"
              KeyColumnNames: ["value"]
            }
        )");
        TestRestoreNegative(runtime, ++txId, "/MyRoot", "IndexedTable", {NKikimrScheme::StatusMultipleModifications});
        env.TestWaitNotification(runtime, {txId - 1, txId});

        TestRestoreNegative(runtime, ++txId, "/MyRoot", "IndexedTable", {NKikimrScheme::StatusInvalidParameter});
    }

    template <typename TEvToDelay>
    void CancelShouldSucceed(const TTestData& data, bool kill = false) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        THolder<IEventHandle> delayed;
        auto prevObserver = SetDelayObserver(runtime, delayed, [](TAutoPtr<IEventHandle>& ev) {
            return ev->GetTypeRewrite() == TEvToDelay::EventType;
        });

        TPortManager portManager;
        THolder<TS3Mock> s3Mock;

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        RestoreNoWait(runtime, txId, portManager.GetPort(), s3Mock, {data});
        const ui64 restoreTxId = txId;

        if (kill) {
            s3Mock.Destroy();
        }

        WaitForDelayed(runtime, delayed, prevObserver);

        runtime.Send(delayed.Release(), 0, true);
        TestCancelTxTable(runtime, ++txId, restoreTxId);
        env.TestWaitNotification(runtime, {restoreTxId, txId});

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.YsonStr, content);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(CancelUponProposeShouldSucceed) {
        auto data = GenerateTestData(Codec, "a", 1);
        data.YsonStr = EmptyYsonStr;
        CancelShouldSucceed<TEvDataShard::TEvProposeTransaction>(data);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(CancelUponProposeResultShouldSucceed) {
        auto data = GenerateTestData(Codec, "a", 1);
        data.YsonStr = EmptyYsonStr;
        CancelShouldSucceed<TEvDataShard::TEvProposeTransactionResult>(data);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(CancelUponUploadResponseShouldSucceed) {
        const auto data = GenerateTestData(Codec, "a", 1);
        CancelShouldSucceed<TEvDataShard::TEvS3UploadRowsResponse>(data);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(CancelHungOperationShouldSucceed) {
        auto data = GenerateTestData(Codec, "a", 1);
        data.YsonStr = EmptyYsonStr;
        CancelShouldSucceed<TEvDataShard::TEvProposeTransactionResult>(data, true);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(CancelAlmostCompleteOperationShouldNotHaveEffect) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Utf8" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        THolder<IEventHandle> schemaChanged;
        auto prevObserver = SetDelayObserver(runtime, schemaChanged, [](TAutoPtr<IEventHandle>& ev) {
            return ev->GetTypeRewrite() == TEvDataShard::TEvSchemaChanged::EventType;
        });

        TPortManager portManager;
        THolder<TS3Mock> s3Mock;
        const auto data = GenerateTestData(Codec, "a", 1);

        RestoreNoWait(runtime, txId, portManager.GetPort(), s3Mock, {data});
        const ui64 restoreTxId = txId;

        WaitForDelayed(runtime, schemaChanged, prevObserver);

        THolder<IEventHandle> progress;
        prevObserver = SetDelayObserver(runtime, progress, [](TAutoPtr<IEventHandle>& ev) {
            return ev->GetTypeRewrite() == TEvPrivate::TEvProgressOperation::EventType;
        });

        TestCancelTxTable(runtime, ++txId, restoreTxId);
        WaitForDelayed(runtime, progress, prevObserver);

        runtime.Send(schemaChanged.Release(), 0, true);
        runtime.Send(progress.Release(), 0, true);
        env.TestWaitNotification(runtime, {restoreTxId, txId});

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.YsonStr, content);
    }
}

Y_UNIT_TEST_SUITE(TRestoreWithRebootsTests) {
    void Restore(TTestWithReboots& t, TTestActorRuntime& runtime, bool& activeZone,
            ui16 port, const TString& creationScheme, TVector<TTestData>&& data, ui32 readBatchSize = 128) {

        THolder<TS3Mock> s3Mock;
        TString schemeStr;

        {
            TInactiveZone inactive(activeZone);

            TestCreateTable(runtime, ++t.TxId, "/MyRoot", creationScheme);
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            const auto desc = DescribePath(runtime, "/MyRoot/Table", true, true);
            UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

            s3Mock.Reset(new TS3Mock(ConvertTestData({GenerateScheme(desc), std::move(data)}), TS3Mock::TSettings(port)));
            UNIT_ASSERT(s3Mock->Start());

            runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
            schemeStr = GenerateTableDescription(desc);
        }

        TestRestore(runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
            TableName: "Table"
            TableDescription {
                %s
            }
            S3Settings {
                Endpoint: "localhost:%d"
                Scheme: HTTP
                Limits {
                    ReadBatchSize: %d
                }
            }
        )", schemeStr.data(), port, readBatchSize));
        t.TestEnv->TestWaitNotification(runtime, t.TxId);
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldSucceedOnSingleShardTable) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const auto data = GenerateTestData(Codec, "a", 1);

            Restore(t, runtime, activeZone, port, R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )", {data});

            {
                TInactiveZone inactive(activeZone);

                auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
                NKqp::CompareYson(data.YsonStr, content);
            }
        });
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldSucceedOnMultiShardTable) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const auto a = GenerateTestData(Codec, "a", 1);
            const auto b = GenerateTestData(Codec, "b", 1);

            Restore(t, runtime, activeZone, port, R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                SplitBoundary {
                  KeyPrefix {
                    Tuple { Optional { Text: "b" } }
                  }
                }
            )", {a, b});

            {
                TInactiveZone inactive(activeZone);
                {
                    auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 0);
                    NKqp::CompareYson(a.YsonStr, content);
                }
                {
                    auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 1);
                    NKqp::CompareYson(b.YsonStr, content);
                }
            }
        });
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldSucceedOnMultiShardTableAndLimitedResources) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                using namespace NResourceBroker;

                auto config = MakeDefaultConfig();
                for (auto& queue : *config.MutableQueues()) {
                    if (queue.GetName() == "queue_restore") {
                        queue.MutableLimit()->SetCpu(1);
                        break;
                    }
                }

                runtime.RegisterService(MakeResourceBrokerID(),
                    runtime.Register(CreateResourceBrokerActor(config, runtime.GetDynamicCounters(0))));
            }

            const auto a = GenerateTestData(Codec, "a", 1);
            const auto b = GenerateTestData(Codec, "b", 1);

            Restore(t, runtime, activeZone, port, R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                SplitBoundary {
                  KeyPrefix {
                    Tuple { Optional { Text: "b" } }
                  }
                }
            )", {a, b});

            {
                TInactiveZone inactive(activeZone);
                {
                    auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 0);
                    NKqp::CompareYson(a.YsonStr, content);
                }
                {
                    auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 1);
                    NKqp::CompareYson(b.YsonStr, content);
                }
            }
        });
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldSucceedOnLargeData) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const auto data = GenerateTestData(Codec, "", 100);
            UNIT_ASSERT(data.Data.size() > 128);

            Restore(t, runtime, activeZone, port, R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint32" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )", {data});

            {
                TInactiveZone inactive(activeZone);

                auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key", "Uint32", "0"});
                NKqp::CompareYson(data.YsonStr, content);
            }
        });
    }

    Y_UNIT_TEST(ShouldSucceedOnMultipleFrames) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const auto data = GenerateZstdTestData("a", 3, 2);
            const ui32 batchSize = 7; // less than any frame

            Restore(t, runtime, activeZone, port, R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )", {data}, batchSize);

            {
                TInactiveZone inactive(activeZone);

                auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
                NKqp::CompareYson(data.YsonStr, content);
            }
        });
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldFailOnFileWithoutNewLines) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const TString v = "\"a1\",\"value1\"";
            const auto d = Codec == ECompressionCodec::Zstd ? ZstdCompress(v) : v;
            const auto data = TTestData(d, EmptyYsonStr, Codec);

            Restore(t, runtime, activeZone, port, R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )", {data});

            {
                TInactiveZone inactive(activeZone);

                auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
                NKqp::CompareYson(data.YsonStr, content);
            }
        });
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldFailOnEmptyToken) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const TString v = "\"a1\",\n";
            const auto d = Codec == ECompressionCodec::Zstd ? ZstdCompress(v) : v;
            const auto data = TTestData(d, EmptyYsonStr, Codec);

            Restore(t, runtime, activeZone, port, R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )", {data});

            {
                TInactiveZone inactive(activeZone);

                auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
                NKqp::CompareYson(data.YsonStr, content);
            }
        });
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldFailOnInvalidValue) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const TString v = "\"a1\",\"value1\"\n";
            const auto d = Codec == ECompressionCodec::Zstd ? ZstdCompress(v) : v;
            const auto data = TTestData(d, EmptyYsonStr, Codec);

            Restore(t, runtime, activeZone, port, R"(
                Name: "Table"
                Columns { Name: "key" Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )", {data});

            {
                TInactiveZone inactive(activeZone);

                auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets, "Table", {"key", "Uint64", "0"});
                NKqp::CompareYson(data.YsonStr, content);
            }
        });
    }

    Y_UNIT_TEST_WITH_COMPRESSION(ShouldFailOnOutboundKey) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            const auto a = GenerateTestData(Codec, "a", 1);
            const auto b = TTestData(a.Data, EmptyYsonStr);

            Restore(t, runtime, activeZone, port, R"(
                Name: "Table"
                Columns { Name: "key" Type: "Utf8" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
                SplitBoundary {
                  KeyPrefix {
                    Tuple { Optional { Text: "b" } }
                  }
                }
            )", {a, b});

            {
                TInactiveZone inactive(activeZone);
                {
                    auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 0);
                    NKqp::CompareYson(a.YsonStr, content);
                }
                {
                    auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 1);
                    NKqp::CompareYson(b.YsonStr, content);
                }
            }
        });
    }

    Y_UNIT_TEST_WITH_COMPRESSION(CancelShouldSucceed) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();
        const auto data = GenerateTestData(Codec, "a", 1);

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            THolder<TS3Mock> s3Mock;
            TString schemeStr;

            {
                TInactiveZone inactive(activeZone);

                TestCreateTable(runtime, ++t.TxId, "/MyRoot", R"(
                    Name: "Table"
                    Columns { Name: "key" Type: "Utf8" }
                    Columns { Name: "value" Type: "Utf8" }
                    KeyColumnNames: ["key"]
                )");
                t.TestEnv->TestWaitNotification(runtime, t.TxId);

                const auto desc = DescribePath(runtime, "/MyRoot/Table", true, true);
                UNIT_ASSERT_VALUES_EQUAL(desc.GetStatus(), NKikimrScheme::StatusSuccess);

                s3Mock.Reset(new TS3Mock(ConvertTestData({GenerateScheme(desc), {data}}), TS3Mock::TSettings(port)));
                UNIT_ASSERT(s3Mock->Start());

                runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
                schemeStr = GenerateTableDescription(desc);
            }

            AsyncRestore(runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                TableName: "Table"
                TableDescription {
                    %s
                }
                S3Settings {
                    Endpoint: "localhost:%d"
                    Scheme: HTTP
                    Limits {
                        ReadBatchSize: 128
                    }
                }
            )", schemeStr.data(), port));
            const ui64 restoreTxId = t.TxId;

            t.TestEnv->ReliablePropose(runtime, CancelTxRequest(++t.TxId, restoreTxId), {
                NKikimrScheme::StatusAccepted,
                NKikimrScheme::StatusTxIdNotExists
            });
            t.TestEnv->TestWaitNotification(runtime, {restoreTxId, t.TxId});
        });
    }
}

Y_UNIT_TEST_SUITE(TImportTests) {
    void Run(TTestBasicRuntime& runtime, TTestEnv& env,
            THashMap<TString, TString>&& data, const TString& request,
            Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS,
            const TString& dbName = "/MyRoot", bool serverless = false, const TString& userSID = "")
    {
        ui64 id = 100;

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock(data, TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        ui64 schemeshardId = TTestTxConfig::SchemeShard;
        if (dbName != "/MyRoot") {
            TestCreateExtSubDomain(runtime, ++id, "/MyRoot", Sprintf(R"(
                Name: "%s"
            )", TStringBuf(serverless ? "/MyRoot/Shared" : dbName).RNextTok('/').data()));
            env.TestWaitNotification(runtime, id);

            TestAlterExtSubDomain(runtime, ++id, "/MyRoot", Sprintf(R"(
                PlanResolution: 50
                Coordinators: 1
                Mediators: 1
                TimeCastBucketsPerMediator: 2
                ExternalSchemeShard: true
                Name: "%s"
                StoragePools {
                  Name: "name_User_kind_hdd-1"
                  Kind: "common"
                }
                StoragePools {
                  Name: "name_User_kind_hdd-2"
                  Kind: "external"
                }
            )", TStringBuf(serverless ? "/MyRoot/Shared" : dbName).RNextTok('/').data()));
            env.TestWaitNotification(runtime, id);

            if (serverless) {
                const auto attrs = AlterUserAttrs({
                    {"cloud_id", "CLOUD_ID_VAL"},
                    {"folder_id", "FOLDER_ID_VAL"},
                    {"database_id", "DATABASE_ID_VAL"}
                });

                TestCreateExtSubDomain(runtime, ++id, "/MyRoot", Sprintf(R"(
                    Name: "%s"
                    ResourcesDomainKey {
                        SchemeShard: %lu
                        PathId: 2
                    }
                )", TStringBuf(dbName).RNextTok('/').data(), TTestTxConfig::SchemeShard), attrs);
                env.TestWaitNotification(runtime, id);

                TestAlterExtSubDomain(runtime, ++id, "/MyRoot", Sprintf(R"(
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    ExternalSchemeShard: true
                    ExternalHive: false
                    Name: "%s"
                    StoragePools {
                      Name: "name_User_kind_hdd-1"
                      Kind: "common"
                    }
                    StoragePools {
                      Name: "name_User_kind_hdd-2"
                      Kind: "external"
                    }
                )", TStringBuf(dbName).RNextTok('/').data()));
                env.TestWaitNotification(runtime, id);
            }

            TestDescribeResult(DescribePath(runtime, dbName), {
                NLs::PathExist,
                NLs::ExtractTenantSchemeshard(&schemeshardId)
            });
        }

        runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);

        auto initialStatus = Ydb::StatusIds::SUCCESS;
        switch (expectedStatus) {
        case Ydb::StatusIds::BAD_REQUEST:
        case Ydb::StatusIds::PRECONDITION_FAILED:
            initialStatus = expectedStatus;
            break;
        default:
            break;
        }

        TestImport(runtime, schemeshardId, ++id, dbName, Sprintf(request.data(), port), userSID, initialStatus);
        env.TestWaitNotification(runtime, id, schemeshardId);

        if (initialStatus != Ydb::StatusIds::SUCCESS) {
            return;
        }

        TestGetImport(runtime, schemeshardId, id, dbName, expectedStatus);
    }

    void Run(TTestBasicRuntime& runtime, THashMap<TString, TString>&& data, const TString& request,
            Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS,
            const TString& dbName = "/MyRoot", bool serverless = false, const TString& userSID = "") {

        TTestEnv env(runtime, TTestEnvOptions());
        Run(runtime, env, std::move(data), request, expectedStatus, dbName, serverless, userSID);
    }

    Y_UNIT_TEST(ShouldSucceedOnSingleShardTable) {
        TTestBasicRuntime runtime;

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 1}});

        Run(runtime, ConvertTestData(data), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )");

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.Data[0].YsonStr, content);
    }

    Y_UNIT_TEST(ShouldSucceedOnMultiShardTable) {
        TTestBasicRuntime runtime;

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
            partition_at_keys {
              split_points {
                type { tuple_type { elements { optional_type { item { type_id: UTF8 } } } } }
                value { items { text_value: "b" } }
              }
            }
        )", {{"a", 1}, {"b", 1}});

        Run(runtime, ConvertTestData(data), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )");

        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 0);
            NKqp::CompareYson(data.Data[0].YsonStr, content);
        }
        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 1);
            NKqp::CompareYson(data.Data[1].YsonStr, content);
        }
    }

    void ShouldSucceedOnIndexedTable(ui32 indexes, const TString& indexType = "global_index {}") {
        TTestBasicRuntime runtime;

        auto scheme = TStringBuilder() << R"(
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

        for (ui32 i = 0; i < indexes; ++i) {
            scheme << Sprintf(R"(
                indexes {
                  name: "by_value_%i"
                  index_columns: "value"
                  %s
                }
            )", i + 1, indexType.data());
        }

        const auto data = GenerateTestData(scheme, {{"a", 1}});

        Run(runtime, ConvertTestData(data), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )");

        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 0);
            NKqp::CompareYson(data.Data[0].YsonStr, content);
        }

        for (ui32 i = 0; i < indexes; ++i) {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 1 + i,
                "indexImplTable", {"value", "Utf8", "\"\""}, {"value", "key"}, "'ExcFrom");
            NKqp::CompareYson(data.Data[0].YsonStr, content);
        }
    }

    Y_UNIT_TEST(ShouldSucceedOnIndexedTable1) {
        ShouldSucceedOnIndexedTable(1);
    }

    Y_UNIT_TEST(ShouldSucceedOnIndexedTable2) {
        ShouldSucceedOnIndexedTable(2);
    }

    Y_UNIT_TEST(ShouldSucceedOnIndexedTable3) {
        ShouldSucceedOnIndexedTable(1, "");
    }

    Y_UNIT_TEST(ShouldSucceedOnManyTables) {
        TTestBasicRuntime runtime;

        const auto a = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 1}});

        const auto b = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"b", 1}});

        Run(runtime, ConvertTestData({{"/a", a}, {"/b", b}}), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: "a"
                destination_path: "/MyRoot/TableA"
              }
              items {
                source_prefix: "b"
                destination_path: "/MyRoot/TableB"
              }
            }
        )");

        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 0, "TableA");
            NKqp::CompareYson(a.Data[0].YsonStr, content);
        }
        {
            auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets + 1, "TableB");
            NKqp::CompareYson(b.Data[0].YsonStr, content);
        }
    }

    Y_UNIT_TEST(ShouldSucceedWithoutTableProfiles) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions()
            .RunFakeConfigDispatcher(true));

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 1}});

        Run(runtime, env, ConvertTestData(data), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )");

        auto content = ReadTable(runtime, TTestTxConfig::FakeHiveTablets);
        NKqp::CompareYson(data.Data[0].YsonStr, content);
    }

    Y_UNIT_TEST(ShouldWriteBillRecordOnServerlessDb) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 100}});

        TVector<TString> billRecords;
        runtime.SetObserverFunc([&billRecords](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != NMetering::TEvMetering::EvWriteMeteringJson) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            billRecords.push_back(ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson);
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Run(runtime, env, ConvertTestData(data), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/User/Table"
              }
            }
        )", Ydb::StatusIds::SUCCESS, "/MyRoot/User", true);

        if (billRecords.empty()) {
            TDispatchOptions opts;
            opts.FinalEvents.emplace_back([&billRecords](IEventHandle&) -> bool {
                return !billRecords.empty();
            });
            runtime.DispatchEvents(opts);
        }

        const TString expectedBillRecord = R"({"usage":{"start":0,"quantity":50,"finish":0,"unit":"request_unit","type":"delta"},"tags":{},"id":"281474976725758-72075186233409549-2-72075186233409549-4","cloud_id":"CLOUD_ID_VAL","source_wt":0,"source_id":"sless-docapi-ydb-ss","resource_id":"DATABASE_ID_VAL","schema":"ydb.serverless.requests.v1","folder_id":"FOLDER_ID_VAL","version":"1.0.0"})";

        UNIT_ASSERT_VALUES_EQUAL(billRecords.size(), 1);
        UNIT_ASSERT_NO_DIFF(billRecords[0], expectedBillRecord + "\n");
    }

    Y_UNIT_TEST(ShouldNotWriteBillRecordOnCommonDb) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 100}});

        TVector<TString> billRecords;
        runtime.SetObserverFunc([&billRecords](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != NMetering::TEvMetering::EvWriteMeteringJson) {
                return TTestActorRuntime::EEventAction::PROCESS;
            }

            billRecords.push_back(ev->Get<NMetering::TEvMetering::TEvWriteMeteringJson>()->MeteringJson);
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Run(runtime, env, ConvertTestData(data), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/User/Table"
              }
            }
        )", Ydb::StatusIds::SUCCESS, "/MyRoot/User");

        UNIT_ASSERT(billRecords.empty());
    }

    void ShouldRestoreSettings(const TString& settings, const TVector<NLs::TCheckFunc>& checks) {
        TTestBasicRuntime runtime;

        const auto empty = TTestData("", EmptyYsonStr);
        const auto data = TTestDataWithScheme(TStringBuilder() << R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "created_at"
              type { optional_type { item { type_id: TIMESTAMP } } }
            }
            columns {
              name: "modified_at"
              type { optional_type { item { type_id: UINT32 } } }
            }
            primary_key: "key"
        )" << settings, {empty, empty});

        Run(runtime, ConvertTestData(data), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/User/Table"
              }
            }
        )", Ydb::StatusIds::SUCCESS, "/MyRoot/User");

        ui64 schemeshardId = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/User"), {
            NLs::PathExist,
            NLs::ExtractTenantSchemeshard(&schemeshardId)
        });

        TestDescribeResult(DescribePath(runtime, schemeshardId, "/MyRoot/User/Table", true, true), checks);
    }

    Y_UNIT_TEST(ShouldRestoreTtlSettingsInDateTypeColumnMode) {
        ShouldRestoreSettings(R"(
            ttl_settings {
              date_type_column {
                column_name: "created_at"
                expire_after_seconds: 3600
              }
            }
        )", {
            NLs::HasTtlEnabled("created_at", TDuration::Hours(1)),
        });
    }

    Y_UNIT_TEST(ShouldRestoreTtlSettingsInValueSinceUnixEpochMode) {
        ShouldRestoreSettings(R"(
            ttl_settings {
              value_since_unix_epoch {
                column_name: "modified_at"
                column_unit: UNIT_SECONDS
                expire_after_seconds: 7200
              }
            }
        )", {
            NLs::HasTtlEnabled("modified_at", TDuration::Hours(2), NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS),
        });
    }

    Y_UNIT_TEST(ShouldRestoreStorageSettings) {
        auto check = [](const NKikimrScheme::TEvDescribeSchemeResult& desc) {
            const auto& config = desc.GetPathDescription().GetTable().GetPartitionConfig().GetColumnFamilies(0).GetStorageConfig();

            UNIT_ASSERT_VALUES_EQUAL(config.GetSysLog().GetPreferredPoolKind(), "common");
            UNIT_ASSERT_VALUES_EQUAL(config.GetLog().GetPreferredPoolKind(), "common");
            UNIT_ASSERT_VALUES_EQUAL(config.GetExternal().GetPreferredPoolKind(), "external");
            UNIT_ASSERT(config.HasExternalThreshold());
        };

        ShouldRestoreSettings(R"(
            storage_settings {
              tablet_commit_log0 { media: "common" }
              tablet_commit_log1 { media: "common" }
              external { media: "external" }
              store_external_blobs: ENABLED
            }
        )", {
            check,
        });
    }

    Y_UNIT_TEST(ShouldRestoreColumnFamilies) {
        ShouldRestoreSettings(R"(
            storage_settings {
              tablet_commit_log0 { media: "common" }
              tablet_commit_log1 { media: "common" }
            }
            column_families {
              name: "compressed"
              data { media: "common" }
              compression: COMPRESSION_LZ4
            }
        )", {
            NLs::ColumnFamiliesHas(1, "compressed"),
        });
    }

    Y_UNIT_TEST(ShouldRestoreAttributes) {
        ShouldRestoreSettings(R"(
            attributes {
              key: "key"
              value: "value"
            }
        )", {
            NLs::UserAttrsEqual({{"key", "value"}}),
        });
    }

    Y_UNIT_TEST(ShouldRestoreIncrementalBackupFlag) {
        ShouldRestoreSettings(R"(
            attributes {
              key: "__incremental_backup"
              value: "{}"
            }
        )", {
            NLs::IncrementalBackup(true),
        });
    }

    Y_UNIT_TEST(ShouldRestoreIncrementalBackupFlagNullAsFalse) {
        ShouldRestoreSettings(R"(
            attributes {
              key: "__incremental_backup"
              value: "null"
            }
        )", {
            NLs::IncrementalBackup(false),
        });
    }

    // Skip compaction_policy (not supported)
    // Skip uniform_partitions (has no effect)

    Y_UNIT_TEST(ShouldRestoreSplitPoints) {
        ShouldRestoreSettings(R"(
            partition_at_keys {
              split_points {
                type { tuple_type { elements { optional_type { item { type_id: UTF8 } } } } }
                value { items { text_value: "b" } }
              }
            }
        )", {
            NLs::CheckBoundaries,
        });
    }

    Y_UNIT_TEST(ShouldRestorePartitioningBySize) {
        ShouldRestoreSettings(R"(
            partitioning_settings {
              partitioning_by_size: ENABLED
              partition_size_mb: 1024
            }
        )", {
            NLs::SizeToSplitEqual(1 << 30),
        });
    }

    Y_UNIT_TEST(ShouldRestorePartitioningByLoad) {
        ShouldRestoreSettings(R"(
            partitioning_settings {
              partitioning_by_load: ENABLED
            }
        )", {
            NLs::PartitioningByLoadStatus(true),
        });
    }

    Y_UNIT_TEST(ShouldRestoreMinMaxPartitionsCount) {
        ShouldRestoreSettings(R"(
            partitioning_settings {
              min_partitions_count: 2
              max_partitions_count: 3
            }
        )", {
            NLs::MinPartitionsCountEqual(2),
            NLs::MaxPartitionsCountEqual(3),
        });
    }

    Y_UNIT_TEST(ShouldRestoreKeyBloomFilter) {
        ShouldRestoreSettings(R"(
            key_bloom_filter: ENABLED
        )", {
            NLs::KeyBloomFilterStatus(true),
        });
    }

    Y_UNIT_TEST(ShouldRestorePerAzReadReplicas) {
        NKikimrHive::TFollowerGroup group;
        group.SetFollowerCount(1);
        group.SetRequireAllDataCenters(true);
        group.SetFollowerCountPerDataCenter(true);

        ShouldRestoreSettings(R"(
            read_replicas_settings {
              per_az_read_replicas_count: 1
            }
        )", {
            NLs::FollowerGroups({group}),
        });
    }

    Y_UNIT_TEST(ShouldRestoreAnyAzReadReplicas) {
        NKikimrHive::TFollowerGroup group;
        group.SetFollowerCount(1);
        group.SetRequireAllDataCenters(false);

        ShouldRestoreSettings(R"(
            read_replicas_settings {
              any_az_read_replicas_count: 1
            }
        )", {
            NLs::FollowerGroups({group}),
        });
    }

    void ShouldRestoreIndexTableSettings(const TString& schemeAdditions, auto&& tableDescriptionChecker) {
        TTestBasicRuntime runtime;

        const auto empty = TTestData("", EmptyYsonStr);
        const auto data = TTestDataWithScheme(TStringBuilder() << R"(
                columns {
                    name: "key"
                    type { optional_type { item { type_id: UTF8 } } }
                }
                columns {
                    name: "value"
                    type { optional_type { item { type_id: UINT32 } } }
                }
                primary_key: "key"
            )" << schemeAdditions,
            {empty}
        );

        Run(runtime, ConvertTestData(data), R"(
            ImportFromS3Settings {
                endpoint: "localhost:%d"
                scheme: HTTP
                items {
                    source_prefix: ""
                    destination_path: "/MyRoot/User/Table"
                }
            }
        )", Ydb::StatusIds::SUCCESS, "/MyRoot/User");

        ui64 schemeshardId = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/User"), {
            NLs::PathExist,
            NLs::ExtractTenantSchemeshard(&schemeshardId)
        });

        tableDescriptionChecker(
            DescribePath(runtime, schemeshardId, "/MyRoot/User/Table/ByValue/indexImplTable", true, true, true)
        );
    }

    Y_UNIT_TEST(ShouldRestoreIndexTableSplitPoints) {
        ShouldRestoreIndexTableSettings(R"(
                indexes {
                    name: "ByValue"
                    index_columns: "value"
                    global_index {
                        settings {
                            partition_at_keys {
                                split_points {
                                    type { tuple_type { elements { optional_type { item { type_id: UINT32 } } } } }
                                    value { items { uint32_value: 1 } }
                                }
                            }
                        }
                    }
                }
            )",
            [](const NKikimrScheme::TEvDescribeSchemeResult& tableDescription) {
                TestDescribeResult(
                    tableDescription,
                    {NLs::CheckBoundaries}
                );
            }
        );
    }

    Y_UNIT_TEST(ShouldRestoreIndexTableUniformPartitionsCount) {
        ShouldRestoreIndexTableSettings(R"(
                indexes {
                    name: "ByValue"
                    index_columns: "value"
                    global_index {
                        settings {
                            uniform_partitions: 10
                        }
                    }
                }
            )",
            [](const NKikimrScheme::TEvDescribeSchemeResult& tableDescription) {
                const auto& pathDescription = tableDescription.GetPathDescription();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    pathDescription.TablePartitionsSize(), 10,
                    pathDescription.ShortDebugString()
                );
            }
        );
    }

    Y_UNIT_TEST(ShouldRestoreIndexTablePartitioningSettings) {
        ShouldRestoreIndexTableSettings(R"(
                indexes {
                    name: "ByValue"
                    index_columns: "value"
                    global_index {
                        settings {
                            partitioning_settings {
                                partitioning_by_size: ENABLED
                                partition_size_mb: 1024
                                partitioning_by_load: ENABLED
                                min_partitions_count: 2
                                max_partitions_count: 3
                            }
                        }
                    }
                }
            )",
            [](const NKikimrScheme::TEvDescribeSchemeResult& tableDescription) {
                TestDescribeResult(
                    tableDescription,
                    {
                        NLs::SizeToSplitEqual(1 << 30),
                        NLs::PartitioningByLoadStatus(true),
                        NLs::MinPartitionsCountEqual(2),
                        NLs::MaxPartitionsCountEqual(3)
                    }
                );
            }
        );
    }

    Y_UNIT_TEST(ShouldFailOnInvalidSchema) {
        TTestBasicRuntime runtime;

        Run(runtime, ConvertTestData(GenerateTestData("", {{"a", 1}})), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )", Ydb::StatusIds::CANCELLED);
    }

    void ShouldFailOnInvalidCsv(const TString& csv) {
        TTestBasicRuntime runtime;

        const auto data = TTestDataWithScheme(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {TTestData(csv, EmptyYsonStr)});

        Run(runtime, ConvertTestData(data), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(ShouldFailOnFileWithoutNewLines) {
        ShouldFailOnInvalidCsv("\"a1\",\"value1\"");
    }

    Y_UNIT_TEST(ShouldFailOnEmptyToken) {
        ShouldFailOnInvalidCsv("\"a1\",\n");
    }

    Y_UNIT_TEST(ShouldFailOnInvalidValue) {
        TTestBasicRuntime runtime;

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UINT64 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 1}});

        Run(runtime, ConvertTestData(data), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(ShouldFailOnOutboundKey) {
        TTestBasicRuntime runtime;

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
            partition_at_keys {
              split_points {
                type { tuple_type { elements { optional_type { item { type_id: UTF8 } } } } }
                value { items { text_value: "b" } }
              }
            }
        )", {{"a", 1}, {"a", 1}});

        Run(runtime, ConvertTestData(data), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(ShouldFailOnAbsentData) {
        TTestBasicRuntime runtime;

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
            partition_at_keys {
              split_points {
                type { tuple_type { elements { optional_type { item { type_id: UTF8 } } } } }
                value { items { text_value: "b" } }
              }
            }
        )", {{"a", 1}});

        Run(runtime, ConvertTestData(data), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(ShouldFailOnNonUniqDestinationPaths) {
        TTestBasicRuntime runtime;

        auto unusedTestData = THashMap<TString, TString>();
        Run(runtime, std::move(unusedTestData), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: "a"
                destination_path: "/MyRoot/Table"
              }
              items {
                source_prefix: "b"
                destination_path: "/MyRoot/Table"
              }
            }
        )", Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(ShouldFailOnInvalidPath) {
        TTestBasicRuntime runtime;

        auto unusedTestData = THashMap<TString, TString>();
        Run(runtime, std::move(unusedTestData), R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: "a"
                destination_path: "/InvalidRoot/Table"
              }
            }
        )", Ydb::StatusIds::BAD_REQUEST);
    }

    void CancelShouldSucceed(TDelayFunc delayFunc) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());
        ui64 txId = 100;

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
            indexes {
              name: "by_value"
              index_columns: "value"
              global_index {}
            }
        )", {{"a", 1}});

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock(ConvertTestData(data), TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);

        THolder<IEventHandle> delayed;
        auto prevObserver = SetDelayObserver(runtime, delayed, delayFunc);

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )", port));
        const ui64 importId = txId;

        WaitForDelayed(runtime, delayed, prevObserver);

        TestCancelImport(runtime, ++txId, "/MyRoot", importId);
        runtime.Send(delayed.Release(), 0, true);
        env.TestWaitNotification(runtime, importId);

        TestGetImport(runtime, importId, "/MyRoot", Ydb::StatusIds::CANCELLED);
    }

    Y_UNIT_TEST(CancelUponGettingSchemeShouldSucceed) {
        CancelShouldSucceed([](TAutoPtr<IEventHandle>& ev) {
            return ev->GetTypeRewrite() == TEvPrivate::EvImportSchemeReady;
        });
    }

    Y_UNIT_TEST(CancelUponCreatingTableShouldSucceed) {
        CancelShouldSucceed([](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvSchemeShard::EvModifySchemeTransaction) {
                return false;
            }

            return ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record
                .GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateIndexedTable;
        });
    }

    Y_UNIT_TEST(CancelUponTransferringShouldSucceed) {
        CancelShouldSucceed([](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvSchemeShard::EvModifySchemeTransaction) {
                return false;
            }

            return ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record
                .GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpRestore;
        });
    }

    Y_UNIT_TEST(CancelUponBuildingIndicesShouldSucceed) {
        CancelShouldSucceed([](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvSchemeShard::EvModifySchemeTransaction) {
                return false;
            }

            return ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record
                .GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpApplyIndexBuild;
        });
    }

    Y_UNIT_TEST(ShouldCheckQuotas) {
        const TString userSID = "user@builtin";
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().SystemBackupSIDs({userSID}));

        TSchemeLimits lowLimits;
        lowLimits.MaxImports = 0;
        SetSchemeshardSchemaLimits(runtime, lowLimits);

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 1}});

        const TString request = R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )";

        Run(runtime, env, ConvertTestData(data), request, Ydb::StatusIds::PRECONDITION_FAILED);
        Run(runtime, env, ConvertTestData(data), request, Ydb::StatusIds::SUCCESS, "/MyRoot", false, userSID);
    }

    Y_UNIT_TEST(UidAsIdempotencyKey) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions());
        ui64 txId = 100;

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 1}});

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock(ConvertTestData(data), TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        const auto request = Sprintf(R"(
            OperationParams {
              labels {
                key: "uid"
                value: "foo"
              }
            }
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )", port);

        // create operation
        TestImport(runtime, ++txId, "/MyRoot", request);
        const ui64 importId = txId;
        // create operation again with same uid
        TestImport(runtime, ++txId, "/MyRoot", request);
        // new operation was not created
        TestGetImport(runtime, txId, "/MyRoot", Ydb::StatusIds::NOT_FOUND);
        // check previous operation
        TestGetImport(runtime, importId, "/MyRoot");
        env.TestWaitNotification(runtime, importId);
    }

    Y_UNIT_TEST(ImportStartTime) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.UpdateCurrentTime(TInstant::Now());
        ui64 txId = 100;

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 1}});

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock(ConvertTestData(data), TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )", port));

        const auto desc = TestGetImport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_PREPARING);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(!entry.HasEndTime());
    }

    Y_UNIT_TEST(CompletedImportEndTime) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.UpdateCurrentTime(TInstant::Now());
        ui64 txId = 100;

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 1}});

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock(ConvertTestData(data), TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )", port));

        runtime.AdvanceCurrentTime(TDuration::Seconds(30)); // doing import

        env.TestWaitNotification(runtime, txId);

        const auto desc = TestGetImport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_DONE);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_LT(entry.GetStartTime().seconds(), entry.GetEndTime().seconds());
    }

    Y_UNIT_TEST(CancelledImportEndTime) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.UpdateCurrentTime(TInstant::Now());
        ui64 txId = 100;

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 1}});

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock(ConvertTestData(data), TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        auto delayFunc = [](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvSchemeShard::EvModifySchemeTransaction) {
                return false;
            }

            return ev->Get<TEvSchemeShard::TEvModifySchemeTransaction>()->Record
                .GetTransaction(0).GetOperationType() == NKikimrSchemeOp::ESchemeOpRestore;
        };

        THolder<IEventHandle> delayed;
        auto prevObserver = SetDelayObserver(runtime, delayed, delayFunc);

        TestImport(runtime, ++txId, "/MyRoot", Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )", port));
        const ui64 importId = txId;

        runtime.AdvanceCurrentTime(TDuration::Seconds(30)); // doing import

        WaitForDelayed(runtime, delayed, prevObserver);

        TestCancelImport(runtime, ++txId, "/MyRoot", importId);

        auto desc = TestGetImport(runtime, importId, "/MyRoot");
        auto entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_CANCELLATION);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(!entry.HasEndTime());

        runtime.Send(delayed.Release(), 0, true);
        env.TestWaitNotification(runtime, importId);

        desc = TestGetImport(runtime, importId, "/MyRoot", Ydb::StatusIds::CANCELLED);
        entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_CANCELLED);
        UNIT_ASSERT(entry.HasStartTime());
        UNIT_ASSERT(entry.HasEndTime());
        UNIT_ASSERT_LT(entry.GetStartTime().seconds(), entry.GetEndTime().seconds());
    }

    Y_UNIT_TEST(UserSID) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        const auto data = GenerateTestData(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )", {{"a", 1}});

        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        TS3Mock s3Mock(ConvertTestData(data), TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        const TString request = Sprintf(R"(
            ImportFromS3Settings {
              endpoint: "localhost:%d"
              scheme: HTTP
              items {
                source_prefix: ""
                destination_path: "/MyRoot/Table"
              }
            }
        )", port);
        const TString userSID = "user@builtin";
        TestImport(runtime, ++txId, "/MyRoot", request, userSID);

        const auto desc = TestGetImport(runtime, txId, "/MyRoot");
        const auto& entry = desc.GetResponse().GetEntry();
        UNIT_ASSERT_VALUES_EQUAL(entry.GetProgress(), Ydb::Import::ImportProgress::PROGRESS_PREPARING);
        UNIT_ASSERT_VALUES_EQUAL(entry.GetUserSID(), userSID);
    }
}

Y_UNIT_TEST_SUITE(TImportWithRebootsTests) {
    void ShouldSucceed(const TString& scheme) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        const auto data = GenerateTestData(scheme, {{"a", 1}});

        TS3Mock s3Mock(ConvertTestData(data), TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
                runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);
            }

            AsyncImport(runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                ImportFromS3Settings {
                  endpoint: "localhost:%d"
                  scheme: HTTP
                  items {
                    source_prefix: ""
                    destination_path: "/MyRoot/Table"
                  }
                }
            )", port));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);

            {
                TInactiveZone inactive(activeZone);
                TestGetImport(runtime, t.TxId, "/MyRoot", {
                    Ydb::StatusIds::SUCCESS,
                    Ydb::StatusIds::NOT_FOUND
                });
            }
        });
    }

    Y_UNIT_TEST(ShouldSucceedOnSimpleTable) {
        ShouldSucceed(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )");
    }

    Y_UNIT_TEST(ShouldSucceedOnIndexedTable) {
        ShouldSucceed(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
            indexes {
              name: "by_value"
              index_columns: "value"
              global_index {}
            }
        )");
    }

    void CancelShouldSucceed(const TString& scheme) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();

        const auto data = GenerateTestData(scheme, {{"a", 1}});

        TS3Mock s3Mock(ConvertTestData(data), TS3Mock::TSettings(port));
        UNIT_ASSERT(s3Mock.Start());

        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {
            {
                TInactiveZone inactive(activeZone);

                runtime.SetLogPriority(NKikimrServices::DATASHARD_RESTORE, NActors::NLog::PRI_TRACE);
                runtime.SetLogPriority(NKikimrServices::IMPORT, NActors::NLog::PRI_TRACE);
            }

            AsyncImport(runtime, ++t.TxId, "/MyRoot", Sprintf(R"(
                ImportFromS3Settings {
                  endpoint: "localhost:%d"
                  scheme: HTTP
                  items {
                    source_prefix: ""
                    destination_path: "/MyRoot/Table"
                  }
                }
            )", port));
            t.TestEnv->TestWaitNotification(runtime, t.TxId);
            const ui64 importId = t.TxId;

            t.TestEnv->ReliablePropose(runtime, CancelImportRequest(++t.TxId, "/MyRoot", importId), {
                Ydb::StatusIds::SUCCESS,
                Ydb::StatusIds::NOT_FOUND
            });
            t.TestEnv->TestWaitNotification(runtime, importId);

            {
                TInactiveZone inactive(activeZone);
                TestGetImport(runtime, importId, "/MyRoot", {
                    Ydb::StatusIds::SUCCESS,
                    Ydb::StatusIds::CANCELLED,
                    Ydb::StatusIds::NOT_FOUND
                });
            }
        });
    }

    Y_UNIT_TEST(CancelShouldSucceedOnSimpleTable) {
        CancelShouldSucceed(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
        )");
    }

    Y_UNIT_TEST(CancelShouldSucceedOnIndexedTable) {
        CancelShouldSucceed(R"(
            columns {
              name: "key"
              type { optional_type { item { type_id: UTF8 } } }
            }
            columns {
              name: "value"
              type { optional_type { item { type_id: UTF8 } } }
            }
            primary_key: "key"
            indexes {
              name: "by_value"
              index_columns: "value"
              global_index {}
            }
        )");
    }
}
