#include <ydb/core/backup/yt/format.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/stream/str.h>
#include <util/stream/zlib.h>

using namespace NKikimr::NBackup::NYt;

namespace {
    TSchema Schema(TStringBuf columns) {
        TSchema schema;
        TString error;
        const TString metadata = TString("{schema=<strict=%true;unique_keys=%true>[") + columns + "];user_attributes={}}";
        UNIT_ASSERT_C(ParseMetadata(metadata, schema, error), error);
        return schema;
    }
    TString Binary(TStringBuf text) {
        auto rows = NYT::NodeFromYsonString(text, NYson::EYsonType::ListFragment);
        TString binary = NYT::NodeToYsonString(rows, NYson::EYsonFormat::Binary);
        return binary.substr(1, binary.size() - 2);
    }
    TString Gzip(TStringBuf data) {
        TString result;
        TStringOutput output(result);
        TZLibCompress compressor(TZLibCompress::TParams(&output).SetType(ZLib::GZip));
        compressor.Write(data);
        compressor.Finish();
        return result;
    }
} // namespace

Y_UNIT_TEST_SUITE(YtBackupFormat) {
    Y_UNIT_TEST(TypeV3HasPrecedence) {
        auto schema = Schema("{name=k;type=int64;required=%false;type_v3=int8;sort_order=ascending};{name=v;type=any;required=%true;type_v3={type_name=optional;item=utf8}};{name=required_value;type_v3=uint64}");
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(schema.Columns[0].Type), static_cast<int>(Ydb::Type::INT8));
        UNIT_ASSERT(!schema.Columns[0].Nullable);
        UNIT_ASSERT(schema.Columns[1].Nullable);
        UNIT_ASSERT_VALUES_EQUAL(schema.Table.primary_key(0), "k");
        UNIT_ASSERT(schema.Table.columns(1).type().has_optional_type());
        UNIT_ASSERT(schema.Table.columns(0).has_not_null());
        UNIT_ASSERT(schema.Table.columns(0).not_null());
        UNIT_ASSERT(schema.Table.columns(1).has_not_null());
        UNIT_ASSERT(!schema.Table.columns(1).not_null());
        UNIT_ASSERT(schema.Table.columns(2).has_not_null());
        UNIT_ASSERT(schema.Table.columns(2).not_null());
    }
    Y_UNIT_TEST(RejectUnsupportedSchemas) {
        for (const auto& columns : {
                 "{name=k;type=int64;required=%true;sort_order=descending}",
                 "{name=k;type=int64;required=%true}",
                 "{name=k;type=int64;required=%true;sort_order=ascending};{name=k;type=string}",
                 "{name=v;type=string};{name=k;type=int64;required=%true;sort_order=ascending}",
                 "{name=k;type=int64;type_v3={type_name=optional;item=int64};sort_order=ascending}",
                 "{name=k;type=int64;required=%true;sort_order=ascending};{name=v;type=string;type_v3={type_name=list;item=int64}}",
                 "{name=k;type=int64;required=%true;sort_order=ascending;expression=foo}"}) {
            TSchema schema;
            TString error;
            UNIT_ASSERT(!ParseMetadata(TString("{schema=<strict=%true;unique_keys=%true>[") + columns + "]}", schema, error));
            UNIT_ASSERT(!error.empty());
        }
        for (const auto& attributes : {"<strict=%false;unique_keys=%true>", "<strict=%true;unique_keys=%false>", ""}) {
            TSchema schema;
            TString error;
            UNIT_ASSERT(!ParseMetadata(TString("{schema=") + attributes + "[{name=k;type=int64;required=%true;sort_order=ascending}]}", schema, error));
        }
    }
    Y_UNIT_TEST(BinaryRowsSplitAtEveryByte) {
        auto schema = Schema("{name=k;type_v3=int64;sort_order=ascending};{name=v;type_v3={type_name=optional;item=utf8}}");
        const TString input = Binary("{k=-9223372036854775808;v=\"hello 🌍\"};{k=9223372036854775807;v=#};{k=0}");
        ui32 rows = 0;
        TRowDecoder decoder(schema, [&](const TDecodedRow& row, TString&) {
            UNIT_ASSERT_VALUES_EQUAL(row.Cells.size(), 2);
            if (rows == 0) {
                UNIT_ASSERT_VALUES_EQUAL(row.Cells[0].AsValue<i64>(), Min<i64>());
                UNIT_ASSERT_VALUES_EQUAL(row.Cells[1].AsBuf(), "hello 🌍");
            } else {
                UNIT_ASSERT(row.Cells[1].IsNull());
            }
            ++rows;
            return true;
        });
        TString error;
        for (char byte : input) {
            UNIT_ASSERT_C(decoder.Feed(TStringBuf(&byte, 1), error), error);
        }
        UNIT_ASSERT_C(decoder.Finish(error), error);
        UNIT_ASSERT_VALUES_EQUAL(rows, 3);
    }
    Y_UNIT_TEST(RejectBadRows) {
        auto schema = Schema("{name=k;type_v3=int8;sort_order=ascending};{name=v;type_v3={type_name=optional;item=utf8}}");
        for (const auto& input : {"{k=128};", "{k=-129};", "{k=1u};", "{k=#};", "{};", "{k=1;k=2};", "{k=1;unknown=1};", "{k=1;v=[]};", "{k=1;v=\"\\xff\"};", "{k=1"}) {
            TRowDecoder decoder(schema, [](const TDecodedRow&, TString&) { return true; });
            TString error;
            UNIT_ASSERT(!decoder.Feed(input, error) || !decoder.Finish(error));
            UNIT_ASSERT(!error.empty());
        }
    }
    Y_UNIT_TEST(CallbackErrorsAndRowLimit) {
        auto schema = Schema("{name=k;type_v3=string;sort_order=ascending}");
        TString error;
        TRowDecoder rejected(schema, [](const TDecodedRow&, TString& error) {
            error = "duplicate key";
            return false;
        });
        UNIT_ASSERT(!rejected.Feed("{k=abc};", error));
        UNIT_ASSERT_STRING_CONTAINS(error, "duplicate key");
        TRowDecoder limited(schema, [](const TDecodedRow&, TString&) { return true; }, 32);
        UNIT_ASSERT(!limited.Feed(TString("{k=\"") + TString(100, 'x') + "\"};", error));
    }
    Y_UNIT_TEST(InternalSchemaWithTypeIds) {
        auto original = Schema("{name=k;type_v3=int64;sort_order=ascending};{name=v;type_v3={type_name=optional;item=string}}");
        auto description = original.Description;
        for (auto& column : *description.MutableColumns()) {
            column.ClearType();
        }
        TSchema restored;
        TString error;
        UNIT_ASSERT_C(BuildSchema(description, restored, error), error);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(restored.Columns[0].Type), static_cast<int>(Ydb::Type::INT64));
        UNIT_ASSERT(restored.Columns[1].Nullable);
    }
    Y_UNIT_TEST(PartSuffixGrammar) {
        TPartRange range;
        TString error;
        UNIT_ASSERT(ParsePartKey("path_prefix", "path_prefix_9_100", range, error));
        UNIT_ASSERT_VALUES_EQUAL(range.Start, 9);
        UNIT_ASSERT_VALUES_EQUAL(range.End, 100);
        UNIT_ASSERT(ParsePartKey("path_prefix", "path_prefix_0_0", range, error));
        UNIT_ASSERT(ParsePartKey("path_prefix", "path_prefix_42_42", range, error));
        for (const auto& key : {"path_prefix_2_1", "path_prefix_-1_2", "path_prefix_1_2.gz", "path_prefix_1_2_3", "path_prefix_1_", "path_prefix__2", "path_prefix_1_18446744073709551616", "path_prefixx_1_2"}) {
            UNIT_ASSERT(!ParsePartKey("path_prefix", key, range, error));
        }
    }
    Y_UNIT_TEST(EmptyFragmentAndTruncatedBinary) {
        auto schema = Schema("{name=k;type_v3=uint64;sort_order=ascending}");
        ui32 rows = 0;
        TString error;
        TRowDecoder empty(schema, [&](const TDecodedRow&, TString&) { ++rows; return true; });
        UNIT_ASSERT(empty.Finish(error));
        UNIT_ASSERT_VALUES_EQUAL(rows, 0);
        TString input = Binary("{k=18446744073709551615u}");
        TRowDecoder truncated(schema, [](const TDecodedRow&, TString&) { return true; });
        UNIT_ASSERT(truncated.Feed(TStringBuf(input).SubStr(0, input.size() - 2), error));
        UNIT_ASSERT(!truncated.Finish(error));
    }
    Y_UNIT_TEST(FirstKeyProbeChunkedGzip) {
        const auto schema = Schema("{name=k;type_v3=uint64;sort_order=ascending};{name=v;type_v3=string}");
        const auto compressed = Gzip(Binary("{k=42u;v=hello};{k=100u;v=world}"));
        TString error;
        TFirstKeyProbe probe(schema);
        for (size_t index = 0; index < compressed.size() && !probe.HasKey(); ++index) {
            UNIT_ASSERT_C(probe.Feed(TStringBuf(compressed).SubStr(index, 1), index + 1 == compressed.size(), error), error);
        }
        UNIT_ASSERT(probe.HasKey());
        TSerializedCellVec key(probe.GetKey());
        UNIT_ASSERT_VALUES_EQUAL(key.GetCells().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(key.GetCells()[0].AsValue<ui64>(), 42);
    }
    Y_UNIT_TEST(FirstKeyProbeRejectsTruncatedAndEmptyGzip) {
        const auto schema = Schema("{name=k;type_v3=uint64;sort_order=ascending}");
        TString error;
        TFirstKeyProbe truncated(schema);
        const auto compressed = Gzip(Binary("{k=42u}"));
        UNIT_ASSERT(!truncated.Feed(TStringBuf(compressed).SubStr(0, 10), true, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "truncated");
        TFirstKeyProbe empty(schema);
        UNIT_ASSERT(!empty.Feed(Gzip(""), true, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "no rows");
    }
    Y_UNIT_TEST(FirstKeyProbeBoundsCompressedAndDecodedInput) {
        const auto schema = Schema("{name=k;type_v3=uint64;sort_order=ascending};{name=v;type_v3=string}");
        TString error;
        TFirstKeyProbe decoded(schema, 256);
        const auto large = Gzip(Binary(TString("{k=42u;v=\"") + TString(1024, 'x') + "\"}"));
        UNIT_ASSERT(!decoded.Feed(large, true, error));
        UNIT_ASSERT(!error.empty());
        TFirstKeyProbe compressed(schema, 8);
        UNIT_ASSERT(!compressed.Feed(large, false, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "bounded source read");
    }
} // Y_UNIT_TEST_SUITE(YtBackupFormat)
