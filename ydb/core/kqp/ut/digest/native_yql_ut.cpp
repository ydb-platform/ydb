#include "digest_test_common.h"
#include <ydb/core/scheme/table_digest/table_digest.h>

#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <util/string/cast.h>
#include <util/string/hex.h>

#include <string>

namespace NKikimr::NKqp {
namespace {

using namespace NDigestTest;
namespace NDigest = NYql::NDigest;
namespace NNative = NKikimr::NTableDigest;
using namespace NYdb;
using namespace NYdb::NTable;
using NScheme::TDecimalType;
using NScheme::TTypeInfo;
namespace NTypeIds = NScheme::NTypeIds;

template <class T> TString NativeBytes(T value) {
  return TString(reinterpret_cast<const char *>(&value), sizeof(value));
}

struct TNativeYqlCase {
  TTypeInfo Type;
  TString Constructor;
  TString TypeName;
  TString Bytes;
};

TVector<TNativeYqlCase> NativeYqlCases() {
  const NYdb::TUuidValue uuid("00112233-4455-6677-8899-aabbccddeeff");
  const auto json = std::get<NBinaryJson::TBinaryJson>(
      NBinaryJson::SerializeToBinaryJson(R"({"b":2})"));
  const auto dyNumber = NDyNumber::ParseDyNumberString("-10.23");
  UNIT_ASSERT(dyNumber);
  // Decimal(5, 2) stores 11.3 as the integer coefficient 1130.
  const std::array<ui64, 2> decimal = {1130, 0};
  return {
      {TTypeInfo(NTypeIds::Bool), "Bool('true')", "Bool", NativeBytes<ui8>(1)},
      {TTypeInfo(NTypeIds::Int8), "Int8('-8')", "Int8", NativeBytes<i8>(-8)},
      {TTypeInfo(NTypeIds::Uint8), "Uint8('200')", "Uint8",
       NativeBytes<ui8>(200)},
      {TTypeInfo(NTypeIds::Int16), "Int16('-16000')", "Int16",
       NativeBytes<i16>(-16000)},
      {TTypeInfo(NTypeIds::Uint16), "Uint16('60000')", "Uint16",
       NativeBytes<ui16>(60000)},
      {TTypeInfo(NTypeIds::Int32), "Int32('-100000')", "Int32",
       NativeBytes<i32>(-100000)},
      {TTypeInfo(NTypeIds::Uint32), "Uint32('100000')", "Uint32",
       NativeBytes<ui32>(100000)},
      {TTypeInfo(NTypeIds::Int64), "Int64('-5000000000')", "Int64",
       NativeBytes<i64>(-5000000000)},
      {TTypeInfo(NTypeIds::Uint64), "Uint64('5000000000')", "Uint64",
       NativeBytes<ui64>(5000000000)},
      {TTypeInfo(NTypeIds::Float), "Float('1.5')", "Float",
       NativeBytes<float>(1.5f)},
      {TTypeInfo(NTypeIds::Double), "Double('2.25')", "Double",
       NativeBytes<double>(2.25)},
      {TTypeInfo(NTypeIds::String), "String('hello')", "String", "hello"},
      {TTypeInfo(NTypeIds::Utf8), "Utf8('мир')", "Utf8", "мир"},
      {TTypeInfo(NTypeIds::Yson), "Yson('[1;2]')", "Yson", "[1;2]"},
      {TTypeInfo(NTypeIds::Json), "Json('{\"a\":1}')", "Json", R"({"a":1})"},
      {TTypeInfo(NTypeIds::JsonDocument), "JsonDocument('{\"b\":2}')",
       "JsonDocument", TString(json.Data(), json.Size())},
      {TTypeInfo(NTypeIds::Uuid),
       "Uuid('00112233-4455-6677-8899-aabbccddeeff')", "Uuid",
       TString(uuid.Buf_.Bytes, sizeof(uuid.Buf_.Bytes))},
      {TTypeInfo(NTypeIds::DyNumber), "DyNumber('-10.23')", "DyNumber",
       *dyNumber},
      {TTypeInfo(NTypeIds::Date), "Date('1970-01-02')", "Date",
       NativeBytes<ui16>(1)},
      {TTypeInfo(NTypeIds::Datetime), "Datetime('1970-01-02T00:00:00Z')",
       "Datetime", NativeBytes<ui32>(86400)},
      {TTypeInfo(NTypeIds::Timestamp),
       "Timestamp('1970-01-02T00:00:00.000000Z')", "Timestamp",
       NativeBytes<ui64>(86400000000)},
      {TTypeInfo(NTypeIds::Interval), "Interval('P1D')", "Interval",
       NativeBytes<i64>(86400000000)},
      {TTypeInfo(NTypeIds::Date32), "Date32('1969-12-31')", "Date32",
       NativeBytes<i32>(-1)},
      {TTypeInfo(NTypeIds::Datetime64), "Datetime64('1969-12-31T00:00:00Z')",
       "Datetime64", NativeBytes<i64>(-86400)},
      {TTypeInfo(NTypeIds::Timestamp64),
       "Timestamp64('1969-12-31T00:00:00.000000Z')", "Timestamp64",
       NativeBytes<i64>(-86400000000)},
      {TTypeInfo(NTypeIds::Interval64), "Interval64('P1D')", "Interval64",
       NativeBytes<i64>(86400000000)},
      {TTypeInfo(TDecimalType(5, 2)), "Decimal('11.3',5,2)", "Decimal(5,2)",
       TString(reinterpret_cast<const char *>(decimal.data()),
               sizeof(decimal))},
  };
}

} // namespace

Y_UNIT_TEST_SUITE(DigestNativeParity) {
  Y_UNIT_TEST(AllScalarTypesAndNulls) {
    TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
    auto session =
        kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto cases = NativeYqlCases();
    UNIT_ASSERT_VALUES_EQUAL(cases.size(), 27);
    TVector<NNative::TColumn> columns;
    TVector<TCell> cells;
    TStringBuilder present;
    TStringBuilder nulls;
    for (size_t i = 0; i < cases.size(); ++i) {
      const auto &item = cases[i];
      const TString name = "c" + ToString(i);
      columns.push_back({name, item.Type, true});
      cells.emplace_back(item.Bytes.data(), item.Bytes.size());
      if (i) {
        present << ", ";
        nulls << ", ";
      }
      present << "Just(" << item.Constructor << ") AS " << name;
      nulls << "CAST(NULL AS " << item.TypeName << "?) AS " << name;
    }
    for (auto algorithm :
         {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
          NDigest::EAlgorithm::Sha256}) {
      NNative::TAccumulator native(columns, algorithm);
      native.AddRow(cells);
      native.AddRow(TVector<TCell>(columns.size()));
      const TString query = TStringBuilder()
                            << DigestFactorySql()
                            << "$data = (SELECT " << present
                            << " UNION ALL SELECT " << nulls << ");\n"
                            << "SELECT AGGREGATE_BY(TableRow(), $DigestFactory(\""
                            << NDigest::GetAlgorithmName(algorithm)
                            << "\")) FROM $data;";
      const auto result =
          session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx())
              .GetValueSync();
      UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
      AssertDigest(ReadDigest(result.GetResultSet(0)), native.GetState());
    }
  }

  Y_UNIT_TEST(JsonDocumentBinaryLayoutsAndTextRestore) {
    // Same array, different physical order of the string and number indexes.
    const std::array<TString, 2> documents = {
        HexDecode("0103000080000000e405000083030000e406000003040000"
                  "0200000040050000e0050000616c706861006265746100"
                  "000000000000f03f0000000000000040"),
        HexDecode("0103000080000000e406000003040000e405000083030000"
                  "0200000020050000e00500006265746100616c70686100"
                  "0000000000000040000000000000f03f"),
    };
    // Restoring exported negative zero loses its sign bit, so its digest must stay stable.
    const TString negativeZero =
        HexDecode("810100002200000004020000000000000000000000000080");
    UNIT_ASSERT(NBinaryJson::IsValidBinaryJson(negativeZero));
    UNIT_ASSERT(documents[0] != documents[1]);
    const TString text = NBinaryJson::SerializeToJson(documents[0]);
    for (const auto &document : documents) {
      UNIT_ASSERT(NBinaryJson::IsValidBinaryJson(document));
      UNIT_ASSERT_VALUES_EQUAL(NBinaryJson::SerializeToJson(document), text);
    }

    TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
    auto session =
        kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto schema = session
                            .ExecuteSchemeQuery(R"(
      CREATE TABLE json_original (key Uint64 NOT NULL, doc JsonDocument, PRIMARY KEY (key));
      CREATE TABLE json_reordered (key Uint64 NOT NULL, doc JsonDocument, PRIMARY KEY (key));
      CREATE TABLE json_restored (key Uint64 NOT NULL, doc JsonDocument, PRIMARY KEY (key));
    )")
                            .GetValueSync();
    UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
    const TStringBuf tables[] = {"json_original", "json_reordered"};
    for (size_t i = 0; i < documents.size(); ++i) {
      const auto params =
          TParamsBuilder()
              .AddParam("$bytes")
              .String(std::string(documents[i].data(), documents[i].size()))
              .Build()
              .AddParam("$zero")
              .String(std::string(negativeZero.data(), negativeZero.size()))
              .Build()
              .Build();
      // String and JsonDocument use the same existing pickle representation.
      // Unpickle preserves the fixture bytes instead of reparsing JSON text.
      const auto inserted =
          session
              .ExecuteDataQuery(
                  TStringBuilder()
                      << "DECLARE $bytes AS String; DECLARE $zero AS String; UPSERT INTO " << tables[i]
                      << " (key, doc) VALUES (1, Unpickle(JsonDocument, "
                         "StablePickle($bytes))), (2, NULL), "
                         "(3, Unpickle(JsonDocument, StablePickle($zero)));",
                  TTxControl::BeginTx().CommitTx(), params)
              .GetValueSync();
      UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
    }

    // Public JsonDocument values are exported as text, then parsed again on
    // import.
    const auto exported =
        session
            .ExecuteDataQuery("SELECT doc FROM json_original WHERE key != 2 ORDER BY key;",
                              TTxControl::BeginTx().CommitTx())
            .GetValueSync();
    UNIT_ASSERT_C(exported.IsSuccess(), exported.GetIssues().ToString());
    TResultSetParser rows(exported.GetResultSet(0));
    UNIT_ASSERT(rows.TryNextRow());
    const auto exportedText =
        rows.ColumnParser("doc").GetOptionalJsonDocument();
    UNIT_ASSERT(exportedText);
    UNIT_ASSERT_VALUES_EQUAL(TString(*exportedText), text);
    UNIT_ASSERT(rows.TryNextRow());
    const auto exportedZero = rows.ColumnParser("doc").GetOptionalJsonDocument();
    UNIT_ASSERT(exportedZero);
    UNIT_ASSERT(!rows.TryNextRow());
    const auto restoredParams = TParamsBuilder()
                                    .AddParam("$doc")
                                    .JsonDocument(*exportedText)
                                    .Build()
                                    .AddParam("$zero")
                                    .JsonDocument(*exportedZero)
                                    .Build()
                                    .Build();
    const auto restored = session
                              .ExecuteDataQuery(R"(
      DECLARE $doc AS JsonDocument;
      DECLARE $zero AS JsonDocument;
      UPSERT INTO json_restored (key, doc) VALUES (1, $doc), (2, NULL), (3, $zero);
    )", TTxControl::BeginTx().CommitTx(), restoredParams)
                              .GetValueSync();
    UNIT_ASSERT_C(restored.IsSuccess(), restored.GetIssues().ToString());

    for (auto algorithm :
         {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
          NDigest::EAlgorithm::Sha256}) {
      const TVector<NNative::TColumn> columns = {
          {"key", TTypeInfo(NTypeIds::Uint64), false},
          {"doc", TTypeInfo(NTypeIds::JsonDocument), true},
      };
      NNative::TAccumulator native(columns, algorithm);
      native.AddRow({TCell::Make(ui64(1)),
                     TCell(documents[0].data(), documents[0].size())});
      native.AddRow({TCell::Make(ui64(2)), TCell()});
      native.AddRow({TCell::Make(ui64(3)),
                     TCell(negativeZero.data(), negativeZero.size())});
      for (const auto &document : documents) {
        const auto params =
            TParamsBuilder()
                .AddParam("$bytes")
                .String(std::string(document.data(), document.size()))
                .Build()
                .AddParam("$zero")
                .String(std::string(negativeZero.data(), negativeZero.size()))
                .Build()
                .Build();
        const auto result =
            session
                .ExecuteDataQuery(
                    TStringBuilder()
                        << "DECLARE $bytes AS String;\n"
                        << "DECLARE $zero AS String;\n"
                        << DigestFactorySql()
                        << "$data = (SELECT 1ul AS key, "
                           "Just(Unpickle(JsonDocument, StablePickle($bytes))) "
                           "AS doc "
                        << "UNION ALL SELECT 2ul AS key, CAST(NULL AS "
                           "JsonDocument?) AS doc "
                        << "UNION ALL SELECT 3ul AS key, "
                           "Just(Unpickle(JsonDocument, StablePickle($zero))) AS doc);\n"
                        << "SELECT AGGREGATE_BY(TableRow(), $DigestFactory(\""
                        << NDigest::GetAlgorithmName(algorithm)
                        << "\")) FROM $data;", TTxControl::BeginTx().CommitTx(),
                    params)
                .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        AssertDigest(ReadDigest(result.GetResultSet(0)), native.GetState());
      }
      for (TStringBuf table :
           {"json_original", "json_reordered", "json_restored"}) {
        const auto result =
            session
                .ExecuteDataQuery(
                    TStringBuilder()
                        << "CHECKSUM TABLE " << table << " WITH (ALGORITHM = \""
                        << NDigest::GetAlgorithmName(algorithm)
                        << "\");", TTxControl::BeginTx().CommitTx())
                .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        AssertDigest(ReadChecksum(result.GetResultSet(0)), native.GetState());
      }
    }
  }

  Y_UNIT_TEST(JsonDocumentPreservesDoublePrecision) {
    // These adjacent double values collapse under 16-significant-digit output.
    const std::array<TString, 2> documents = {
        HexDecode("81010000220000000402000000000000000000000000f03f"),
        HexDecode("81010000220000000402000000000000010000000000f03f"),
    };
    const TStringBuf texts[] = {"1", "1.0000000000000002"};
    TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
    auto session =
        kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto schema = session
                            .ExecuteSchemeQuery(R"(
      CREATE TABLE json_precision (key Uint64 NOT NULL, doc JsonDocument, PRIMARY KEY (key));
    )")
                            .GetValueSync();
    UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
    TVector<TString> previousHashes;
    for (size_t i = 0; i < documents.size(); ++i) {
      UNIT_ASSERT(NBinaryJson::IsValidBinaryJson(documents[i]));
      const auto params =
          TParamsBuilder()
              .AddParam("$doc")
              .JsonDocument(std::string(texts[i].data(), texts[i].size()))
              .Build()
              .Build();
      const auto inserted = session
                                .ExecuteDataQuery(R"(
        DECLARE $doc AS JsonDocument;
        UPSERT INTO json_precision (key, doc) VALUES (1, $doc);
      )", TTxControl::BeginTx().CommitTx(), params)
                                .GetValueSync();
      UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
      size_t algorithmIndex = 0;
      for (auto algorithm :
           {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
            NDigest::EAlgorithm::Sha256}) {
        NNative::TAccumulator native(
            {{"key", TTypeInfo(NTypeIds::Uint64), false},
             {"doc", TTypeInfo(NTypeIds::JsonDocument), true}}, algorithm);
        native.AddRow({TCell::Make(ui64(1)),
                       TCell(documents[i].data(), documents[i].size())});
        const auto direct =
            session
                .ExecuteDataQuery(
                    TStringBuilder()
                        << "DECLARE $doc AS JsonDocument;\n"
                        << DigestFactorySql()
                        << "$data = AsList(AsStruct(1ul AS key, Just($doc) AS "
                           "doc));\n"
                        << "SELECT AGGREGATE_BY(TableRow(), $DigestFactory(\""
                        << NDigest::GetAlgorithmName(algorithm)
                        << "\")) FROM AS_TABLE($data);",
                    TTxControl::BeginTx().CommitTx(), params)
                .GetValueSync();
        UNIT_ASSERT_C(direct.IsSuccess(), direct.GetIssues().ToString());
        AssertDigest(ReadDigest(direct.GetResultSet(0)), native.GetState());
        const auto command =
            session
                .ExecuteDataQuery(
                    TStringBuilder()
                        << "CHECKSUM TABLE json_precision WITH (ALGORITHM = \""
                        << NDigest::GetAlgorithmName(algorithm)
                        << "\");", TTxControl::BeginTx().CommitTx())
                .GetValueSync();
        UNIT_ASSERT_C(command.IsSuccess(), command.GetIssues().ToString());
        const auto digest = ReadChecksum(command.GetResultSet(0));
        AssertDigest(digest, native.GetState());
        if (!i) {
          previousHashes.push_back(digest.Hash);
        } else {
          UNIT_ASSERT(digest.Hash != previousHashes[algorithmIndex]);
        }
        ++algorithmIndex;
      }
    }
  }

  Y_UNIT_TEST(JsonDocumentInfinityRemainsDistinctFromStrings) {
    TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
    auto session =
        kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    const auto schema = session.ExecuteSchemeQuery(R"(
      CREATE TABLE json_infinity (key Uint64 NOT NULL, doc JsonDocument, PRIMARY KEY (key));
    )").GetValueSync();
    UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());

    // OLAP's CAST_TO_INFINITY overflow policy can store numeric infinities.
    // Unpickle retains their binary representation without text validation.
    const std::array<TStringBuf, 4> texts = {
        "1e100000000", R"("inf")", "-1e100000000", R"("-inf")"};
    std::array<TString, 3> numericHashes;
    for (size_t i = 0; i < texts.size(); ++i) {
      const auto document = std::get<NBinaryJson::TBinaryJson>(
          NBinaryJson::SerializeToBinaryJson(texts[i], /*allowInf=*/true));
      const auto params = TParamsBuilder()
                              .AddParam("$bytes")
                              .String(std::string(document.Data(), document.Size()))
                              .Build()
                              .Build();
      const auto inserted = session.ExecuteDataQuery(R"(
        DECLARE $bytes AS String;
        UPSERT INTO json_infinity (key, doc)
        VALUES (1, Unpickle(JsonDocument, StablePickle($bytes)));
      )", TTxControl::BeginTx().CommitTx(), params).GetValueSync();
      UNIT_ASSERT_C(inserted.IsSuccess(), inserted.GetIssues().ToString());
      size_t algorithmIndex = 0;
      for (auto algorithm :
           {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
            NDigest::EAlgorithm::Sha256}) {
        NNative::TAccumulator native(
            {{"key", TTypeInfo(NTypeIds::Uint64), false},
             {"doc", TTypeInfo(NTypeIds::JsonDocument), true}}, algorithm);
        native.AddRow({TCell::Make(ui64(1)),
                       TCell(document.Data(), document.Size())});
        const auto direct = session.ExecuteDataQuery(
            TStringBuilder()
                << "DECLARE $bytes AS String;\n" << DigestFactorySql()
                << "$data = AsList(AsStruct(1ul AS key, "
                   "Just(Unpickle(JsonDocument, StablePickle($bytes))) AS doc));\n"
                << "SELECT AGGREGATE_BY(TableRow(), $DigestFactory(\""
                << NDigest::GetAlgorithmName(algorithm)
                << "\")) FROM AS_TABLE($data);",
            TTxControl::BeginTx().CommitTx(), params).GetValueSync();
        UNIT_ASSERT_C(direct.IsSuccess(), direct.GetIssues().ToString());
        AssertDigest(ReadDigest(direct.GetResultSet(0)), native.GetState());
        const auto command = session.ExecuteDataQuery(
            TStringBuilder()
                << "CHECKSUM TABLE json_infinity WITH (ALGORITHM = \""
                << NDigest::GetAlgorithmName(algorithm) << "\");",
            TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(command.IsSuccess(), command.GetIssues().ToString());
        const auto digest = ReadChecksum(command.GetResultSet(0));
        AssertDigest(digest, native.GetState());
        if (i % 2 == 0) {
          numericHashes[algorithmIndex] = digest.Hash;
        } else {
          UNIT_ASSERT(digest.Hash != numericHashes[algorithmIndex]);
        }
        ++algorithmIndex;
      }
    }
  }
}

} // namespace NKikimr::NKqp
