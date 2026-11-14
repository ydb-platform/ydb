#include <ydb/core/scheme/table_digest/table_digest.h>
#include <ydb/library/table_digest/json_document.h>

#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/digest/city.h>
#include <util/string/cast.h>
#include <util/string/hex.h>

#include <algorithm>
#include <array>
#include <limits>
#include <tuple>

namespace NKikimr::NTableDigest {
namespace {

namespace NDigest = NYql::NDigest;
using NScheme::TDecimalType;
using NScheme::TTypeInfo;
namespace NTypeIds = NScheme::NTypeIds;

template <class T> TString CellBytes(T value) {
  return TString(reinterpret_cast<const char *>(&value), sizeof(value));
}

struct TCase {
  TTypeInfo Type;
  TString Bytes;
  bool StringLike = false;
};

TVector<TCase> AllTypes() {
  const auto json = std::get<NBinaryJson::TBinaryJson>(
      NBinaryJson::SerializeToBinaryJson(R"({"a":123})"));
  const auto dyNumber = NDyNumber::ParseDyNumberString("-123.45");
  UNIT_ASSERT(dyNumber);
  const std::array<ui64, 2> decimal = {static_cast<ui64>(-12345), Max<ui64>()};
  return {
      {TTypeInfo(NTypeIds::Bool), CellBytes<ui8>(1)},
      {TTypeInfo(NTypeIds::Int8), CellBytes<i8>(-17)},
      {TTypeInfo(NTypeIds::Uint8), CellBytes<ui8>(201)},
      {TTypeInfo(NTypeIds::Int16), CellBytes<i16>(-1234)},
      {TTypeInfo(NTypeIds::Uint16), CellBytes<ui16>(54321)},
      {TTypeInfo(NTypeIds::Int32), CellBytes<i32>(-123456)},
      {TTypeInfo(NTypeIds::Uint32), CellBytes<ui32>(3000000000u)},
      {TTypeInfo(NTypeIds::Int64), CellBytes<i64>(Min<i64>())},
      {TTypeInfo(NTypeIds::Uint64), CellBytes<ui64>(Max<ui64>())},
      {TTypeInfo(NTypeIds::Float), CellBytes<float>(-12.5f)},
      {TTypeInfo(NTypeIds::Double), CellBytes<double>(-123.25)},
      {TTypeInfo(NTypeIds::Date), CellBytes<ui16>(1234)},
      {TTypeInfo(NTypeIds::Datetime), CellBytes<ui32>(123456)},
      {TTypeInfo(NTypeIds::Timestamp), CellBytes<ui64>(123456789)},
      {TTypeInfo(NTypeIds::Interval), CellBytes<i64>(-123456789)},
      {TTypeInfo(NTypeIds::Date32), CellBytes<i32>(-1234)},
      {TTypeInfo(NTypeIds::Datetime64), CellBytes<i64>(-123456)},
      {TTypeInfo(NTypeIds::Timestamp64), CellBytes<i64>(-123456789)},
      {TTypeInfo(NTypeIds::Interval64), CellBytes<i64>(-123456789)},
      {TTypeInfo(NTypeIds::String), TString("ab\0cd", 5), true},
      {TTypeInfo(NTypeIds::Utf8),
       "\xD0\xBF\xD1\x80\xD0\xB8\xD0\xB2\xD0\xB5\xD1\x82", true},
      {TTypeInfo(NTypeIds::Yson), "{a=1;}", true},
      {TTypeInfo(NTypeIds::Json), R"({"a":1})", true},
      {TTypeInfo(NTypeIds::Uuid), HexDecode("33221100554477668899aabbccddeeff"),
       true},
      {TTypeInfo(NTypeIds::JsonDocument), TString(json.Data(), json.Size()),
       true},
      {TTypeInfo(NTypeIds::DyNumber), *dyNumber, true},
      {TTypeInfo(TDecimalType(22, 9)),
       TString(reinterpret_cast<const char *>(decimal.data()), 16)},
  };
}

void AssertSameState(const NDigest::TState &actual,
                     const NDigest::TState &expected) {
  UNIT_ASSERT_VALUES_EQUAL(actual.Serialize(), expected.Serialize());
}

TString RawNumberDocument(double value) {
  // A top-level number followed by its single double in the number index.
  return HexDecode("81010000220000000402000000000000") + CellBytes(value);
}

NDigest::TState ScalarDigest(NScheme::TTypeId typeId, TStringBuf value,
                            NDigest::EAlgorithm algorithm) {
  TAccumulator accumulator({{"doc", TTypeInfo(typeId), false}}, algorithm);
  accumulator.AddRow({TCell(value.data(), value.size())});
  return accumulator.GetState();
}

} // namespace

Y_UNIT_TEST_SUITE(TNativeTableDigest) {
  Y_UNIT_TEST(AllNativeTypesAndColumnOrder) {
    const auto cases = AllTypes();
    UNIT_ASSERT_VALUES_EQUAL(cases.size(), 27);
    TVector<TColumn> nativeColumns;
    TVector<TCell> cells;
    for (size_t i = 0; i < cases.size(); ++i) {
      const auto &item = cases[i];
      const TString name = "col" + ToString(i);
      nativeColumns.push_back({name, item.Type, true});
      cells.emplace_back(item.Bytes.data(), item.Bytes.size());
    }
    for (auto algorithm :
         {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
          NDigest::EAlgorithm::Sha256}) {
      TAccumulator native(nativeColumns, algorithm);
      native.AddRow(cells);
      const TVector<TCell> nullCells(cells.size());
      native.AddRow(nullCells);
      UNIT_ASSERT_VALUES_EQUAL(native.GetState().GetCount(), 2);

      auto reversedColumns = nativeColumns;
      auto reversedCells = cells;
      std::reverse(reversedColumns.begin(), reversedColumns.end());
      std::reverse(reversedCells.begin(), reversedCells.end());
      TAccumulator reordered(reversedColumns, algorithm);
      reordered.AddRow(nullCells);
      reordered.AddRow(reversedCells);
      AssertSameState(reordered.GetState(), native.GetState());

      TAccumulator onlyNulls(nativeColumns, algorithm);
      onlyNulls.AddRow(nullCells);
      onlyNulls.AddRow(nullCells);
      UNIT_ASSERT(onlyNulls.GetState().GetHash() !=
                  native.GetState().GetHash());
    }
  }

  Y_UNIT_TEST(NativeRowMatchesStablePickleGolden) {
    const TVector<TColumn> columns = {
        {"x", TTypeInfo(NTypeIds::Uint64), false}};
    const TVector<TCell> row = {TCell::Make(ui64(42))};
    // Generated independently by StablePickle(AsStruct(42ul AS x)).
    const auto serialized = HexDecode("0354");
    const auto city128 = ::CityHash128(serialized.data(), serialized.size());
    const NDigest::TWords hashes[] = {
        {::CityHash64(serialized.data(), serialized.size())},
        {city128.first, city128.second},
        // SHA256 of the fixed StablePickle bytes, calculated independently.
        {0x89dba643a7c1b059ULL, 0x506f12a077a9ce0dULL, 0xeeecf767d2f456e7ULL,
         0x3373f27f67346f5aULL},
    };
    size_t index = 0;
    for (auto algorithm :
         {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
          NDigest::EAlgorithm::Sha256}) {
      TAccumulator native(columns, algorithm);
      NDigest::TState expected(algorithm);
      native.AddRow(row);
      expected.AddHash(hashes[index++]);
      AssertSameState(native.GetState(), expected);

      TAccumulator renamed({{"renamed", TTypeInfo(NTypeIds::Uint64), false}},
                           algorithm);
      renamed.AddRow(row);
      AssertSameState(renamed.GetState(), native.GetState());
    }
  }

  Y_UNIT_TEST(JsonDocumentIgnoresBinaryDictionaryOrder) {
    // Both documents encode [1,"alpha",2,"beta"]. The second reverses the
    // string and number indexes and adjusts the four tree offsets accordingly.
    const std::array<TString, 2> documents = {
        HexDecode("0103000080000000e405000083030000e406000003040000"
                  "0200000040050000e0050000616c706861006265746100"
                  "000000000000f03f0000000000000040"),
        HexDecode("0103000080000000e406000003040000e405000083030000"
                  "0200000020050000e00500006265746100616c70686100"
                  "0000000000000040000000000000f03f"),
    };
    UNIT_ASSERT(documents[0] != documents[1]);
    const TString text = NBinaryJson::SerializeToJson(documents[0]);
    for (const auto &document : documents) {
      UNIT_ASSERT(NBinaryJson::IsValidBinaryJson(document));
      UNIT_ASSERT_VALUES_EQUAL(NBinaryJson::SerializeToJson(document), text);
    }
    const auto restored = std::get<NBinaryJson::TBinaryJson>(
        NBinaryJson::SerializeToBinaryJson(text));

    for (auto algorithm :
         {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
          NDigest::EAlgorithm::Sha256}) {
      auto calculate = [&](NScheme::TTypeId typeId, TStringBuf value) {
        TAccumulator accumulator({{"doc", TTypeInfo(typeId), true}}, algorithm);
        accumulator.AddRow({TCell(value.data(), value.size())});
        accumulator.AddRow(TVector<TCell>(1));
        return accumulator.GetState();
      };
      const auto expected = calculate(NTypeIds::Json, text);
      for (const auto &document : documents) {
        AssertSameState(calculate(NTypeIds::JsonDocument, document), expected);
      }
      AssertSameState(calculate(NTypeIds::JsonDocument,
                                TStringBuf(restored.Data(), restored.Size())),
                      expected);
    }
  }

  Y_UNIT_TEST(JsonDocumentCanonicalTextPreservesMembers) {
    const TStringBuf text =
        R"({"a":[true,false,null,{"k":1,"k":1.0000000000000002}],"s":"мир"})";
    const auto document = std::get<NBinaryJson::TBinaryJson>(
        NBinaryJson::SerializeToBinaryJson(text));
    UNIT_ASSERT_VALUES_EQUAL(NDigest::CanonicalJsonDocument(
        TStringBuf(document.Data(), document.Size())), text);
  }

  Y_UNIT_TEST(JsonDocumentPreservesDoublePrecision) {
    const std::array<TString, 2> documents = {
        HexDecode("81010000220000000402000000000000000000000000f03f"),
        HexDecode("81010000220000000402000000000000010000000000f03f"),
    };
    const TStringBuf texts[] = {"1", "1.0000000000000002"};
    for (const auto &document : documents) {
      UNIT_ASSERT(NBinaryJson::IsValidBinaryJson(document));
    }
    for (auto algorithm :
         {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
          NDigest::EAlgorithm::Sha256}) {
      TString previousHash;
      for (size_t i = 0; i < documents.size(); ++i) {
        TAccumulator native({{"doc", TTypeInfo(NTypeIds::JsonDocument), false}},
                            algorithm);
        native.AddRow({TCell(documents[i].data(), documents[i].size())});
        TAccumulator expected({{"doc", TTypeInfo(NTypeIds::Json), false}},
                              algorithm);
        expected.AddRow({TCell(texts[i].data(), texts[i].size())});
        AssertSameState(native.GetState(), expected.GetState());
        const auto hash = native.GetState().GetHashBytes();
        if (i) {
          UNIT_ASSERT(hash != previousHash);
        }
        previousHash = hash;
      }
    }
  }

  Y_UNIT_TEST(JsonDocumentNormalizesSignedZero) {
    const TString negative = RawNumberDocument(-0.0);
    const TString positive = RawNumberDocument(0.0);
    UNIT_ASSERT(negative != positive);
    UNIT_ASSERT(NBinaryJson::IsValidBinaryJson(negative));
    UNIT_ASSERT_VALUES_EQUAL(NDigest::CanonicalJsonDocument(negative), "0");
    const TString exported = NBinaryJson::SerializeToJson(negative);
    const auto restored = std::get<NBinaryJson::TBinaryJson>(
        NBinaryJson::SerializeToBinaryJson(exported));

    for (auto algorithm :
         {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
          NDigest::EAlgorithm::Sha256}) {
      const auto expected = ScalarDigest(NTypeIds::Json, "0", algorithm);
      AssertSameState(ScalarDigest(NTypeIds::JsonDocument, negative, algorithm),
                      expected);
      AssertSameState(ScalarDigest(NTypeIds::JsonDocument, positive, algorithm),
                      expected);
      AssertSameState(ScalarDigest(NTypeIds::JsonDocument,
                                  TStringBuf(restored.Data(), restored.Size()),
                                  algorithm),
                      expected);
    }
  }

  Y_UNIT_TEST(JsonDocumentNonfiniteNumbersRemainDistinctFromStrings) {
    const std::array<std::pair<double, TStringBuf>, 3> cases = {{
        {std::numeric_limits<double>::infinity(), "inf"},
        {-std::numeric_limits<double>::infinity(), "-inf"},
        {std::numeric_limits<double>::quiet_NaN(), "nan"},
    }};
    for (const auto &[number, token] : cases) {
      const TString document = RawNumberDocument(number);
      const TString quoted = "\"" + TString(token) + "\"";
      const auto stringDocument = std::get<NBinaryJson::TBinaryJson>(
          NBinaryJson::SerializeToBinaryJson(quoted));
      UNIT_ASSERT_VALUES_EQUAL(NDigest::CanonicalJsonDocument(document), token);
      for (auto algorithm :
           {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
            NDigest::EAlgorithm::Sha256}) {
        const auto actual =
            ScalarDigest(NTypeIds::JsonDocument, document, algorithm);
        AssertSameState(actual, ScalarDigest(NTypeIds::String, token, algorithm));
        const auto stringDigest = ScalarDigest(
            NTypeIds::JsonDocument,
            TStringBuf(stringDocument.Data(), stringDocument.Size()), algorithm);
        UNIT_ASSERT(actual.GetHash() != stringDigest.GetHash());
      }
    }
  }

  Y_UNIT_TEST(JsonDocumentNonfiniteNumbersInContainers) {
    const auto document = std::get<NBinaryJson::TBinaryJson>(
        NBinaryJson::SerializeToBinaryJson(
            R"({"a":[1e999,"inf",{"n":-1e999},null],"b":false})",
            /*allowInf=*/true));
    const TStringBuf bytes(document.Data(), document.Size());
    const TStringBuf expected =
        R"({"a":[inf,"inf",{"n":-inf},null],"b":false})";
    UNIT_ASSERT_VALUES_EQUAL(NDigest::CanonicalJsonDocument(bytes), expected);
    for (auto algorithm :
         {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
          NDigest::EAlgorithm::Sha256}) {
      AssertSameState(ScalarDigest(NTypeIds::JsonDocument, bytes, algorithm),
                      ScalarDigest(NTypeIds::String, expected, algorithm));
    }
  }

  Y_UNIT_TEST(EmptyStringsAndNullsRemainDistinct) {
    const TVector<TColumn> columns = {{"a", TTypeInfo(NTypeIds::String), true}};
    TAccumulator empty(columns);
    TAccumulator null(columns);
    const TString emptyString;
    const TVector<TCell> emptyRow = {TCell(emptyString.data(), 0)};
    const TVector<TCell> nullRow(1);
    empty.AddRow(emptyRow);
    null.AddRow(nullRow);
    UNIT_ASSERT(empty.GetState().Serialize() != null.GetState().Serialize());
  }

  Y_UNIT_TEST(MergeAndSerializedStatesMatchFullScan) {
    const TVector<TColumn> columns = {{"a", TTypeInfo(NTypeIds::Int64), false}};
    for (auto algorithm :
         {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
          NDigest::EAlgorithm::Sha256}) {
      TAccumulator full(columns, algorithm);
      TAccumulator first(columns, algorithm);
      TAccumulator second(columns, algorithm);
      for (i64 i = -4; i < 9; ++i) {
        const TVector<TCell> row = {TCell::Make(i)};
        full.AddRow(row);
        (i < 2 ? first : second).AddRow(row);
      }
      first.Merge(NDigest::TState::Deserialize(second.GetState().Serialize()));
      AssertSameState(first.GetState(), full.GetState());
    }
  }

  Y_UNIT_TEST(FloatCanonicalizationSurvivesNativeConversion) {
    for (auto [type, width] :
         {std::tuple(NTypeIds::Float, 4u), std::tuple(NTypeIds::Double, 8u)}) {
      const TVector<TColumn> columns = {{"a", TTypeInfo(type), false}};
      auto calculate = [&](ui64 bits) {
        TAccumulator accumulator(columns);
        const TString bytes(reinterpret_cast<const char *>(&bits), width);
        const TVector<TCell> row = {TCell(bytes.data(), bytes.size())};
        accumulator.AddRow(row);
        return accumulator.GetState().Serialize();
      };
      const bool isFloat = type == NTypeIds::Float;
      UNIT_ASSERT_VALUES_EQUAL(
          calculate(0),
          calculate(isFloat ? 0x80000000ULL : 0x8000000000000000ULL));
      UNIT_ASSERT_VALUES_EQUAL(
          calculate(isFloat ? 0x7fc00001ULL : 0x7ff8000000000001ULL),
          calculate(isFloat ? 0xff800001ULL : 0xfff0000000000001ULL));
    }
  }

  Y_UNIT_TEST(RejectsUnsupportedTypesAndInvalidCells) {
    UNIT_ASSERT_EXCEPTION(
        (TAccumulator(
            {{"pg", TTypeInfo(static_cast<const NPg::ITypeDesc *>(nullptr)),
              true}})), yexception);
    UNIT_ASSERT_EXCEPTION(
        (TAccumulator({{"legacy", TTypeInfo(NTypeIds::PairUi64Ui64), true}})),
        yexception);
    UNIT_ASSERT_EXCEPTION(
        (TAccumulator({{"a", TTypeInfo(NTypeIds::Int64), true},
                       {"a", TTypeInfo(NTypeIds::Int64), true}})), yexception);

    for (const auto &item : AllTypes()) {
      if (item.StringLike && item.Type.GetTypeId() != NTypeIds::Uuid) {
        continue;
      }
      TAccumulator accumulator({{"a", item.Type, true}});
      const TString bytes(item.Bytes.size() + 1, 'x');
      const TVector<TCell> row = {TCell(bytes.data(), bytes.size())};
      UNIT_ASSERT_EXCEPTION(accumulator.AddRow(row), yexception);
      UNIT_ASSERT_VALUES_EQUAL(accumulator.GetState().GetCount(), 0);
    }

    TAccumulator nonNullable({{"a", TTypeInfo(NTypeIds::Int64), false}});
    const TVector<TCell> nullRow(1);
    const TVector<TCell> missingRow;
    UNIT_ASSERT_EXCEPTION(nonNullable.AddRow(nullRow), yexception);
    UNIT_ASSERT_EXCEPTION(nonNullable.AddRow(missingRow), yexception);
    const ui8 invalidBool = 2;
    const TVector<TCell> boolRow = {TCell::Make(invalidBool)};
    TAccumulator boolean({{"a", TTypeInfo(NTypeIds::Bool), false}});
    UNIT_ASSERT_EXCEPTION(boolean.AddRow(boolRow), yexception);
  }
}

} // namespace NKikimr::NTableDigest
