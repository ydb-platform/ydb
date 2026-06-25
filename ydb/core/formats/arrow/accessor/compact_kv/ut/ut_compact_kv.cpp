#include <ydb/core/formats/arrow/accessor/compact_kv/serializer.h>
#include <ydb/core/formats/arrow/accessor/compact_kv/settings.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>

#include <optional>

Y_UNIT_TEST_SUITE(CompactKVAccessor) {
    using namespace NKikimr;
    namespace NCompactKV = NKikimr::NArrow::NAccessor::NCompactKV;
    using NCompactKV::TSettings;

    // Builds a plain binary-JSON arrow::BinaryArray from a list of JSON texts
    // (std::nullopt means a null row), mirroring how JsonDocument columns are stored.
    std::shared_ptr<arrow::Array> BuildBinaryJsonArray(const std::vector<std::optional<TString>>& rows) {
        arrow::BinaryBuilder builder;
        for (const auto& row : rows) {
            if (!row) {
                UNIT_ASSERT(builder.AppendNull().ok());
                continue;
            }
            auto bj = NBinaryJson::SerializeToBinaryJson(TStringBuf(row->data(), row->size()));
            const TString* err = std::get_if<TString>(&bj);
            UNIT_ASSERT_C(!err, "cannot build binary json: " << (err ? *err : TString()));
            const auto& binaryJson = std::get<NBinaryJson::TBinaryJson>(bj);
            UNIT_ASSERT(builder.Append(binaryJson.data(), binaryJson.size()).ok());
        }
        std::shared_ptr<arrow::Array> result;
        UNIT_ASSERT(builder.Finish(&result).ok());
        return result;
    }

    // Canonical JSON text (sorted keys, no whitespace) for a single binary-JSON row.
    TString RowToCanonicalJson(const arrow::BinaryArray& arr, ui32 idx) {
        if (arr.IsNull(idx)) {
            return "null";
        }
        const auto view = arr.GetView(idx);
        const TString text = NBinaryJson::SerializeToJson(TStringBuf(view.data(), view.size()));
        NJson::TJsonValue val;
        UNIT_ASSERT(NJson::ReadJsonFastTree(text, &val));
        return NJson::WriteJson(&val, false, true, false);
    }

    TString Canonicalize(const TString& jsonText) {
        NJson::TJsonValue val;
        UNIT_ASSERT(NJson::ReadJsonFastTree(jsonText, &val));
        return NJson::WriteJson(&val, false, true, false);
    }

    // Round-trips through SerializeArray/DeserializeArray (no compression).
    std::shared_ptr<arrow::BinaryArray> RoundTrip(const std::shared_ptr<arrow::Array>& input, const TSettings& settings = {}) {
        const TString blob = NCompactKV::TSerializer::SerializeArray(input, settings, nullptr);
        UNIT_ASSERT(NCompactKV::TSerializer::IsCompactKV(blob));
        auto decoded = NCompactKV::TSerializer::DeserializeArray(blob, input->length(), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(decoded->length(), input->length());
        UNIT_ASSERT_VALUES_EQUAL((int)decoded->type_id(), (int)arrow::Type::BINARY);
        return std::static_pointer_cast<arrow::BinaryArray>(decoded);
    }

    Y_UNIT_TEST(HomogeneousRoundTrip) {
        const std::vector<std::optional<TString>> rows = {
            R"({"a":1,"b":"x"})",
            R"({"a":2,"b":"y"})",
            std::nullopt,
            R"({"a":3,"b":"z"})",
        };
        auto input = BuildBinaryJsonArray(rows);
        auto decoded = RoundTrip(input);
        for (ui32 i = 0; i < rows.size(); ++i) {
            const TString expected = rows[i] ? Canonicalize(*rows[i]) : TString("null");
            UNIT_ASSERT_VALUES_EQUAL_C(RowToCanonicalJson(*decoded, i), expected, "row " << i);
        }
    }

    // Regression test: a key that is a number in one row and a string in another
    // makes MergeTypes() widen the field to String. Previously the numeric value
    // was written as if it were a value-dictionary id, so DeserializeArray threw
    // std::out_of_range ("vector") from valDict.at(id). It must now decode cleanly,
    // with the widened scalar represented as its JSON text.
    Y_UNIT_TEST(HeterogeneousNumberThenStringNoThrow) {
        const std::vector<std::optional<TString>> rows = {
            R"({"x":42})",
            R"({"x":"hello"})",
            R"({"x":7})",
        };
        auto input = BuildBinaryJsonArray(rows);

        std::shared_ptr<arrow::BinaryArray> decoded;
        UNIT_ASSERT_NO_EXCEPTION(decoded = RoundTrip(input));

        // The string row survives verbatim; the numeric rows collapse to their text form.
        UNIT_ASSERT_VALUES_EQUAL(RowToCanonicalJson(*decoded, 0), Canonicalize(R"({"x":"42"})"));
        UNIT_ASSERT_VALUES_EQUAL(RowToCanonicalJson(*decoded, 1), Canonicalize(R"({"x":"hello"})"));
        UNIT_ASSERT_VALUES_EQUAL(RowToCanonicalJson(*decoded, 2), Canonicalize(R"({"x":"7"})"));
    }

    // A value that is an object in one row and a string in another also widens to
    // String and must not corrupt decoding.
    Y_UNIT_TEST(HeterogeneousObjectThenStringNoThrow) {
        const std::vector<std::optional<TString>> rows = {
            R"({"x":{"n":1}})",
            R"({"x":"plain"})",
        };
        auto input = BuildBinaryJsonArray(rows);

        std::shared_ptr<arrow::BinaryArray> decoded;
        UNIT_ASSERT_NO_EXCEPTION(decoded = RoundTrip(input));
        UNIT_ASSERT_VALUES_EQUAL(RowToCanonicalJson(*decoded, 1), Canonicalize(R"({"x":"plain"})"));
    }

    Y_UNIT_TEST(AllNullRoundTrip) {
        const std::vector<std::optional<TString>> rows = {std::nullopt, std::nullopt};
        auto input = BuildBinaryJsonArray(rows);
        auto decoded = RoundTrip(input);
        UNIT_ASSERT(decoded->IsNull(0));
        UNIT_ASSERT(decoded->IsNull(1));
    }
}
