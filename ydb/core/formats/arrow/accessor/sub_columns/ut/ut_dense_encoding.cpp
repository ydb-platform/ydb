#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/common/additional_data.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/data_extractor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/dense_encoding/constructors.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/dense_encoding/encoding.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>

#include "ut_helpers.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/write.h>

namespace {

std::shared_ptr<arrow::util::Codec> ZstdCodec() {
    return NKikimr::NArrow::TStatusValidator::GetValid(arrow::util::Codec::Create(arrow::Compression::ZSTD));
}

std::shared_ptr<arrow::util::Codec> RawCodec() {
    return nullptr;
}

// std::nullopt entries become nulls.
std::shared_ptr<arrow::BinaryArray> MakeBinary(const std::vector<std::optional<TString>>& values) {
    arrow::BinaryBuilder builder;
    for (const auto& v : values) {
        if (v) {
            UNIT_ASSERT(builder.Append(v->data(), v->size()).ok());
        } else {
            UNIT_ASSERT(builder.AppendNull().ok());
        }
    }
    std::shared_ptr<arrow::BinaryArray> result;
    UNIT_ASSERT(builder.Finish(&result).ok());
    return result;
}

std::shared_ptr<arrow::UInt8Array> MakePositions(const std::vector<std::optional<ui32>>& values) {
    arrow::UInt8Builder builder;
    for (const auto& value : values) {
        if (value) {
            UNIT_ASSERT(builder.Append(*value).ok());
        } else {
            UNIT_ASSERT(builder.AppendNull().ok());
        }
    }
    std::shared_ptr<arrow::UInt8Array> result;
    UNIT_ASSERT(builder.Finish(&result).ok());
    return result;
}

}   // namespace

Y_UNIT_TEST_SUITE(DenseEncoding) {
    using namespace NKikimr::NArrow;
    using namespace NKikimr::NArrow::NAccessor::NSubColumns;

    Y_UNIT_TEST(LengthsRoundTrip) {
        for (const auto& [values, width] : std::vector<std::pair<std::vector<ui32>, ui8>>{
                 { {}, 1 }, { { 0 }, 1 }, { { 0, 0, 0, 0 }, 1 }, { { 0, 5, 5, 12, 100, 100, 200 }, 1 },
                 { { 0, 300, 700 }, 2 }, { { 0, 5, 5, 12, 100, 100, 1000000 }, 4 }, { { 7, 300, 70000, 16000000, 300000000 }, 4 } }) {
            const TString encoded = EncodeLengths(values);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(encoded[0]), width);
            UNIT_ASSERT_VALUES_EQUAL(encoded.size(), 1u + width * values.size());
            const TVector<ui32> decoded = DecodeLengths(encoded, values.size());
            UNIT_ASSERT_VALUES_EQUAL(decoded.size(), values.size());
            for (size_t i = 0; i < values.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(decoded[i], values[i]);
            }
        }
    }

    void CheckBinaryArrayRoundTrip(const arrow::BinaryArray& array, const std::shared_ptr<arrow::util::Codec>& codec) {
        const TString blob = SerializeBinaryArray(array, codec);
        auto restored = DeserializeBinaryArray(blob, array.length(), codec);
        UNIT_ASSERT_VALUES_EQUAL(restored->length(), array.length());
        for (i64 i = 0; i < array.length(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(restored->IsNull(i), array.IsNull(i));
            if (!array.IsNull(i)) {
                const auto view = restored->GetView(i);
                const auto expected = array.GetView(i);
                UNIT_ASSERT_VALUES_EQUAL(TString(view.data(), view.size()), TString(expected.data(), expected.size()));
            }
        }
    }

    void CheckBinaryRoundTrip(
        const std::vector<std::optional<TString>>& values, const std::shared_ptr<arrow::util::Codec>& codec) {
        CheckBinaryArrayRoundTrip(*MakeBinary(values), codec);
    }

    void CheckIndicesRoundTrip(const std::vector<std::optional<ui32>>& values, const std::shared_ptr<arrow::util::Codec>& codec) {
        auto positions = MakePositions(values);
        const auto indexType = std::make_shared<arrow::UInt8Type>();
        const TString blob = SerializeIndices(positions, indexType, codec);
        const auto restored = DeserializeIndices(blob, values.size(), indexType, codec);
        UNIT_ASSERT(restored->Equals(*positions));
    }

    Y_UNIT_TEST(BinaryRoundTrip) {
        const std::vector<std::optional<TString>> withNulls = {
            TString("alpha"), std::nullopt, TString(""), TString("a longer string value"), std::nullopt, TString("z") };
        const std::vector<std::optional<TString>> dense = { TString("a"), TString("bb"), TString("ccc"), TString("dddd") };
        for (const auto& codec : { ZstdCodec(), RawCodec() }) {
            CheckBinaryRoundTrip(withNulls, codec);
            CheckBinaryRoundTrip(dense, codec);
            CheckBinaryRoundTrip({}, codec);
        }
    }

    Y_UNIT_TEST(BinarySliceRoundTrip) {
        const auto array = MakeBinary({
            "0", std::nullopt, "2", "3", std::nullopt, "5", "6", "7", "8", std::nullopt, "10", "11", std::nullopt, "13", "14", "15" });
        const arrow::BinaryArray byteAligned(array->data()->Slice(8, 6));
        const arrow::BinaryArray bitMisaligned(array->data()->Slice(3, 6));
        for (const auto& codec : { ZstdCodec(), RawCodec() }) {
            CheckBinaryArrayRoundTrip(byteAligned, codec);
            CheckBinaryArrayRoundTrip(bitMisaligned, codec);
        }
    }

    Y_UNIT_TEST(IndicesRoundTrip) {
        const std::vector<std::optional<ui32>> indexes = { 2, std::nullopt, 0, 3, std::nullopt, 1 };
        for (const auto& codec : { ZstdCodec(), RawCodec() }) {
            CheckIndicesRoundTrip(indexes, codec);
        }
    }

    Y_UNIT_TEST(DictionaryMetadata) {
        const auto dictionary = MakeBinary({ "alpha", "beta", "gamma" });
        const auto positions = MakePositions({ 0, 1, 0, 2, std::nullopt, 1 });
        const auto array = std::make_shared<NAccessor::TDictionaryArray>(dictionary, positions);
        const auto serializer = NSerialization::TSerializerContainer::GetDefaultSerializer();
        const NAccessor::TChunkConstructionData constructionData(array->GetRecordsCount(), nullptr, arrow::binary(), serializer);
        const TDictionaryDenseConstructor constructor;

        const auto blobAndMeta = constructor.SerializeToBlobAndMeta(array, constructionData);
        const auto* meta = dynamic_cast<const NAccessor::TDictionaryAccessorData*>(blobAndMeta.Meta.get());
        UNIT_ASSERT(meta);
        UNIT_ASSERT_VALUES_EQUAL(meta->DictionaryBlobSize + meta->PositionsBlobSize, blobAndMeta.Blob.size());

        const auto restored = std::static_pointer_cast<NAccessor::TDictionaryArray>(
            constructor.DeserializeFromString(blobAndMeta.Blob, constructionData.WithAdditionalAccessorData(blobAndMeta.Meta)).DetachResult());
        UNIT_ASSERT(restored->GetDictionary()->Equals(*dictionary));
        UNIT_ASSERT(restored->GetPositions()->Equals(*positions));
    }

}

// End-to-end: build a TSubColumnsArray, serialize, deserialize, and compare the reconstructed JSON
// documents with and without dense encoding. dictionaryUniqueFraction 0 keeps binary sub-columns
// as plain Arrays; fraction 1 makes them Dictionary - so the two fractions exercise both paths.
Y_UNIT_TEST_SUITE(DenseEncodingEndToEnd) {
    using namespace NKikimr;
    using namespace NKikimr::NArrow;
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow::NAccessor::NSubColumns;
    using NKikimr::NArrow::NAccessor::NSubColumns::NTesting::PrintBinaryJsons;

    struct TRoundTrip {
        TString Blob;
        TString Json;
    };

    TRoundTrip RunRoundTrip(ui32 encodingVersion, bool nativeColumns, double dictFraction) {
        NSubColumns::TSettings settings(
            4, 1024, 0, /*othersFraction*/ 0, NSubColumns::TDataAdapterContainer::GetDefault(), dictFraction);
        settings.SetDenseEncodingVersion(encodingVersion);
        settings.SetEnableNativeColumns(nativeColumns);

        const std::vector<TString> jsons = {
            R"({"kind":"alpha","id":"rec-00001"})",
            R"({"kind":"beta","id":"rec-00002"})",
            R"({"kind":"alpha"})",
            "null",
            R"({"kind":"gamma","id":"rec-00005"})",
            R"({"kind":"beta","id":"rec-00006"})",
            R"({"kind":"alpha","id":"rec-00007"})",
            R"({"id":"rec-00008"})",
            R"({"kind":"gamma","id":"rec-00009"})",
            R"({"kind":"beta"})",
        };

        TTrivialArray::TPlainBuilder<arrow::BinaryType> builder;
        for (ui32 i = 0; i < jsons.size(); ++i) {
            if (jsons[i] != "null") {
                auto v = NBinaryJson::SerializeToBinaryJson(jsons[i]);
                auto* bj = std::get_if<NBinaryJson::TBinaryJson>(&v);
                UNIT_ASSERT(bj);
                builder.AddRecord(i, std::string_view(bj->data(), bj->size()));
            }
        }
        auto arr = builder.Finish(jsons.size());
        auto data = TSubColumnsArray::Make(arr, settings, arr->GetDataType()).DetachResult();

        auto serializer = NSerialization::TSerializerContainer::GetDefaultSerializer();
        TChunkConstructionData cData(data->GetRecordsCount(), nullptr, arrow::binary(), serializer);
        const TString blob = data->SerializeToString(cData);
        NSubColumns::TConstructor constructor(settings);
        auto restored = constructor.DeserializeFromString(blob, cData).DetachResult();
        return { blob, PrintBinaryJsons(restored->GetChunkedArray()) };
    }

    // Every encoding version must reconstruct the reference documents byte-for-byte, and each must
    // reach a distinct on-disk encoding (otherwise the version silently did nothing).
    void CheckLevers(const bool nativeColumns, const double dictFraction, const TString& what) {
        const auto legacy = RunRoundTrip(/*version*/ 0, nativeColumns, dictFraction);
        for (ui32 version = 1; version <= GetMaxDenseEncodingVersion(); ++version) {
            const auto encoded = RunRoundTrip(version, nativeColumns, dictFraction);
            UNIT_ASSERT_VALUES_EQUAL_C(encoded.Json, legacy.Json, what + " v" + ToString(version));
            UNIT_ASSERT_C(encoded.Blob != legacy.Blob, what + " v" + ToString(version) + ": must change the on-disk bytes");
        }
    }

    Y_UNIT_TEST(PlainArrays) {
        CheckLevers(/*native*/ false, /*dictFraction*/ 0.0, "plain");
    }

    Y_UNIT_TEST(Dictionaries) {
        CheckLevers(/*native*/ false, /*dictFraction*/ 1.0, "dictionary");
    }

    Y_UNIT_TEST(NativeStringColumns) {
        CheckLevers(/*native*/ true, /*dictFraction*/ 0.0, "native");
    }

    Y_UNIT_TEST(PartialReaderKeepsEncodingSettings) {
        NSubColumns::TSettings settings(
            4, 1024, 0, /*othersFraction*/ 0, NSubColumns::TDataAdapterContainer::GetDefault(), /*dictFraction*/ 0.0);
        settings.SetDenseEncodingVersion(1);

        const auto value = NBinaryJson::SerializeToBinaryJson(R"({"key":"value"})");
        const auto* binaryJson = std::get_if<NBinaryJson::TBinaryJson>(&value);
        UNIT_ASSERT(binaryJson);
        TTrivialArray::TPlainBuilder<arrow::BinaryType> builder;
        builder.AddRecord(0, std::string_view(binaryJson->data(), binaryJson->size()));
        auto array = builder.Finish(1);
        auto data = TSubColumnsArray::Make(array, settings, array->GetDataType()).DetachResult();

        const auto serializer = NSerialization::TSerializerContainer::GetDefaultSerializer();
        const TChunkConstructionData constructionData(data->GetRecordsCount(), nullptr, arrow::binary(), serializer);
        const TString blob = data->SerializeToString(constructionData);
        const auto partial = NSubColumns::TConstructor::BuildPartialReader(blob, constructionData, settings).DetachResult();
        UNIT_ASSERT(partial->GetSettings().GetEncodingParams().IsEnabled());
    }
}
