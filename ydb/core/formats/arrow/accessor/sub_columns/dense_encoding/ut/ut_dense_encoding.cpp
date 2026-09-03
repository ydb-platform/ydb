#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/common/additional_data.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/data_extractor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/dense_encoding/constructors.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/dense_encoding/encoding.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>

#include <ydb/core/formats/arrow/accessor/sub_columns/ut_common/ut_helpers.h>

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

std::shared_ptr<arrow::UInt8Array> MakePositions(const std::vector<std::optional<ui8>>& values) {
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

    Y_UNIT_TEST(UnsupportedDenseEncodingVersion) {
        NKikimrArrowAccessorProto::TConstructor::TSubColumns::TSettings proto;
        proto.SetDenseEncodingVersion(GetMaxDenseEncodingVersion() + 1);
        TSettings settings;
        UNIT_ASSERT(settings.DeserializeFromProto(proto));
        UNIT_ASSERT(!settings.IsDenseEncodingVersionSupported());
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

    void CheckStringArrayRoundTrip(
        const std::vector<std::optional<TString>>& values, const std::shared_ptr<arrow::util::Codec>& codec) {
        CheckBinaryArrayRoundTrip(*MakeBinary(values), codec);
    }

    void CheckIndicesRoundTrip(const std::vector<std::optional<ui8>>& values, const std::shared_ptr<arrow::util::Codec>& codec) {
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
            CheckStringArrayRoundTrip(withNulls, codec);
            CheckStringArrayRoundTrip(dense, codec);
            CheckStringArrayRoundTrip({}, codec);
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
        const std::vector<std::optional<ui8>> indexes = { 2, std::nullopt, 0, 3, std::nullopt, 1 };
        for (const auto& codec : { ZstdCodec(), RawCodec() }) {
            CheckIndicesRoundTrip(indexes, codec);
        }
    }

    Y_UNIT_TEST(EmptyIndicesRoundTrip) {
        for (const auto& codec : { ZstdCodec(), RawCodec() }) {
            CheckIndicesRoundTrip({}, codec);
        }
    }

    Y_UNIT_TEST(DictionaryMetadataSplitsBlobsAndRoundTrips) {
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

    // Dictionary has one value but UInt16 positions; dense encoding narrows them to UInt8.
    Y_UNIT_TEST(DictionaryWithWidePositionsRoundTrips) {
        const auto dictionary = MakeBinary({ "alpha" });
        arrow::UInt16Builder positionsBuilder;
        UNIT_ASSERT(positionsBuilder.Append(0).ok());
        // Exercise null preservation while converting wide positions.
        UNIT_ASSERT(positionsBuilder.AppendNull().ok());
        UNIT_ASSERT(positionsBuilder.Append(0).ok());
        std::shared_ptr<arrow::UInt16Array> positions;
        UNIT_ASSERT(positionsBuilder.Finish(&positions).ok());
        const auto array = std::make_shared<NAccessor::TDictionaryArray>(dictionary, positions);
        const auto serializer = NSerialization::TSerializerContainer::GetDefaultSerializer();
        const NAccessor::TChunkConstructionData constructionData(array->GetRecordsCount(), nullptr, arrow::binary(), serializer);
        const TDictionaryDenseConstructor constructor;

        const auto blobAndMeta = constructor.SerializeToBlobAndMeta(array, constructionData);
        const auto restored = std::static_pointer_cast<NAccessor::TDictionaryArray>(
            constructor.DeserializeFromString(blobAndMeta.Blob, constructionData.WithAdditionalAccessorData(blobAndMeta.Meta)).DetachResult());
        UNIT_ASSERT(restored->GetChunkedArray()->Equals(*array->GetChunkedArray()));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(restored->GetPositions()->type_id()), static_cast<int>(arrow::Type::UINT8));
    }

    // A 256-value dictionary uses UInt16 positions; the slice has one value and uses UInt8.
    Y_UNIT_TEST(DictionarySliceWithWidePositionsRoundTrips) {
        auto builder = NAccessor::TTrivialArray::MakeBuilderBinary(257, 1024);
        // Exercise null preservation while remapping slice positions.
        builder.AddNull(0);
        for (ui32 i = 1; i <= 256; ++i) {
            builder.AddRecord(i, ToString(i));
        }
        const auto source = builder.Finish(257);
        const auto serializer = NSerialization::TSerializerContainer::GetDefaultSerializer();
        const NAccessor::TChunkConstructionData sourceData(source->GetRecordsCount(), nullptr, arrow::binary(), serializer);
        const auto dictionary = std::static_pointer_cast<NAccessor::TDictionaryArray>(
            NAccessor::NDictionary::TConstructor().Construct(source, sourceData).DetachResult());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(dictionary->GetPositions()->type_id()), static_cast<int>(arrow::Type::UINT16));

        const auto slice = std::static_pointer_cast<NAccessor::TDictionaryArray>(dictionary->ISlice(0, 2));
        UNIT_ASSERT_VALUES_EQUAL(slice->GetDictionary()->length(), 1);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(slice->GetPositions()->type_id()), static_cast<int>(arrow::Type::UINT8));

        const NAccessor::TChunkConstructionData sliceData(slice->GetRecordsCount(), nullptr, arrow::binary(), serializer);
        const TDictionaryDenseConstructor constructor;
        const auto blobAndMeta = constructor.SerializeToBlobAndMeta(slice, sliceData);
        const auto restored = std::static_pointer_cast<NAccessor::TDictionaryArray>(
            constructor.DeserializeFromString(blobAndMeta.Blob, sliceData.WithAdditionalAccessorData(blobAndMeta.Meta)).DetachResult());
        UNIT_ASSERT(restored->GetChunkedArray()->Equals(*slice->GetChunkedArray()));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(restored->GetPositions()->type_id()), static_cast<int>(arrow::Type::UINT8));
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

        const std::vector<std::optional<TString>> jsons = {
            R"({"kind":"alpha","id":"rec-00001"})",
            R"({"kind":"beta","id":"rec-00002"})",
            R"({"kind":"alpha"})",
            std::nullopt,
            R"({"kind":"gamma","id":"rec-00005"})",
            R"({"kind":"beta","id":"rec-00006"})",
            R"({"kind":"alpha","id":"rec-00007"})",
            R"({"id":"rec-00008"})",
            R"({"kind":"gamma","id":"rec-00009"})",
            R"({"kind":"beta"})",
        };

        TTrivialArray::TPlainBuilder<arrow::BinaryType> builder;
        for (ui32 i = 0; i < jsons.size(); ++i) {
            if (jsons[i]) {
                auto v = NBinaryJson::SerializeToBinaryJson(*jsons[i]);
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
    void CheckEncodingModes(const bool nativeColumns, const double dictFraction, const TString& what) {
        const auto legacy = RunRoundTrip(/*version*/ 0, nativeColumns, dictFraction);
        for (ui32 version = 1; version <= GetMaxDenseEncodingVersion(); ++version) {
            const auto encoded = RunRoundTrip(version, nativeColumns, dictFraction);
            UNIT_ASSERT_VALUES_EQUAL_C(encoded.Json, legacy.Json, what + " v" + ToString(version));
            UNIT_ASSERT_C(encoded.Blob != legacy.Blob, what + " v" + ToString(version) + ": must change the on-disk bytes");
        }
    }

    Y_UNIT_TEST(PlainArrays) {
        CheckEncodingModes(/*native*/ false, /*dictFraction*/ 0.0, "plain");
    }

    Y_UNIT_TEST(Dictionaries) {
        CheckEncodingModes(/*native*/ false, /*dictFraction*/ 1.0, "dictionary");
    }

    Y_UNIT_TEST(NativeStringColumns) {
        CheckEncodingModes(/*native*/ true, /*dictFraction*/ 0.0, "native");
    }

    Y_UNIT_TEST(NativeAndDictionary) {
        CheckEncodingModes(/*native*/ true, /*dictFraction*/ 1.0, "native_and_dictionary");
    }
}
