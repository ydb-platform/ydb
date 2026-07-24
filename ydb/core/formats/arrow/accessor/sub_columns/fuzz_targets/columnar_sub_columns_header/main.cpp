#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/header.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <yql/essentials/types/binary_json/write.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <optional>
#include <string_view>
#include <variant>
#include <vector>

namespace {

using namespace NKikimr::NArrow::NAccessor;
using namespace NKikimr::NArrow::NAccessor::NSubColumns;

constexpr size_t MaxInputSize = 1 << 20;
constexpr size_t MaxProtoSize = 64 << 10;
constexpr size_t MaxSerializedBlobSize = 256 << 10;
constexpr ui32 MaxRecords = 8;
constexpr ui32 MaxGeneratedFields = 6;

TChunkConstructionData MakeChunkData(const ui32 recordsCount) {
    return TChunkConstructionData(
        std::max<ui32>(recordsCount, 1),
        nullptr,
        arrow::binary(),
        NKikimr::NArrow::NSerialization::TSerializerContainer::GetFastestSerializer());
}

TSettings MakeSettings(FuzzedDataProvider& fdp) {
    return TSettings(
        fdp.ConsumeIntegralInRange<ui32>(1, 16),
        fdp.ConsumeIntegralInRange<ui32>(0, 6),
        fdp.ConsumeIntegralInRange<ui32>(0, 16 << 10),
        fdp.ConsumeIntegralInRange<ui32>(0, 100) / 100.0,
        TDataAdapterContainer::GetDefault());
}

std::optional<ui32> ReadUi32Prefix(const TString& blob) {
    if (blob.size() < sizeof(ui32)) {
        return std::nullopt;
    }
    ui32 value = 0;
    std::memcpy(&value, blob.data(), sizeof(value));
    return value;
}

TString MakeToken(FuzzedDataProvider& fdp, const size_t maxLen) {
    static constexpr std::string_view alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    TString result;
    const std::string bytes = fdp.ConsumeRandomLengthString(maxLen);
    result.reserve(std::max<size_t>(bytes.size(), 1));
    for (const unsigned char c : bytes) {
        result.push_back(alphabet[c % alphabet.size()]);
    }
    if (!result) {
        result = "x";
    }
    return result;
}

TString MakeScalarJson(FuzzedDataProvider& fdp) {
    switch (fdp.ConsumeIntegralInRange<int>(0, 6)) {
        case 0:
            return ToString(fdp.ConsumeIntegralInRange<int>(-1024, 1024));
        case 1:
            return TStringBuilder() << "\"" << MakeToken(fdp, 16) << "\"";
        case 2:
            return fdp.ConsumeBool() ? "true" : "false";
        case 3:
            return "null";
        case 4:
            return TStringBuilder() << "{\"x\":" << fdp.ConsumeIntegralInRange<int>(-64, 64) << "}";
        case 5:
            return TStringBuilder() << "["
                                    << fdp.ConsumeIntegralInRange<int>(0, 32) << ",\""
                                    << MakeToken(fdp, 8) << "\"," << (fdp.ConsumeBool() ? "true" : "false") << "]";
        default:
            return TStringBuilder() << "{\"deep\":{\"z\":\"" << MakeToken(fdp, 8) << "\"}}";
    }
}

TString MakeObjectJson(FuzzedDataProvider& fdp) {
    static constexpr std::array<std::string_view, 9> keys = {
        "a", "a1", "arr", "b", "b1", "c", "flag", "nested", "s"
    };

    std::array<bool, keys.size()> used = {};
    const ui32 fieldsCount = fdp.ConsumeIntegralInRange<ui32>(0, MaxGeneratedFields);
    TStringBuilder result;
    result << "{";
    bool first = true;
    for (ui32 i = 0; i < fieldsCount; ++i) {
        const size_t keyIndex = fdp.ConsumeIntegralInRange<size_t>(0, keys.size() - 1);
        if (used[keyIndex]) {
            continue;
        }
        used[keyIndex] = true;
        if (!first) {
            result << ",";
        }
        first = false;
        result << "\"" << keys[keyIndex] << "\":" << MakeScalarJson(fdp);
    }
    result << "}";
    return result;
}

TString MakeJsonDocument(FuzzedDataProvider& fdp, const ui32 rowIndex) {
    static const std::array<TString, 6> presets = {
        TString(R"({"a":1,"b":1,"c":"1111"})"),
        TString(R"({"a1":2,"b":2,"c":"2222"})"),
        TString(R"({"a":3,"b":3,"c":"3333","nested":{"x":4}})"),
        TString(R"({"a":5,"b1":5,"arr":[1,"x",false]})"),
        TString(R"({"flag":true,"s":"seed"})"),
        TString(R"({})"),
    };

    if (fdp.ConsumeIntegralInRange<int>(0, 2) != 0) {
        return presets[(rowIndex + fdp.ConsumeIntegralInRange<ui32>(0, presets.size() - 1)) % presets.size()];
    }

    switch (fdp.ConsumeIntegralInRange<int>(0, 5)) {
        case 0:
            return MakeScalarJson(fdp);
        case 1:
            return TStringBuilder() << "[" << MakeScalarJson(fdp) << "," << MakeScalarJson(fdp) << "]";
        default:
            return MakeObjectJson(fdp);
    }
}

std::optional<TString> BuildSerializedSubColumnsBlob(FuzzedDataProvider& fdp, ui32& recordsCount) {
    recordsCount = fdp.ConsumeIntegralInRange<ui32>(1, MaxRecords);

    TTrivialArray::TPlainBuilder<arrow::BinaryType> builder(recordsCount, recordsCount * 64);
    for (ui32 i = 0; i < recordsCount; ++i) {
        if (fdp.ConsumeIntegralInRange<int>(0, 5) == 0) {
            builder.AddNull(i);
            continue;
        }

        const TString json = MakeJsonDocument(fdp, i);
        auto serialized = NKikimr::NBinaryJson::SerializeToBinaryJson(json);
        if (auto* binaryJson = std::get_if<NKikimr::NBinaryJson::TBinaryJson>(&serialized)) {
            builder.AddRecord(i, std::string_view(binaryJson->data(), binaryJson->size()));
        } else {
            builder.AddNull(i);
        }
    }

    auto sourceArray = builder.Finish(recordsCount);
    auto subColumns = TSubColumnsArray::Make(sourceArray, MakeSettings(fdp), sourceArray->GetDataType());
    if (subColumns.IsFail()) {
        return std::nullopt;
    }

    const TString blob = subColumns.GetResult()->SerializeToString(MakeChunkData(recordsCount));
    if (blob.size() > MaxSerializedBlobSize) {
        return std::nullopt;
    }
    return blob;
}

void ExerciseHeaderSizeApis(const TString& blob) {
    try {
        (void)TConstructor::GetHeaderSize(blob);
    } catch (...) {
    }

    const auto protoSize = ReadUi32Prefix(blob);
    if (!protoSize || *protoSize > MaxProtoSize || *protoSize > blob.size() - sizeof(ui32)) {
        return;
    }

    try {
        (void)TConstructor::GetFullHeaderSize(blob);
    } catch (...) {
    }
}

bool TryParseAccessorProto(const TString& blob, NKikimrArrowAccessorProto::TSubColumnsAccessor& proto) {
    const auto protoSize = ReadUi32Prefix(blob);
    if (!protoSize || *protoSize > MaxProtoSize || *protoSize > blob.size() - sizeof(ui32)) {
        return false;
    }
    return proto.ParseFromArray(blob.data() + sizeof(ui32), *protoSize);
}

void ExerciseSafeRawHeader(const TString& blob, const TChunkConstructionData& chunkData) {
    ExerciseHeaderSizeApis(blob);

    NKikimrArrowAccessorProto::TSubColumnsAccessor proto;
    if (!TryParseAccessorProto(blob, proto)) {
        return;
    }

    if (proto.GetColumnStatsSize() || proto.GetOtherStatsSize()) {
        return;
    }

    try {
        auto header = TSubColumnsHeader::ReadHeader(blob, chunkData);
        if (header.IsSuccess()) {
            (void)header.GetResult().DebugJson();
            (void)TConstructor::BuildPartialReader(blob, chunkData);
        }
    } catch (...) {
    }
}

void CheckValidHeaderInvariants(const TString& blob, const TChunkConstructionData& chunkData) {
    auto headerSize = TConstructor::GetHeaderSize(blob);
    Y_ABORT_UNLESS(headerSize.IsSuccess());
    auto fullHeaderSize = TConstructor::GetFullHeaderSize(blob);
    Y_ABORT_UNLESS(fullHeaderSize.IsSuccess());
    Y_ABORT_UNLESS(headerSize.GetResult() <= fullHeaderSize.GetResult());
    Y_ABORT_UNLESS(fullHeaderSize.GetResult() <= blob.size());

    auto headerConclusion = TSubColumnsHeader::ReadHeader(blob, chunkData);
    Y_ABORT_UNLESS(headerConclusion.IsSuccess());
    const auto& header = headerConclusion.GetResult();
    const auto& proto = header.GetAddressesProto();

    Y_ABORT_UNLESS(header.GetHeaderSize() == fullHeaderSize.GetResult());
    Y_ABORT_UNLESS(header.GetColumnStats().GetColumnsCount() == static_cast<ui32>(proto.GetKeyColumns().size()));
    Y_ABORT_UNLESS(header.GetHeaderSize() + header.GetColumnsSize() + header.GetOthersSize() == blob.size());

    ui32 expectedOffset = header.GetHeaderSize();
    for (ui32 i = 0; i < static_cast<ui32>(proto.GetKeyColumns().size()); ++i) {
        const auto range = header.GetColumnReadRange(i);
        Y_ABORT_UNLESS(range.GetOffset() == expectedOffset);
        Y_ABORT_UNLESS(range.GetSize() == proto.GetKeyColumns(i).GetSize());
        Y_ABORT_UNLESS(static_cast<ui64>(range.GetOffset()) + range.GetSize() <= blob.size());
        expectedOffset += range.GetSize();

        const TString name = header.GetColumnStats().GetColumnNameString(i);
        Y_ABORT_UNLESS(header.HasSubColumn(name));
        (void)header.GetAccessorConstructor(i);
        (void)header.GetField(i);
    }

    const auto othersRange = header.GetOthersReadRange();
    Y_ABORT_UNLESS(othersRange.GetOffset() == expectedOffset);
    Y_ABORT_UNLESS(othersRange.GetSize() == header.GetOthersSize());
    Y_ABORT_UNLESS(static_cast<ui64>(othersRange.GetOffset()) + othersRange.GetSize() <= blob.size());

    for (ui32 i = 0; i < header.GetOtherStats().GetColumnsCount(); ++i) {
        const TString name = header.GetOtherStats().GetColumnNameString(i);
        Y_ABORT_UNLESS(header.HasSubColumn(name));
    }

    auto partialConclusion = TConstructor::BuildPartialReader(blob, chunkData);
    Y_ABORT_UNLESS(partialConclusion.IsSuccess());
    const auto partial = partialConclusion.GetResult();
    Y_ABORT_UNLESS(partial->GetRecordsCount() == chunkData.GetRecordsCount());
    Y_ABORT_UNLESS(partial->GetHeader().GetHeaderSize() == header.GetHeaderSize());
    (void)partial->GetHeader().DebugJson();

    for (const TString& name : {TString(R"("a")"), TString(R"("b")"), TString(R"("c")"), TString(R"("nested")")}) {
        (void)partial->HasSubColumnData(name);
        (void)partial->NeedFetch(std::string_view(name.data(), name.size()));
    }
}

void ExerciseGeneratedBlob(const TString& blob, const ui32 recordsCount, FuzzedDataProvider& fdp) {
    const auto chunkData = MakeChunkData(recordsCount);
    CheckValidHeaderInvariants(blob, chunkData);

    TConstructor constructor(TSettings(4, 6, 16 << 10, 0.25, TDataAdapterContainer::GetDefault()));
    auto deserialized = constructor.DeserializeFromString(blob, chunkData);
    Y_ABORT_UNLESS(deserialized.IsSuccess());
    Y_ABORT_UNLESS(deserialized.GetResult()->GetRecordsCount() == recordsCount);
    if (fdp.ConsumeBool()) {
        (void)deserialized.GetResult()->GetChunkedArray();
    }

    const ui32 fullHeaderSize = TConstructor::GetFullHeaderSize(blob).GetResult();
    const TString headerOnly(blob.data(), fullHeaderSize);
    auto headerOnlyConclusion = TSubColumnsHeader::ReadHeader(headerOnly, chunkData);
    Y_ABORT_UNLESS(headerOnlyConclusion.IsSuccess());
    auto partialHeaderOnly = TConstructor::BuildPartialReader(headerOnly, chunkData);
    Y_ABORT_UNLESS(partialHeaderOnly.IsSuccess());

    if (blob.size() > fullHeaderSize) {
        TString bodyMutated = blob;
        const ui32 mutations = fdp.ConsumeIntegralInRange<ui32>(1, 4);
        for (ui32 i = 0; i < mutations; ++i) {
            const size_t pos = fdp.ConsumeIntegralInRange<size_t>(fullHeaderSize, bodyMutated.size() - 1);
            bodyMutated[pos] = bodyMutated[pos] ^ static_cast<char>(1u << fdp.ConsumeIntegralInRange<int>(0, 6));
        }
        CheckValidHeaderInvariants(bodyMutated, chunkData);
        (void)constructor.DeserializeFromString(bodyMutated, chunkData);
    }

    if (fullHeaderSize > sizeof(ui32)) {
        const size_t truncatedSize = fdp.ConsumeIntegralInRange<size_t>(sizeof(ui32), fullHeaderSize - 1);
        ExerciseSafeRawHeader(TString(blob.data(), truncatedSize), chunkData);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > MaxInputSize) {
        return 0;
    }

    const TString raw(reinterpret_cast<const char*>(data), size);
    ExerciseSafeRawHeader(raw, MakeChunkData(std::max<ui32>(1, std::min<size_t>(MaxRecords, size + 1))));

    FuzzedDataProvider fdp(data, size);
    ui32 recordsCount = 0;
    if (const auto generatedBlob = BuildSerializedSubColumnsBlob(fdp, recordsCount)) {
        ExerciseGeneratedBlob(*generatedBlob, recordsCount, fdp);
    }

    return 0;
}
