#include <benchmark/benchmark.h>

#include <library/cpp/yt/logging/tag.h>
#include <library/cpp/yt/logging/tagged_payload.h>

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/string_builder.h>

#include <library/cpp/yt/string/guid.h>

#include <library/cpp/yt/misc/guid.h>

#include <util/generic/string.h>

#include <cstring>
#include <string>

namespace NYT::NLogging {
namespace {

////////////////////////////////////////////////////////////////////////////////

const TGuid ChunkId(0x12345678, 0x9abcdef0, 0x11223344, 0x55667788);
const std::string Address = "some-node.example.net:9012";
const std::string LongValue(4096, 'x');

////////////////////////////////////////////////////////////////////////////////

//! The pre-optimization path, reproduced so both can be measured in one binary.
class TTagListViaTemporary
{
public:
    template <class TValue>
    TTagListViaTemporary& Add(TStringBuf key, const TValue& value)
    {
        TStringBuilder builder;
        FormatValue(&builder, value, "v"_sb);
        auto value_ = builder.GetBuffer();

        auto keySize = static_cast<ui32>(key.size());
        auto valueSize = static_cast<ui32>(value_.size());
        auto& buffer = Payload_.Underlying();
        auto offset = buffer.size();
        ResizeUninitialized(buffer, offset + 2 * sizeof(ui32) + key.size() + value_.size());

        char* ptr = buffer.data() + offset;
        auto write = [&] (const void* data, size_t size) {
            ::memcpy(ptr, data, size);
            ptr += size;
        };
        write(&keySize, sizeof(keySize));
        write(key.data(), key.size());
        write(&valueSize, sizeof(valueSize));
        write(value_.data(), value_.size());
        return *this;
    }

private:
    TLoggingTagListPayload Payload_;
};

////////////////////////////////////////////////////////////////////////////////

void BM_TagListViaFormat(benchmark::State& state)
{
    for (auto _ : state) {
        auto result = Format(
            "ChunkId: %v, Address: %v, Index: %v, Count: %v, Sealed: %v, Offset: %v",
            ChunkId,
            Address,
            17,
            123456,
            true,
            8192);
        benchmark::DoNotOptimize(result);
    }
}

BENCHMARK(BM_TagListViaFormat);

void BM_TagListWithTemporary(benchmark::State& state)
{
    for (auto _ : state) {
        TTagListViaTemporary tags;
        tags
            .Add("ChunkId", ChunkId)
            .Add("Address", Address)
            .Add("Index", 17)
            .Add("Count", 123456)
            .Add("Sealed", true)
            .Add("Offset", 8192);
        benchmark::DoNotOptimize(tags);
    }
}

BENCHMARK(BM_TagListWithTemporary);

void BM_TagList(benchmark::State& state)
{
    for (auto _ : state) {
        auto tags = TLoggingTagList()
            .With("ChunkId", ChunkId)
            .With("Address", Address)
            .With("Index", 17)
            .With("Count", 123456)
            .With("Sealed", true)
            .With("Offset", 8192);
        benchmark::DoNotOptimize(tags);
    }
}

BENCHMARK(BM_TagList);

////////////////////////////////////////////////////////////////////////////////

void BM_TagListSingleTagWithTemporary(benchmark::State& state)
{
    for (auto _ : state) {
        TTagListViaTemporary tags;
        tags.Add("ChunkId", ChunkId);
        benchmark::DoNotOptimize(tags);
    }
}

BENCHMARK(BM_TagListSingleTagWithTemporary);

void BM_TagListSingleTag(benchmark::State& state)
{
    for (auto _ : state) {
        auto tags = TLoggingTagList().With("ChunkId", ChunkId);
        benchmark::DoNotOptimize(tags);
    }
}

BENCHMARK(BM_TagListSingleTag);

////////////////////////////////////////////////////////////////////////////////

//! Past TStringBuilderBase::MinBufferLength, so both paths must grow.
void BM_TagListLongValueWithTemporary(benchmark::State& state)
{
    for (auto _ : state) {
        TTagListViaTemporary tags;
        tags.Add("Payload", LongValue);
        benchmark::DoNotOptimize(tags);
    }
}

BENCHMARK(BM_TagListLongValueWithTemporary);

void BM_TagListLongValue(benchmark::State& state)
{
    for (auto _ : state) {
        auto tags = TLoggingTagList().With("Payload", LongValue);
        benchmark::DoNotOptimize(tags);
    }
}

BENCHMARK(BM_TagListLongValue);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
