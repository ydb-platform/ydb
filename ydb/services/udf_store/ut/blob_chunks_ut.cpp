#include <ydb/services/udf_store/blob_chunks.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NUdfStore;

Y_UNIT_TEST_SUITE(TBlobChunksTest) {

Y_UNIT_TEST(SplitEmpty) {
    const auto chunks = SplitBlob(TStringBuf());
    UNIT_ASSERT(chunks.empty());
    UNIT_ASSERT(JoinBlobs(chunks).empty());
}

Y_UNIT_TEST(SplitJoinExactMultiple) {
    const TString data(2 * WasmBlobChunkSize, 'a');
    const auto chunks = SplitBlob(data);
    UNIT_ASSERT_VALUES_EQUAL(chunks.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(chunks[0].size(), WasmBlobChunkSize);
    UNIT_ASSERT_VALUES_EQUAL(chunks[1].size(), WasmBlobChunkSize);
    UNIT_ASSERT_VALUES_EQUAL(JoinBlobs(chunks), data);
}

Y_UNIT_TEST(SplitJoinWithRemainder) {
    const ui64 size = WasmBlobChunkSize + 123;
    const TString data(size, 'b');
    const auto chunks = SplitBlob(data);
    UNIT_ASSERT_VALUES_EQUAL(chunks.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(chunks[0].size(), WasmBlobChunkSize);
    UNIT_ASSERT_VALUES_EQUAL(chunks[1].size(), 123u);
    UNIT_ASSERT_VALUES_EQUAL(JoinBlobs(chunks), data);
}

Y_UNIT_TEST(SplitJoinCustomChunkSize) {
    const TString data = "0123456789abcdef";
    const auto chunks = SplitBlob(data, 5);
    UNIT_ASSERT_VALUES_EQUAL(chunks.size(), 4u);
    UNIT_ASSERT_VALUES_EQUAL(chunks[0], "01234");
    UNIT_ASSERT_VALUES_EQUAL(chunks[1], "56789");
    UNIT_ASSERT_VALUES_EQUAL(chunks[2], "abcde");
    UNIT_ASSERT_VALUES_EQUAL(chunks[3], "f");
    UNIT_ASSERT_VALUES_EQUAL(JoinBlobs(chunks), data);
}

Y_UNIT_TEST(LargeBlobOverDatashardLimit) {
    // Datashard MaxWriteValueSize is 16 MiB; we must stay under 8 MiB per cell.
    constexpr ui64 blobSize = 23ull * 1024 * 1024;
    TString data;
    data.reserve(blobSize);
    for (ui64 i = 0; i < blobSize; ++i) {
        data.push_back(char('A' + (i % 26)));
    }
    const auto chunks = SplitBlob(data);
    UNIT_ASSERT(chunks.size() >= 3u);
    for (const auto& chunk : chunks) {
        UNIT_ASSERT(chunk.size() <= WasmBlobChunkSize);
        UNIT_ASSERT(chunk.size() < 16ull * 1024 * 1024);
    }
    UNIT_ASSERT_VALUES_EQUAL(JoinBlobs(chunks), data);
}

Y_UNIT_TEST(VerifyAcceptsAWholeBlob) {
    const TString data = "0123456789abcdef";
    const auto chunks = SplitBlob(data, 5);
    TString body;
    TString error;
    UNIT_ASSERT(JoinAndVerifyBlobs(chunks, chunks.size(), data.size(), MD5::Calc(data), body, error));
    UNIT_ASSERT_VALUES_EQUAL(error, "");
    UNIT_ASSERT_VALUES_EQUAL(body, data);
}

Y_UNIT_TEST(VerifyAcceptsAnEmptyBlob) {
    TString body = "leftover";
    TString error;
    UNIT_ASSERT(JoinAndVerifyBlobs({}, 0, 0, {}, body, error));
    UNIT_ASSERT_VALUES_EQUAL(body, "");
}

Y_UNIT_TEST(VerifyRejectsAMissingChunk) {
    const TString data = "0123456789abcdef";
    auto chunks = SplitBlob(data, 5);
    const ui64 chunkCount = chunks.size();
    chunks.pop_back();
    TString body;
    TString error;
    UNIT_ASSERT(!JoinAndVerifyBlobs(chunks, chunkCount, data.size(), MD5::Calc(data), body, error));
    UNIT_ASSERT_STRING_CONTAINS(error, "chunk_count mismatch");
}

Y_UNIT_TEST(VerifyRejectsATruncatedFinalChunk) {
    // Chunks are whole cells, so losing the tail of the last one keeps the
    // chunk count and changes only the size -- the one thing standing between
    // truncated object code and WAVM on the artifact load path.
    const TString data(WasmBlobChunkSize + 4096, 'z');
    auto chunks = SplitBlob(data);
    UNIT_ASSERT_VALUES_EQUAL(chunks.size(), 2u);
    chunks.back().resize(chunks.back().size() - 1);
    TString body;
    TString error;
    UNIT_ASSERT(!JoinAndVerifyBlobs(chunks, chunks.size(), data.size(), {}, body, error));
    UNIT_ASSERT_STRING_CONTAINS(error, "size mismatch");
}

Y_UNIT_TEST(VerifyRejectsAFlippedByte) {
    const TString data = "0123456789abcdef";
    auto chunks = SplitBlob(data, 5);
    chunks[1].replace(0, 1, "X");
    TString body;
    TString error;
    UNIT_ASSERT(!JoinAndVerifyBlobs(chunks, chunks.size(), data.size(), MD5::Calc(data), body, error));
    UNIT_ASSERT_STRING_CONTAINS(error, "md5 mismatch");
}

Y_UNIT_TEST(VerifyRejectsChunksSwappedAround) {
    // The size and the chunk count both survive a reordering; only md5 does not.
    const TString data = "0123456789abcdef";
    auto chunks = SplitBlob(data, 5);
    std::swap(chunks[0], chunks[1]);
    TString body;
    TString error;
    UNIT_ASSERT(!JoinAndVerifyBlobs(chunks, chunks.size(), data.size(), MD5::Calc(data), body, error));
    UNIT_ASSERT_STRING_CONTAINS(error, "md5 mismatch");
}

} // Y_UNIT_TEST_SUITE(TBlobChunksTest)
