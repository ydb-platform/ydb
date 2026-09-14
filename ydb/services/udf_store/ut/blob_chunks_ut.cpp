#include <ydb/services/udf_store/blob_chunks.h>

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

} // Y_UNIT_TEST_SUITE(TBlobChunksTest)
