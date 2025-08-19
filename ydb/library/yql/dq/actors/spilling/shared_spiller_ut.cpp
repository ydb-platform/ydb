#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/actors/spilling/channel_storage.h>
#include <ydb/library/yql/dq/actors/spilling/spilling_counters.h>
#include <yql/essentials/minikql/computation/mock_spiller_ut.h>
#include <yql/essentials/utils/chunked_buffer.h>

#include <util/generic/buffer.h>
#include <util/string/builder.h>

using namespace NYql::NDq;
using namespace NKikimr::NMiniKQL;

Y_UNIT_TEST_SUITE(SharedSpillerTests) {

Y_UNIT_TEST(TestSharedSpillerBetweenChannels) {
    // Create shared spiller
    auto sharedSpiller = CreateMockSpiller();
    auto spillingCounters = MakeIntrusive<TSpillingTaskCounters>();
    
    // Create two channel storages using the same spiller
    auto storage1 = CreateDqChannelStorageWithSharedSpiller(1, sharedSpiller, spillingCounters);
    auto storage2 = CreateDqChannelStorageWithSharedSpiller(2, sharedSpiller, spillingCounters);
    
    UNIT_ASSERT(!storage1->IsFull());
    UNIT_ASSERT(!storage2->IsFull());
    UNIT_ASSERT(storage1->IsEmpty());
    UNIT_ASSERT(storage2->IsEmpty());
    
    // Create test data
    NYql::TChunkedBuffer blob1;
    blob1.Append("test data for channel 1", 22);
    
    NYql::TChunkedBuffer blob2;
    blob2.Append("test data for channel 2", 22);
    
    // Put data to both channels
    storage1->Put(100, std::move(blob1));
    storage2->Put(200, std::move(blob2));
    
    UNIT_ASSERT(!storage1->IsEmpty());
    UNIT_ASSERT(!storage2->IsEmpty());
    
    // Verify that both channels can get their data back
    TBuffer result1, result2;
    
    // Data should be available immediately with mock spiller
    UNIT_ASSERT(storage1->Get(100, result1));
    UNIT_ASSERT(storage2->Get(200, result2));
    
    UNIT_ASSERT_STRINGS_EQUAL(TString(result1.data(), result1.size()), "test data for channel 1");
    UNIT_ASSERT_STRINGS_EQUAL(TString(result2.data(), result2.size()), "test data for channel 2");
    
    UNIT_ASSERT(storage1->IsEmpty());
    UNIT_ASSERT(storage2->IsEmpty());
}

Y_UNIT_TEST(TestSpillerCounters) {
    auto sharedSpiller = CreateMockSpiller();
    auto spillingCounters = MakeIntrusive<TSpillingTaskCounters>();
    
    auto storage = CreateDqChannelStorageWithSharedSpiller(1, sharedSpiller, spillingCounters);
    
    // Initial counters should be zero
    UNIT_ASSERT_VALUES_EQUAL(spillingCounters->ChannelWriteBytes.load(), 0);
    
    NYql::TChunkedBuffer blob;
    const char* testData = "test data for counters";
    size_t testDataSize = strlen(testData);
    blob.Append(testData, testDataSize);
    
    storage->Put(100, std::move(blob));
    
    // Counter should be updated
    UNIT_ASSERT_VALUES_EQUAL(spillingCounters->ChannelWriteBytes.load(), testDataSize);
    
    TBuffer result;
    UNIT_ASSERT(storage->Get(100, result));
    UNIT_ASSERT_VALUES_EQUAL(result.size(), testDataSize);
}

Y_UNIT_TEST(TestMultipleBlobs) {
    auto sharedSpiller = CreateMockSpiller();
    auto spillingCounters = MakeIntrusive<TSpillingTaskCounters>();
    
    auto storage = CreateDqChannelStorageWithSharedSpiller(1, sharedSpiller, spillingCounters);
    
    // Put multiple blobs
    for (ui64 i = 1; i <= 5; ++i) {
        NYql::TChunkedBuffer blob;
        TString data = TStringBuilder() << "blob data " << i;
        blob.Append(data.data(), data.size());
        storage->Put(i, std::move(blob));
    }
    
    UNIT_ASSERT(!storage->IsEmpty());
    
    // Get all blobs back in different order
    TBuffer result;
    
    UNIT_ASSERT(storage->Get(3, result));
    UNIT_ASSERT_STRINGS_EQUAL(TString(result.data(), result.size()), "blob data 3");
    
    UNIT_ASSERT(storage->Get(1, result));
    UNIT_ASSERT_STRINGS_EQUAL(TString(result.data(), result.size()), "blob data 1");
    
    UNIT_ASSERT(storage->Get(5, result));
    UNIT_ASSERT_STRINGS_EQUAL(TString(result.data(), result.size()), "blob data 5");
    
    UNIT_ASSERT(storage->Get(2, result));
    UNIT_ASSERT_STRINGS_EQUAL(TString(result.data(), result.size()), "blob data 2");
    
    UNIT_ASSERT(storage->Get(4, result));
    UNIT_ASSERT_STRINGS_EQUAL(TString(result.data(), result.size()), "blob data 4");
    
    UNIT_ASSERT(storage->IsEmpty());
}

Y_UNIT_TEST(TestBlobNotFound) {
    auto sharedSpiller = CreateMockSpiller();
    auto spillingCounters = MakeIntrusive<TSpillingTaskCounters>();
    
    auto storage = CreateDqChannelStorageWithSharedSpiller(1, sharedSpiller, spillingCounters);
    
    TBuffer result;
    // Should throw exception for non-existent blob
    UNIT_ASSERT_EXCEPTION(storage->Get(999, result), TDqChannelStorageException);
}

Y_UNIT_TEST(TestDuplicatePut) {
    auto sharedSpiller = CreateMockSpiller();
    auto spillingCounters = MakeIntrusive<TSpillingTaskCounters>();
    
    auto storage = CreateDqChannelStorageWithSharedSpiller(1, sharedSpiller, spillingCounters);
    
    NYql::TChunkedBuffer blob1;
    blob1.Append("first blob", 10);
    storage->Put(100, std::move(blob1));
    
    NYql::TChunkedBuffer blob2;
    blob2.Append("second blob", 11);
    
    // Should throw exception for duplicate blob ID
    UNIT_ASSERT_EXCEPTION(storage->Put(100, std::move(blob2)), TDqChannelStorageException);
}

Y_UNIT_TEST(TestSharedSpillerEfficiency) {
    auto mockSpiller = std::static_pointer_cast<TMockSpiller>(CreateMockSpiller());
    auto spillingCounters = MakeIntrusive<TSpillingTaskCounters>();
    
    // Create 3 channels using the same spiller
    auto storage1 = CreateDqChannelStorageWithSharedSpiller(1, mockSpiller, spillingCounters);
    auto storage2 = CreateDqChannelStorageWithSharedSpiller(2, mockSpiller, spillingCounters);
    auto storage3 = CreateDqChannelStorageWithSharedSpiller(3, mockSpiller, spillingCounters);
    
    // Put data to all channels
    for (ui64 channelId = 1; channelId <= 3; ++channelId) {
        auto storage = (channelId == 1) ? storage1 : (channelId == 2) ? storage2 : storage3;
        
        for (ui64 blobId = 1; blobId <= 2; ++blobId) {
            NYql::TChunkedBuffer blob;
            TString data = TStringBuilder() << "channel " << channelId << " blob " << blobId;
            blob.Append(data.data(), data.size());
            storage->Put(channelId * 100 + blobId, std::move(blob));
        }
    }
    
    // Verify that all 6 blobs went to the same spiller
    const auto& putSizes = mockSpiller->GetPutSizes();
    UNIT_ASSERT_VALUES_EQUAL(putSizes.size(), 6);
    
    // Get data back from all channels
    TBuffer result;
    UNIT_ASSERT(storage1->Get(101, result));
    UNIT_ASSERT_STRINGS_EQUAL(TString(result.data(), result.size()), "channel 1 blob 1");
    
    UNIT_ASSERT(storage2->Get(202, result));
    UNIT_ASSERT_STRINGS_EQUAL(TString(result.data(), result.size()), "channel 2 blob 2");
    
    UNIT_ASSERT(storage3->Get(301, result));
    UNIT_ASSERT_STRINGS_EQUAL(TString(result.data(), result.size()), "channel 3 blob 1");
}

} // Y_UNIT_TEST_SUITE(SharedSpillerTests)
