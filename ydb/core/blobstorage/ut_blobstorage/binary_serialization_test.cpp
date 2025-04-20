#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events_binary.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(BinarySerializationTest) {
    
    TString GenerateTestData(size_t size) {
        TString data;
        data.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            data.append(1, static_cast<char>(i % 256));
        }
        return data;
    }
    
    Y_UNIT_TEST(BasicBinarySerializationTest) {
        TLogoBlobID blobId(123456, 1, 2, 3, 1024, 0);
        TString testData = GenerateTestData(1024);
        TVDiskID vdiskId(TGroupId::FromValue(42), 1, 2, 3, 4);
        
        auto original = MakeHolder<TEvBlobStorage::TEvVPut>(
            blobId, 
            TRope(testData), 
            vdiskId, 
            true, // ignoreBlock
            nullptr, // cookie
            TInstant::Now(), 
            NKikimrBlobStorage::TabletLog
        );
        
        // Convert to binary format
        auto binary = MakeHolder<TEvBlobStorage::TEvVPutBinary>(*original);
        
        // Serialize and deserialize
        TString serialized;
        TEvBlobStorage::TEvVPutBinary::Serialize(*binary, serialized);
        
        auto deserialized = TEvBlobStorage::TEvVPutBinary::Deserialize(serialized);
        UNIT_ASSERT(deserialized != nullptr);
        
        UNIT_ASSERT_VALUES_EQUAL(deserialized->BlobId.ToString(), blobId.ToString());
        UNIT_ASSERT_VALUES_EQUAL(deserialized->VDiskId.ToString(), vdiskId.ToString());
        UNIT_ASSERT_VALUES_EQUAL(deserialized->IgnoreBlock, true);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->Buffer.size(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->Buffer, testData);
        
        delete deserialized;
    }
    
    Y_UNIT_TEST(TEvVPutBinaryConversionTest) {
        const size_t payloadSize = 1024;
        TString testData = GenerateTestData(payloadSize);
        
        auto blobId = TLogoBlobID(1, 1, 1, 1, 0x3333, 0x0001A01B);
        auto vdiskId = TVDiskID(TGroupId::FromValue(1), 1, 1, 1, 1);
        
        ui64 cookie = 0x1234567890ABCDEF;
        auto original = MakeHolder<TEvBlobStorage::TEvVPut>(
            blobId, TRope(testData), vdiskId, true, &cookie, TInstant::Now(), 
            NKikimrBlobStorage::TabletLog
        );
        
        original->Record.AddExtraBlockChecks()->SetTabletId(123);
        original->Record.AddExtraBlockChecks()->SetTabletId(456);
        
        auto binary = MakeHolder<TEvBlobStorage::TEvVPutBinary>(*original);
        
        UNIT_ASSERT_VALUES_EQUAL(binary->BlobId, blobId);
        UNIT_ASSERT_VALUES_EQUAL(binary->VDiskId, vdiskId);
        UNIT_ASSERT_VALUES_EQUAL(binary->IgnoreBlock, true);
        UNIT_ASSERT_VALUES_EQUAL(binary->HasCookie, true);
        UNIT_ASSERT_VALUES_EQUAL(binary->Cookie, cookie);
        UNIT_ASSERT_VALUES_EQUAL(binary->HandleClass, NKikimrBlobStorage::TabletLog);
        UNIT_ASSERT_VALUES_EQUAL(binary->Buffer, testData);
        UNIT_ASSERT_VALUES_EQUAL(binary->ExtraBlockChecks.size(), 2);
        
        auto converted = binary->ToOriginal();
        
        UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(converted->Record.GetBlobID()), blobId);
        UNIT_ASSERT_VALUES_EQUAL(VDiskIDFromVDiskID(converted->Record.GetVDiskID()), vdiskId);
        UNIT_ASSERT_VALUES_EQUAL(converted->Record.GetIgnoreBlock(), true);
        UNIT_ASSERT_VALUES_EQUAL(converted->Record.HasCookie(), true);
        UNIT_ASSERT_VALUES_EQUAL(converted->Record.GetCookie(), cookie);
        UNIT_ASSERT_VALUES_EQUAL(converted->Record.GetHandleClass(), NKikimrBlobStorage::TabletLog);
        UNIT_ASSERT_VALUES_EQUAL(converted->Record.GetBuffer(), testData);
        UNIT_ASSERT_VALUES_EQUAL(converted->Record.ExtraBlockChecksSize(), 2);
        
        delete converted;
    }
    
    Y_UNIT_TEST(TEvVPutBinarySerializationRoundtrip) {
        const size_t payloadSize = 1024;
        TString testData = GenerateTestData(payloadSize);
        
        auto blobId = TLogoBlobID(1, 1, 1, 1, 0x3333, 0x0001A01B);
        auto vdiskId = TVDiskID(TGroupId::FromValue(1), 1, 1, 1, 1);
        
        ui64 cookie = 0x1234567890ABCDEF;
        auto original = MakeHolder<TEvBlobStorage::TEvVPut>(
            blobId, TRope(testData), vdiskId, true, &cookie, TInstant::Now(), 
            NKikimrBlobStorage::TabletLog
        );
        
        original->Record.AddExtraBlockChecks()->SetTabletId(123);
        original->Record.AddExtraBlockChecks()->SetTabletId(456);
        
        auto binary = MakeHolder<TEvBlobStorage::TEvVPutBinary>(*original);
        
        TString serialized;
        TEvBlobStorage::TEvVPutBinary::Serialize(*binary, serialized);
        
        auto deserialized = TEvBlobStorage::TEvVPutBinary::Deserialize(serialized);
        UNIT_ASSERT(deserialized != nullptr);
        
        UNIT_ASSERT_VALUES_EQUAL(deserialized->BlobId, blobId);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->VDiskId, vdiskId);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->IgnoreBlock, true);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->HasCookie, true);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->Cookie, cookie);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->HandleClass, NKikimrBlobStorage::TabletLog);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->Buffer, testData);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->ExtraBlockChecks.size(), 2);
        
        delete deserialized;
    }
    
    Y_UNIT_TEST(TEvVPutBinaryDirectConstructionTest) {
        TLogoBlobID blobId(987654, 10, 20, 30, 2048, 5);
        TString testData = GenerateTestData(2048);
        TVDiskID vdiskId(TGroupId::FromValue(123), 10, 20, 30, 40);
        ui64 cookie = 0xDEADBEEF;
        
        auto binary = MakeHolder<TEvBlobStorage::TEvVPutBinary>(
            blobId,
            testData,
            vdiskId,
            true, // ignoreBlock
            &cookie, //  cookie
            TInstant::Seconds(1000), // deadline
            NKikimrBlobStorage::TabletLog // handleClass
        );
        
        UNIT_ASSERT_VALUES_EQUAL(binary->BlobId, blobId);
        UNIT_ASSERT_VALUES_EQUAL(binary->VDiskId, vdiskId);
        UNIT_ASSERT_VALUES_EQUAL(binary->Buffer, testData);
        UNIT_ASSERT_VALUES_EQUAL(binary->IgnoreBlock, true);
        UNIT_ASSERT_VALUES_EQUAL(binary->HasCookie, true);
        UNIT_ASSERT_VALUES_EQUAL(binary->Cookie, cookie);
        UNIT_ASSERT_VALUES_EQUAL(binary->Deadline, TInstant::Seconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(binary->HandleClass, NKikimrBlobStorage::TabletLog);
        
        TString serialized;
        TEvBlobStorage::TEvVPutBinary::Serialize(*binary, serialized);
        
        auto deserialized = TEvBlobStorage::TEvVPutBinary::Deserialize(serialized);
        UNIT_ASSERT(deserialized != nullptr);
        
        UNIT_ASSERT_VALUES_EQUAL(deserialized->BlobId, blobId);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->VDiskId, vdiskId);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->IgnoreBlock, true);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->HasCookie, true);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->Cookie, cookie);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->Buffer, testData);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->HandleClass, NKikimrBlobStorage::TabletLog);
        
        delete deserialized;
    }
    
    Y_UNIT_TEST(TEvVPutBinarySerializeToArcadiaStreamTest) {
        TLogoBlobID blobId(123456, 1, 2, 3, 1024, 0);
        TString testData = GenerateTestData(1024);
        TVDiskID vdiskId(TGroupId::FromValue(42), 1, 2, 3, 4);
        
        auto original = MakeHolder<TEvBlobStorage::TEvVPut>(
            blobId, TRope(testData), vdiskId, true, nullptr, TInstant::Now(), 
            NKikimrBlobStorage::TabletLog
        );
        
        auto binary = MakeHolder<TEvBlobStorage::TEvVPutBinary>(*original);
        
        NActors::TAllocChunkSerializer serializer;
        bool result = binary->SerializeToArcadiaStream(&serializer);
        UNIT_ASSERT(result);
        
        auto serializedData = serializer.Release(TEventSerializationInfo{});
        UNIT_ASSERT(serializedData);
        UNIT_ASSERT(serializedData->GetSize() > 0);
        
        IEventBase* rawEvent = TEvBlobStorage::TEvVPutBinary::Load(serializedData.Get());
        UNIT_ASSERT(rawEvent);
        
        auto deserializedBinary = dynamic_cast<TEvBlobStorage::TEvVPutBinary*>(rawEvent);
        UNIT_ASSERT(deserializedBinary);
        
        UNIT_ASSERT_VALUES_EQUAL(deserializedBinary->BlobId, blobId);
        UNIT_ASSERT_VALUES_EQUAL(deserializedBinary->Buffer, testData);
        
        delete rawEvent;
    }
    
    Y_UNIT_TEST(TEvVPutBinaryRewriteBlobAndIsInternalTest) {
        TLogoBlobID blobId(123456, 1, 2, 3, 1024, 0);
        TString testData = GenerateTestData(1024);
        TVDiskID vdiskId(TGroupId::FromValue(42), 1, 2, 3, 4);
        
        // Тест через конструктор TEvVPut
        auto original = MakeHolder<TEvBlobStorage::TEvVPut>(
            blobId, TRope(testData), vdiskId, true, nullptr, TInstant::Now(), 
            NKikimrBlobStorage::TabletLog
        );
        
        original->RewriteBlob = true;
        original->IsInternal = true;
        
        auto binaryFromPut = MakeHolder<TEvBlobStorage::TEvVPutBinary>(*original);
        
        UNIT_ASSERT_VALUES_EQUAL(binaryFromPut->RewriteBlob, true);
        UNIT_ASSERT_VALUES_EQUAL(binaryFromPut->IsInternal, true);
        
        TString serialized;
        TEvBlobStorage::TEvVPutBinary::Serialize(*binaryFromPut, serialized);
        
        auto deserialized = TEvBlobStorage::TEvVPutBinary::Deserialize(serialized);
        UNIT_ASSERT(deserialized != nullptr);
        
        UNIT_ASSERT_VALUES_EQUAL(deserialized->RewriteBlob, true);
        UNIT_ASSERT_VALUES_EQUAL(deserialized->IsInternal, true);
        
        // Проверяем обратное преобразование в TEvVPut
        auto convertedBack = deserialized->ToOriginal();
        UNIT_ASSERT_VALUES_EQUAL(convertedBack->RewriteBlob, true);
        UNIT_ASSERT_VALUES_EQUAL(convertedBack->IsInternal, true);
        
        delete deserialized;
        delete convertedBack;
        
        // Тест через прямой конструктор
        auto binaryDirect = MakeHolder<TEvBlobStorage::TEvVPutBinary>(
            blobId,
            testData,
            vdiskId,
            true, // ignoreBlock
            nullptr, // cookie
            TInstant::Now(), // deadline
            NKikimrBlobStorage::TabletLog, // handleClass
            true, // rewriteBlob
            true  // isInternal
        );
        
        UNIT_ASSERT_VALUES_EQUAL(binaryDirect->RewriteBlob, true);
        UNIT_ASSERT_VALUES_EQUAL(binaryDirect->IsInternal, true);
        
        TString serializedDirect;
        TEvBlobStorage::TEvVPutBinary::Serialize(*binaryDirect, serializedDirect);
        
        auto deserializedDirect = TEvBlobStorage::TEvVPutBinary::Deserialize(serializedDirect);
        UNIT_ASSERT(deserializedDirect != nullptr);
        
        UNIT_ASSERT_VALUES_EQUAL(deserializedDirect->RewriteBlob, true);
        UNIT_ASSERT_VALUES_EQUAL(deserializedDirect->IsInternal, true);
        
        // Проверяем обратное преобразование в TEvVPut для прямого конструктора
        auto convertedBackDirect = deserializedDirect->ToOriginal();
        UNIT_ASSERT_VALUES_EQUAL(convertedBackDirect->RewriteBlob, true);
        UNIT_ASSERT_VALUES_EQUAL(convertedBackDirect->IsInternal, true);
        
        delete deserializedDirect;
        delete convertedBackDirect;
    }
    
} // Y_UNIT_TEST_SUITE(BinarySerializationTest) 
