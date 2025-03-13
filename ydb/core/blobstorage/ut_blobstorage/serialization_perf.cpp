#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/base/blobstorage.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/hp_timer.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/base.pb.h>

Y_UNIT_TEST_SUITE(SerializationPerformance) {
    
    TString GenerateTestData(size_t size) {
        TString data;
        data.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            data.append(1, static_cast<char>(i % 256));
        }
        return data;
    }

    // Функция для измерения производительности сериализации/десериализации
    void MeasureSerializationPerformance(size_t payloadSize, size_t iterations) {
        TString testData = GenerateTestData(payloadSize);
        
        TString serializedData;
        serializedData.reserve(payloadSize + 1024); 
        
        TInstant startTime;
        double totalSerializationTime = 0;
        double totalDeserializationTime = 0;
        ui64 totalBytes = 0;
        
        for (size_t i = 0; i < iterations; ++i) {
            NKikimrBlobStorage::TEvVPut msg;
            
            auto blobId = TLogoBlobID(1, 
                                    1, 
                                    1, 
                                    1, 
                                    0x3333, 
                                    0x0001A01B); 
            LogoBlobIDFromLogoBlobID(blobId, msg.mutable_blobid());
            
            msg.set_buffer(testData);
            
            auto vdiskId = TVDiskID(TGroupId::FromValue(1), 
                                  1, 
                                  1, 
                                  1, 
                                  1); 
            VDiskIDFromVDiskID(vdiskId, msg.mutable_vdiskid());
            
            msg.set_handleclass(NKikimrBlobStorage::TabletLog);
            
            startTime = TInstant::Now();
            bool serializeResult = msg.SerializeToString(&serializedData);
            totalSerializationTime += (TInstant::Now() - startTime).SecondsFloat();
            UNIT_ASSERT(serializeResult);
            
            startTime = TInstant::Now();
            NKikimrBlobStorage::TEvVPut deserializedMsg;
            bool parseResult = deserializedMsg.ParseFromString(serializedData);
            totalDeserializationTime += (TInstant::Now() - startTime).SecondsFloat();
            UNIT_ASSERT(parseResult);
            
            totalBytes += serializedData.size();
        }
        
        double avgSerializationTime = totalSerializationTime / iterations;
        double avgDeserializationTime = totalDeserializationTime / iterations;
        double totalTime = totalSerializationTime + totalDeserializationTime;
        double throughput = (totalBytes * 2) / totalTime; 
        
        Cerr << "=== Performance Results for " << payloadSize << " bytes payload ===" << Endl;
        Cerr << "Average serialization time: " << (avgSerializationTime * 1e6) << " microseconds" << Endl;
        Cerr << "Average deserialization time: " << (avgDeserializationTime * 1e6) << " microseconds" << Endl;
        Cerr << "Total throughput: " << (throughput / (1024*1024)) << " MB/s" << Endl;
        Cerr << "Average message size: " << (totalBytes / iterations) << " bytes" << Endl;
        Cerr << "=================================================" << Endl;
    }

    Y_UNIT_TEST(TEvVPutSerializationPerformance) {
        const size_t iterations = 100000; 
        
        MeasureSerializationPerformance(32, iterations);
        MeasureSerializationPerformance(128, iterations);
        MeasureSerializationPerformance(4096, iterations);
    }
} 