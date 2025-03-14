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

    void MeasureSerializationPerformance(size_t payloadSize, size_t iterations) {
        TString testData = GenerateTestData(payloadSize);
        
        // cache warming
        for (size_t i = 0; i < 1000; ++i) {
            NKikimrBlobStorage::TEvVPut msg;
            auto blobId = TLogoBlobID(1, 1, 1, 1, 0x3333, 0x0001A01B);
            LogoBlobIDFromLogoBlobID(blobId, msg.mutable_blobid());
            msg.set_buffer(testData);
            auto vdiskId = TVDiskID(TGroupId::FromValue(1), 1, 1, 1, 1);
            VDiskIDFromVDiskID(vdiskId, msg.mutable_vdiskid());
            msg.set_handleclass(NKikimrBlobStorage::TabletLog);
            
            TString warmupData;
            auto ser = msg.SerializeToString(&warmupData);
            NKikimrBlobStorage::TEvVPut warmupMsg;
            auto der = warmupMsg.ParseFromString(warmupData);
            if (!ser || !der)  {
                --i;
            }
        }
        
        // store in array for adding metrics
        TVector<double> serializationTimes;
        TVector<double> deserializationTimes;
        TVector<size_t> messageSizes;
        serializationTimes.reserve(iterations);
        deserializationTimes.reserve(iterations);
        messageSizes.reserve(iterations);
        
        for (size_t i = 0; i < iterations; ++i) {
            NKikimrBlobStorage::TEvVPut msg;
            
            auto blobId = TLogoBlobID(1, 1, 1, 1, 0x3333, 0x0001A01B);
            LogoBlobIDFromLogoBlobID(blobId, msg.mutable_blobid());
            msg.set_buffer(testData);
            auto vdiskId = TVDiskID(TGroupId::FromValue(1), 1, 1, 1, 1);
            VDiskIDFromVDiskID(vdiskId, msg.mutable_vdiskid());
            msg.set_handleclass(NKikimrBlobStorage::TabletLog);
            
            TString serializedData;
            
            auto startTime = TInstant::Now();
            bool serializeResult = msg.SerializeToString(&serializedData);
            auto serializationTime = (TInstant::Now() - startTime).SecondsFloat();
            UNIT_ASSERT(serializeResult);
            
            startTime = TInstant::Now();
            NKikimrBlobStorage::TEvVPut deserializedMsg;
            bool parseResult = deserializedMsg.ParseFromString(serializedData);
            auto deserializationTime = (TInstant::Now() - startTime).SecondsFloat();
            UNIT_ASSERT(parseResult);
            
            // correctness check
            UNIT_ASSERT_VALUES_EQUAL(msg.buffer(), deserializedMsg.buffer());
            UNIT_ASSERT_VALUES_EQUAL(msg.handleclass(), deserializedMsg.handleclass());
            
            serializationTimes.push_back(serializationTime);
            deserializationTimes.push_back(deserializationTime);
            messageSizes.push_back(serializedData.size());
        }
        
        double avgSerializationTime = 0;
        double avgDeserializationTime = 0;
        double avgMessageSize = 0;
        
        for (size_t i = 0; i < iterations; ++i) {
            avgSerializationTime += serializationTimes[i];
            avgDeserializationTime += deserializationTimes[i];
            avgMessageSize += messageSizes[i];
        }
        
        avgSerializationTime /= iterations;
        avgDeserializationTime /= iterations;
        avgMessageSize /= iterations;
        
        double serializationThroughput = avgMessageSize / avgSerializationTime;
        double deserializationThroughput = avgMessageSize / avgDeserializationTime;
        
        Cerr << "=== Performance Results for " << payloadSize << " bytes payload ===" << Endl;
        Cerr << "Average serialization time: " << (avgSerializationTime * 1e6) << " microseconds" << Endl;
        Cerr << "Average deserialization time: " << (avgDeserializationTime * 1e6) << " microseconds" << Endl;
        Cerr << "Serialization throughput: " << (serializationThroughput / (1024*1024)) << " MB/s" << Endl;
        Cerr << "Deserialization throughput: " << (deserializationThroughput / (1024*1024)) << " MB/s" << Endl;
        Cerr << "Average message size: " << avgMessageSize << " bytes" << Endl;
        Cerr << "=================================================" << Endl;
    }

    Y_UNIT_TEST(TEvVPutSerializationPerformance) {
        const size_t iterations = 100000; 
        
        MeasureSerializationPerformance(32, iterations);
        MeasureSerializationPerformance(128, iterations);
        MeasureSerializationPerformance(4096, iterations);
    }
} 