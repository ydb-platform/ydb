#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events_binary.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/hp_timer.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/system/compiler.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(BinarySerializationPerformance) {
    
    const size_t DEFAULT_ITERATIONS = 10000;
    
    TString GenerateTestData(size_t size) {
        TString data;
        data.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            data.append(1, static_cast<char>(i % 256));
        }
        return data;
    }

    void MeasureBinarySerializationPerformance(size_t payloadSize, size_t iterations) {
        TString testData = GenerateTestData(payloadSize);
        
        auto blobId = TLogoBlobID(1, 1, 1, 1, 0x3333, 0x0001A01B);
        auto vdiskId = TVDiskID(TGroupId::FromValue(1), 1, 1, 1, 1);
        
        TVector<double> serializationTimes;
        TVector<double> deserializationTimes;
        TVector<size_t> messageSizes;
        serializationTimes.reserve(iterations);
        deserializationTimes.reserve(iterations);
        messageSizes.reserve(iterations);
        
        Cerr << "Warming up cache for binary format..." << Endl;
        for (size_t i = 0; i < 1000; ++i) {
            auto binaryMsg = MakeHolder<TEvBlobStorage::TEvVPutBinary>(
                blobId, testData, vdiskId
            );
            
            TString serializedData;
            TEvBlobStorage::TEvVPutBinary::Serialize(*binaryMsg, serializedData);
            
            auto deserializedMsg = TEvBlobStorage::TEvVPutBinary::Deserialize(serializedData);
            if (!deserializedMsg) {
                Cerr << "Binary deserialization error during warm-up" << Endl;
                return;
            }
            delete deserializedMsg;
        }
        
        Cerr << "Starting binary measurements for " << payloadSize << " bytes payload..." << Endl;
        
        for (size_t i = 0; i < iterations; ++i) {
            auto binaryMsg = MakeHolder<TEvBlobStorage::TEvVPutBinary>(
                blobId, testData, vdiskId
            );
            
            TString serializedData;
            auto startTime = TInstant::Now();
            TEvBlobStorage::TEvVPutBinary::Serialize(*binaryMsg, serializedData);
            auto endTime = TInstant::Now();
            double serializationTime = (endTime - startTime).SecondsFloat();
            
            size_t messageSize = serializedData.size();
            messageSizes.push_back(messageSize);
            
            startTime = TInstant::Now();
            auto deserializedMsg = TEvBlobStorage::TEvVPutBinary::Deserialize(serializedData);
            endTime = TInstant::Now();
            double deserializationTime = (endTime - startTime).SecondsFloat();
            
            if (!deserializedMsg) {
                Cerr << "Binary deserialization error in iteration " << i << Endl;
                continue;
            }
            
            bool dataValid = true;
            dataValid &= (deserializedMsg->BlobId == blobId);
            dataValid &= (deserializedMsg->VDiskId == vdiskId);
            dataValid &= (deserializedMsg->Buffer == testData);
            
            if (!dataValid) {
                Cerr << "Data after binary deserialization is invalid in iteration " << i << Endl;
            }
            
            serializationTimes.push_back(serializationTime);
            deserializationTimes.push_back(deserializationTime);
            
            delete deserializedMsg;
        }
        
        double avgSerializationTime = 0;
        double avgDeserializationTime = 0;
        double avgMessageSize = 0;
        
        for (size_t i = 0; i < messageSizes.size(); ++i) {
            avgSerializationTime += serializationTimes[i];
            avgDeserializationTime += deserializationTimes[i];
            avgMessageSize += messageSizes[i];
        }
        
        avgSerializationTime /= serializationTimes.size();
        avgDeserializationTime /= deserializationTimes.size();
        avgMessageSize /= messageSizes.size();
        
        double serializationThroughput = (avgMessageSize / 1024.0 / 1024.0) / avgSerializationTime;
        double deserializationThroughput = (avgMessageSize / 1024.0 / 1024.0) / avgDeserializationTime;
        
        double stddevSerialization = 0;
        double stddevDeserialization = 0;
        
        for (size_t i = 0; i < serializationTimes.size(); ++i) {
            stddevSerialization += 
                (serializationTimes[i] - avgSerializationTime) * 
                (serializationTimes[i] - avgSerializationTime);
        }
        
        for (size_t i = 0; i < deserializationTimes.size(); ++i) {
            stddevDeserialization += 
                (deserializationTimes[i] - avgDeserializationTime) * 
                (deserializationTimes[i] - avgDeserializationTime);
        }
        
        stddevSerialization = std::sqrt(stddevSerialization / serializationTimes.size());
        stddevDeserialization = std::sqrt(stddevDeserialization / deserializationTimes.size());
        
        Cerr << "===== TEvVPutBinary Serialization Performance Results =====" << Endl;
        Cerr << "Data size: " << payloadSize << " bytes" << Endl;
        Cerr << "Number of iterations: " << iterations << Endl;
        Cerr << "Average message size: " << avgMessageSize << " bytes" << Endl;
        Cerr << "Serialization time: " << (avgSerializationTime * 1e6) << " μs (standard deviation: " 
             << (stddevSerialization * 1e6) << " μs)" << Endl;
        Cerr << "Deserialization time: " << (avgDeserializationTime * 1e6) << " μs (standard deviation: " 
             << (stddevDeserialization * 1e6) << " μs)" << Endl;
        Cerr << "Serialization throughput: " << serializationThroughput << " MB/s" << Endl;
        Cerr << "Deserialization throughput: " << deserializationThroughput << " MB/s" << Endl;
        Cerr << "=============================================================" << Endl;
    }
    
    void MeasureBinarySizes() {
        std::vector<size_t> testSizes = {32, 128, 512, 1024, 4096, 32768, 65536};
        
        Cerr << "===== Binary Serialization Size Metrics =====" << Endl;
        Cerr << "Payload Size\tTotal Size\tOverhead\tOverhead %\tCompression Ratio" << Endl;
        
        for (size_t size : testSizes) {
            TString testData = GenerateTestData(size);
            auto blobId = TLogoBlobID(1, 1, 1, 1, size, 0);
            auto vdiskId = TVDiskID(TGroupId::FromValue(1), 1, 1, 1, 1);
            
            auto binaryMsg = MakeHolder<TEvBlobStorage::TEvVPutBinary>(
                blobId, testData, vdiskId
            );
            
            TString binaryData;
            TEvBlobStorage::TEvVPutBinary::Serialize(*binaryMsg, binaryData);
            
            size_t binarySize = binaryData.size();
            size_t overhead = binarySize - size;
            double overheadPercent = 100.0 * overhead / binarySize;
            double compressionRatio = (double)binarySize / size;
            
            Cerr << size << "\t" << binarySize << "\t" << overhead << "\t" 
                 << overheadPercent << "%" << "\t" << compressionRatio << "x" << Endl;
        }
        
        Cerr << "=============================================================" << Endl;
    }
    
    Y_UNIT_TEST(TEvVPutBinarySerializationPerformance) {
        Cerr << "\n\n=== TESTING BINARY SERIALIZATION SIZES ===" << Endl;
        MeasureBinarySizes();
        
        Cerr << "\n\n=== TESTING BINARY SERIALIZATION PERFORMANCE ===" << Endl;
        std::vector<size_t> testSizes = {32, 128, 4096};
        
        for (size_t size : testSizes) {
            MeasureBinarySerializationPerformance(size, DEFAULT_ITERATIONS);
        }
    }
    
} // Y_UNIT_TEST_SUITE(BinarySerializationPerformance) 
