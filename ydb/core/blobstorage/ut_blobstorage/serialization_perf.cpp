#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/base/blobstorage.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/hp_timer.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/library/actors/core/event_pb.h>

Y_UNIT_TEST_SUITE(SerializationPerformance) {
    
    const size_t DEFAULT_ITERATIONS = 10000;
    
    TString GenerateTestData(size_t size) {
        TString data;
        data.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            data.append(1, static_cast<char>(i % 256));
        }
        return data;
    }
    
    // // Testing serialization performance of the actual TEvBlobStorage::TEvVPut actor
    // void MeasureActorSerializationPerformance(size_t payloadSize, size_t iterations) {
    //     TString testData = GenerateTestData(payloadSize);
        
    //     auto blobId = TLogoBlobID(1, 1, 1, 1, 0x3333, 0x0001A01B);
    //     auto vdiskId = TVDiskID(TGroupId::FromValue(1), 1, 1, 1, 1);
        
    //     TVector<double> serializationTimes;
    //     TVector<double> deserializationTimes;
    //     TVector<size_t> messageSizes;
    //     serializationTimes.reserve(iterations);
    //     deserializationTimes.reserve(iterations);
    //     messageSizes.reserve(iterations);
        
    //     // Cache warm-up
    //     Cerr << "Warming up cache..." << Endl;
        
    //     // Сначала попробуем один раз, чтобы вывести подробную информацию
    //     Cerr << "Testing with a single event to diagnose issues..." << Endl;
        
    //     auto msg = MakeHolder<TEvBlobStorage::TEvVPut>(
    //         blobId, TRope(testData), vdiskId, false, nullptr, TInstant::Max(), 
    //         NKikimrBlobStorage::TabletLog);
        
    //     // Выведем информацию о созданном сообщении
    //     Cerr << "Created message: " << msg->ToString() << Endl;
    //     Cerr << "Event type: " << TEvBlobStorage::EvVPut << Endl;
        
    //     NActors::TAllocChunkSerializer serializer;
    //     if (!msg->SerializeToArcadiaStream(&serializer)) {
    //         Cerr << "Serialization error during initial test" << Endl;
    //         return;
    //     }
        
    //     auto serializedData = serializer.Release(TEventSerializationInfo{});
    //     Cerr << "Serialized data size: " << serializedData->GetSize() << Endl;
        
    //     try {
    //         // Попробуем десериализовать и выведем детали при ошибке
    //         IEventBase* rawEvent = TEvBlobStorage::TEvVPut::Load(serializedData.Get());
    //         if (!rawEvent) {
    //             Cerr << "Deserialization returned nullptr" << Endl;
    //             return;
    //         }
    //         delete rawEvent;
    //         Cerr << "Initial test passed successfully" << Endl;
    //     } catch (const std::exception& e) {
    //         Cerr << "Exception during deserialization: " << e.what() << Endl;
    //         return;
    //     } catch (...) {
    //         Cerr << "Unknown exception during deserialization" << Endl;
    //         return;
    //     }
        
    //     for (size_t i = 0; i < 1000; ++i) {
    //         auto msg = MakeHolder<TEvBlobStorage::TEvVPut>(
    //             blobId, TRope(testData), vdiskId, false, nullptr, TInstant::Max(), 
    //             NKikimrBlobStorage::TabletLog);
            
    //         NActors::TAllocChunkSerializer serializer;
    //         if (!msg->SerializeToArcadiaStream(&serializer)) {
    //             Cerr << "Serialization error during warm-up" << Endl;
    //             return;
    //         }
            
    //         auto serializedData = serializer.Release(TEventSerializationInfo{});
            
    //         IEventBase* rawEvent = TEvBlobStorage::TEvVPut::Load(serializedData.Get());
    //         if (!rawEvent) {
    //             Cerr << "Deserialization error during warm-up" << Endl;
    //             return;
    //         }
    //         delete rawEvent;
    //     }
        
    //     Cerr << "Starting measurements for " << payloadSize << " bytes payload..." << Endl;
        
    //     for (size_t i = 0; i < iterations; ++i) {
    //         auto msg = MakeHolder<TEvBlobStorage::TEvVPut>(
    //             blobId, TRope(testData), vdiskId, false, nullptr, TInstant::Max(), 
    //             NKikimrBlobStorage::TabletLog);
            
    //         NActors::TAllocChunkSerializer serializer;
    //         auto startTime = TInstant::Now();
    //         bool serializeResult = msg->SerializeToArcadiaStream(&serializer);
    //         auto endTime = TInstant::Now();
    //         double serializationTime = (endTime - startTime).SecondsFloat();
            
    //         if (!serializeResult) {
    //             Cerr << "Serialization error in iteration " << i << Endl;
    //             continue;
    //         }
            
    //         auto serializedData = serializer.Release(TEventSerializationInfo{});
    //         size_t messageSize = serializedData->GetSize();
    //         messageSizes.push_back(messageSize);
            
    //         startTime = TInstant::Now();
    //         IEventBase* rawEvent = TEvBlobStorage::TEvVPut::Load(serializedData.Get());
    //         endTime = TInstant::Now();
    //         double deserializationTime = (endTime - startTime).SecondsFloat();
            
    //         if (!rawEvent) {
    //             Cerr << "Deserialization error in iteration " << i << Endl;
    //             continue;
    //         }
            
    //         auto deserializedMsg = dynamic_cast<TEvBlobStorage::TEvVPut*>(rawEvent);
    //         if (!deserializedMsg) {
    //             Cerr << "Invalid type after deserialization in iteration " << i << Endl;
    //             delete rawEvent;
    //             continue;
    //         }
            
    //         bool dataValid = true;
    //         dataValid &= (LogoBlobIDFromLogoBlobID(deserializedMsg->Record.GetBlobID()) == blobId);
    //         dataValid &= (deserializedMsg->Record.GetHandleClass() == NKikimrBlobStorage::TabletLog);
    //         dataValid &= (deserializedMsg->GetBuffer() == testData);
            
    //         if (!dataValid) {
    //             Cerr << "Data after deserialization is invalid in iteration " << i << Endl;
    //         }
            
    //         serializationTimes.push_back(serializationTime);
    //         deserializationTimes.push_back(deserializationTime);
            
    //         delete rawEvent;
    //     }
        
    //     double avgSerializationTime = 0;
    //     double avgDeserializationTime = 0;
    //     double avgMessageSize = 0;
        
    //     for (size_t i = 0; i < messageSizes.size(); ++i) {
    //         avgSerializationTime += serializationTimes[i];
    //         avgDeserializationTime += deserializationTimes[i];
    //         avgMessageSize += messageSizes[i];
    //     }
        
    //     avgSerializationTime /= serializationTimes.size();
    //     avgDeserializationTime /= deserializationTimes.size();
    //     avgMessageSize /= messageSizes.size();
        
    //     double serializationThroughput = (avgMessageSize / 1024.0 / 1024.0) / avgSerializationTime;
    //     double deserializationThroughput = (avgMessageSize / 1024.0 / 1024.0) / avgDeserializationTime;
        
    //     double stddevSerialization = 0;
    //     double stddevDeserialization = 0;
        
    //     for (size_t i = 0; i < serializationTimes.size(); ++i) {
    //         stddevSerialization += 
    //             (serializationTimes[i] - avgSerializationTime) * 
    //             (serializationTimes[i] - avgSerializationTime);
    //     }
        
    //     for (size_t i = 0; i < deserializationTimes.size(); ++i) {
    //         stddevDeserialization += 
    //             (deserializationTimes[i] - avgDeserializationTime) * 
    //             (deserializationTimes[i] - avgDeserializationTime);
    //     }
        
    //     stddevSerialization = std::sqrt(stddevSerialization / serializationTimes.size());
    //     stddevDeserialization = std::sqrt(stddevDeserialization / deserializationTimes.size());
        
    //     Cerr << "===== TEvVPut Actor Serialization Performance Results =====" << Endl;
    //     Cerr << "Data size: " << payloadSize << " bytes" << Endl;
    //     Cerr << "Number of iterations: " << iterations << Endl;
    //     Cerr << "Average message size: " << avgMessageSize << " bytes" << Endl;
    //     Cerr << "Serialization time: " << (avgSerializationTime * 1e6) << " μs (standard deviation: " 
    //          << (stddevSerialization * 1e6) << " μs)" << Endl;
    //     Cerr << "Deserialization time: " << (avgDeserializationTime * 1e6) << " μs (standard deviation: " 
    //          << (stddevDeserialization * 1e6) << " μs)" << Endl;
    //     Cerr << "Serialization throughput: " << serializationThroughput << " MB/s" << Endl;
    //     Cerr << "Deserialization throughput: " << deserializationThroughput << " MB/s" << Endl;
    //     Cerr << "=============================================================" << Endl;
    // }

    // Testing pure protobuf serialization performance
    void MeasureProtobufSerializationPerformance(size_t payloadSize, size_t iterations) {
        TString testData = GenerateTestData(payloadSize);
        
        auto blobId = TLogoBlobID(1, 1, 1, 1, 0x3333, 0x0001A01B);
        auto vdiskId = TVDiskID(TGroupId::FromValue(1), 1, 1, 1, 1);
        
        TVector<double> serializationTimes;
        TVector<double> deserializationTimes;
        TVector<size_t> messageSizes;
        serializationTimes.reserve(iterations);
        deserializationTimes.reserve(iterations);
        messageSizes.reserve(iterations);
        
        Cerr << "Warming up cache for protobuf..." << Endl;
        for (size_t i = 0; i < 1000; ++i) {
            NKikimrBlobStorage::TEvVPut msg;
            LogoBlobIDFromLogoBlobID(blobId, msg.mutable_blobid());
            msg.set_buffer(testData);
            VDiskIDFromVDiskID(vdiskId, msg.mutable_vdiskid());
            msg.set_handleclass(NKikimrBlobStorage::TabletLog);
            
            TString serializedData;
            if (!msg.SerializeToString(&serializedData)) {
                Cerr << "Protobuf serialization error during warm-up" << Endl;
                return;
            }
            
            NKikimrBlobStorage::TEvVPut deserializedMsg;
            if (!deserializedMsg.ParseFromString(serializedData)) {
                Cerr << "Protobuf deserialization error during warm-up" << Endl;
                return;
            }
        }
        
        Cerr << "Starting protobuf measurements for " << payloadSize << " bytes payload..." << Endl;
        
        for (size_t i = 0; i < iterations; ++i) {
            NKikimrBlobStorage::TEvVPut msg;
            LogoBlobIDFromLogoBlobID(blobId, msg.mutable_blobid());
            msg.set_buffer(testData);
            VDiskIDFromVDiskID(vdiskId, msg.mutable_vdiskid());
            msg.set_handleclass(NKikimrBlobStorage::TabletLog);
            
            TString serializedData;
            auto startTime = TInstant::Now();
            bool serializeResult = msg.SerializeToString(&serializedData);
            auto endTime = TInstant::Now();
            double serializationTime = (endTime - startTime).SecondsFloat();
            
            if (!serializeResult) {
                Cerr << "Protobuf serialization error in iteration " << i << Endl;
                continue;
            }
            
            messageSizes.push_back(serializedData.size());
            
            startTime = TInstant::Now();
            NKikimrBlobStorage::TEvVPut deserializedMsg;
            bool parseResult = deserializedMsg.ParseFromString(serializedData);
            endTime = TInstant::Now();
            double deserializationTime = (endTime - startTime).SecondsFloat();
            
            if (!parseResult) {
                Cerr << "Protobuf deserialization error in iteration " << i << Endl;
                continue;
            }
            
            bool dataValid = true;
            dataValid &= (LogoBlobIDFromLogoBlobID(deserializedMsg.GetBlobID()) == blobId);
            dataValid &= (deserializedMsg.GetHandleClass() == NKikimrBlobStorage::TabletLog);
            dataValid &= (deserializedMsg.GetBuffer() == testData);
            
            if (!dataValid) {
                Cerr << "Data after protobuf deserialization is invalid in iteration " << i << Endl;
            }
            
            serializationTimes.push_back(serializationTime);
            deserializationTimes.push_back(deserializationTime);
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
        
        Cerr << "===== Pure Protobuf Serialization Performance Results =====" << Endl;
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

    Y_UNIT_TEST(TEvVPutSerializationPerformance) {
        // Cerr << "\n\n=== TESTING TEvVPut ACTOR SERIALIZATION ===" << Endl;
        // MeasureActorSerializationPerformance(32, DEFAULT_ITERATIONS); 
        // MeasureActorSerializationPerformance(128, DEFAULT_ITERATIONS);   
        // MeasureActorSerializationPerformance(4096, DEFAULT_ITERATIONS);
        
        Cerr << "\n\n=== TESTING PURE PROTOBUF SERIALIZATION ===" << Endl;
        MeasureProtobufSerializationPerformance(32, DEFAULT_ITERATIONS);
        MeasureProtobufSerializationPerformance(128, DEFAULT_ITERATIONS);
        MeasureProtobufSerializationPerformance(4096, DEFAULT_ITERATIONS);
    }
}
