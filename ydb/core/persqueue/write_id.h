#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/kafka_proxy/kafka_producer_instance_id.h>

#include <util/system/types.h>
#include <util/digest/multi.h>
#include <util/stream/output.h>

namespace NKikimr::NPQ {

/**
This class represents identifier of the transaction either in YDB Topic API or in Kafka API.

If this class represents transaction from the Topic API, then nodeId and keyId will be used, where:
- nodeId is the id of the node where KQP, coordinationg this transaction is located
- keyId is the subsequent number of an operation on the node specified with nodeId

If this class represents transaction from the Kafka API, then KafkaProducerInstanceId will be used.
*/
struct TWriteId {
    TWriteId() = default;
    TWriteId(ui64 nodeId, ui64 keyId);
    explicit TWriteId(NKafka::TProducerInstanceId kafkaProducerInstanceId);

    bool operator==(const TWriteId& rhs) const;
    bool operator<(const TWriteId& rhs) const;

    void ToStream(IOutputStream& s) const;
    TString ToString() const;

    size_t GetHash() const
    {
        return KafkaApiTransaction ?
            MultiHash(KafkaProducerInstanceId.Id, KafkaProducerInstanceId.Epoch)
            : MultiHash(NodeId, KeyId);
    }

    bool IsTopicApiTransaction() const 
    {
        return !KafkaApiTransaction;
    }

    bool KafkaApiTransaction = false;
    // these fields are used to identify topic api transaction
    ui64 NodeId = 0;
    ui64 KeyId = 0;
    // Identifies kafka api transaction
    NKafka::TProducerInstanceId KafkaProducerInstanceId;
};

inline
IOutputStream& operator<<(IOutputStream& s, const TWriteId& v)
{
    v.ToStream(s);
    return s;
}

TWriteId GetWriteId(const NKikimrPQ::TTransaction& m);
void SetWriteId(NKikimrPQ::TTransaction& m, const TWriteId& writeId);

TWriteId GetWriteId(const NKikimrPQ::TDataTransaction& m);
void SetWriteId(NKikimrPQ::TDataTransaction& m, const TWriteId& writeId);

TWriteId GetWriteId(const NKikimrPQ::TTabletTxInfo::TTxWriteInfo& m);
void SetWriteId(NKikimrPQ::TTabletTxInfo::TTxWriteInfo& m, const TWriteId& writeId);

TWriteId GetWriteId(const NKikimrClient::TPersQueuePartitionRequest& m);
void SetWriteId(NKikimrClient::TPersQueuePartitionRequest& m, const TWriteId& writeId);

TWriteId GetWriteId(const NKikimrKqp::TTopicOperationsResponse& m);
void SetWriteId(NKikimrKqp::TTopicOperationsResponse& m, const TWriteId& writeId);

}

template <>
struct THash<NKikimr::NPQ::TWriteId> {
    inline size_t operator()(const NKikimr::NPQ::TWriteId& v) const
    {
        return v.GetHash();
    }
};
