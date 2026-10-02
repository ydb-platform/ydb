#include "write_id.h"

#include "pqdata_transaction_compat.h"

#include <util/digest/multi.h>
#include <util/stream/output.h>
#include <util/system/yassert.h>

namespace NKikimr::NPQ {

namespace {

int CompareScalars(auto lhs, auto rhs)
{
    if (lhs < rhs) {
        return -1;
    }
    if (rhs < lhs) {
        return 1;
    }
    return 0;
}

int CompareWriteIdProto(const NKikimrPQ::TWriteId& lhs, const NKikimrPQ::TWriteId& rhs)
{
    if (lhs.Id_case() != rhs.Id_case()) {
        return CompareScalars(lhs.Id_case(), rhs.Id_case());
    }

    switch (lhs.Id_case()) {
        case NKikimrPQ::TWriteId::kTopicApi: {
            const auto& l = lhs.GetTopicApi();
            const auto& r = rhs.GetTopicApi();
            if (auto cmp = CompareScalars(l.GetNodeId(), r.GetNodeId()); cmp != 0) {
                return cmp;
            }
            return CompareScalars(l.GetKeyId(), r.GetKeyId());
        }
        case NKikimrPQ::TWriteId::kKafkaApi: {
            const auto& l = lhs.GetKafkaApi().GetKafkaProducerInstanceId();
            const auto& r = rhs.GetKafkaApi().GetKafkaProducerInstanceId();
            if (auto cmp = CompareScalars(l.GetId(), r.GetId()); cmp != 0) {
                return cmp;
            }
            return CompareScalars(l.GetEpoch(), r.GetEpoch());
        }
        case NKikimrPQ::TWriteId::kDeferredPublicationApi: {
            const auto& l = lhs.GetDeferredPublicationApi();
            const auto& r = rhs.GetDeferredPublicationApi();
            return CompareScalars(l.GetIntPublicationId(), r.GetIntPublicationId());
        }
        case NKikimrPQ::TWriteId::ID_NOT_SET:
            return 0;
    }
    Y_UNREACHABLE();
    return 0;
}

} // namespace

void TWriteId::SyncKafkaProducerInstanceIdFromProto()
{
    if (IsKafkaApiTransaction()) {
        const auto& producerInstanceId = Proto.GetKafkaApi().GetKafkaProducerInstanceId();
        KafkaProducerInstanceId = {producerInstanceId.GetId(), producerInstanceId.GetEpoch()};
    } else {
        KafkaProducerInstanceId = {};
    }
}

TWriteId::TWriteId(ui64 nodeId, ui64 keyId)
{
    auto* topicApi = Proto.MutableTopicApi();
    topicApi->SetNodeId(nodeId);
    topicApi->SetKeyId(keyId);
}

TWriteId::TWriteId(NKafka::TProducerInstanceId kafkaProducerInstanceId)
    : KafkaProducerInstanceId(kafkaProducerInstanceId)
{
    auto* kafkaApi = Proto.MutableKafkaApi();
    auto* producerInstanceId = kafkaApi->MutableKafkaProducerInstanceId();
    producerInstanceId->SetId(kafkaProducerInstanceId.Id);
    producerInstanceId->SetEpoch(kafkaProducerInstanceId.Epoch);
}

TWriteId::TWriteId(NKikimrPQ::TWriteId proto)
    : Proto(std::move(proto))
{
    EnsureCanonical(Proto);
    SyncKafkaProducerInstanceIdFromProto();
}

bool TWriteId::operator==(const TWriteId& rhs) const
{
    return CompareWriteIdProto(Proto, rhs.Proto) == 0;
}

bool TWriteId::operator<(const TWriteId& rhs) const
{
    return CompareWriteIdProto(Proto, rhs.Proto) < 0;
}

size_t TWriteId::GetHash() const
{
    const auto idCase = static_cast<size_t>(Proto.Id_case());
    switch (Proto.Id_case()) {
        case NKikimrPQ::TWriteId::kTopicApi:
            return MultiHash(idCase, Proto.GetTopicApi().GetNodeId(), Proto.GetTopicApi().GetKeyId());
        case NKikimrPQ::TWriteId::kKafkaApi:
            return MultiHash(idCase, KafkaProducerInstanceId.Id, KafkaProducerInstanceId.Epoch);
        case NKikimrPQ::TWriteId::kDeferredPublicationApi:
            return MultiHash(idCase, Proto.GetDeferredPublicationApi().GetIntPublicationId());
        case NKikimrPQ::TWriteId::ID_NOT_SET:
            return MultiHash(idCase);
    }
    Y_UNREACHABLE();
    return 0;
}

void TWriteId::ToStream(IOutputStream& s) const
{
    switch (Proto.Id_case()) {
        case NKikimrPQ::TWriteId::kKafkaApi:
            s << "KafkaTransactionWriteId{" << KafkaProducerInstanceId.Id << ", " << KafkaProducerInstanceId.Epoch << '}';
            break;
        case NKikimrPQ::TWriteId::kTopicApi:
            s << '{' << GetNodeId() << ", " << GetKeyId() << '}';
            break;
        case NKikimrPQ::TWriteId::kDeferredPublicationApi: {
            const auto& deferredPublicationApi = Proto.GetDeferredPublicationApi();
            s << "DeferredPublicationWriteId{" << deferredPublicationApi.GetIntPublicationId()
                << ", " << deferredPublicationApi.GetExtPublicationId() << '}';
            break;
        }
        case NKikimrPQ::TWriteId::ID_NOT_SET:
            s << "{}";
            break;
    }
}

TString TWriteId::ToString() const {
    TStringStream ss;
    ToStream(ss);
    return ss.Str();
}

template <class T>
TWriteId GetWriteIdImpl(const T& m)
{
    return TWriteId(m.GetWriteId());
}

template <class T>
void SetWriteIdImpl(T& m, const TWriteId& writeId)
{
    auto* w = m.MutableWriteId();
    *w = writeId.GetProto();
    DowngradeToLegacy(*w);
}

TWriteId GetWriteId(const NKikimrPQ::TTransaction& m)
{
    return GetWriteIdImpl(m);
}

void SetWriteId(NKikimrPQ::TTransaction& m, const TWriteId& writeId)
{
    SetWriteIdImpl(m, writeId);
}

TWriteId GetWriteId(const NKikimrPQ::TDataTransaction& m)
{
    return GetWriteIdImpl(m);
}

void SetWriteId(NKikimrPQ::TDataTransaction& m, const TWriteId& writeId)
{
    SetWriteIdImpl(m, writeId);
}

TWriteId GetWriteId(const NKikimrPQ::TTabletTxInfo::TTxWriteInfo& m)
{
    return GetWriteIdImpl(m);
}

void SetWriteId(NKikimrPQ::TTabletTxInfo::TTxWriteInfo& m, const TWriteId& writeId)
{
    SetWriteIdImpl(m, writeId);
}

TWriteId GetWriteId(const NKikimrClient::TPersQueuePartitionRequest& m)
{
    return GetWriteIdImpl(m);
}

void SetWriteId(NKikimrClient::TPersQueuePartitionRequest& m, const NKikimr::NPQ::TWriteId& writeId)
{
    SetWriteIdImpl(m, writeId);
}

TWriteId GetWriteId(const NKikimrKqp::TTopicOperationsResponse& m)
{
    const auto& writeId = m.GetWriteId();
    if (writeId.GetKafkaTransaction()) {
        const auto& kafkaProducerInstanceId = writeId.GetKafkaProducerInstanceId();
        return TWriteId{NKafka::TProducerInstanceId{kafkaProducerInstanceId.GetId(), kafkaProducerInstanceId.GetEpoch()}};
    }
    return {writeId.GetNodeId(), writeId.GetKeyId()};
}

void SetWriteId(NKikimrKqp::TTopicOperationsResponse& m, const TWriteId& writeId)
{
    auto* w = m.MutableWriteId();
    if (writeId.IsKafkaApiTransaction()) {
        const auto& producerInstanceId = writeId.GetKafkaProducerInstanceId();
        w->SetKafkaTransaction(true);
        auto* kafkaProducerInstanceId = w->MutableKafkaProducerInstanceId();
        kafkaProducerInstanceId->SetId(producerInstanceId.Id);
        kafkaProducerInstanceId->SetEpoch(producerInstanceId.Epoch);
    } else if (writeId.IsTopicApiTransaction()) {
        w->SetKafkaTransaction(false);
        w->SetNodeId(writeId.GetNodeId());
        w->SetKeyId(writeId.GetKeyId());
    }
}

} // namespace NKikimr::NPQ

template <>
void Out<NKikimr::NPQ::TWriteId>(IOutputStream& s, const NKikimr::NPQ::TWriteId& v)
{
    v.ToStream(s);
}
