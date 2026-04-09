#include "dq_pq_meta_extractor.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>

namespace NYql::NDq {

namespace {

const std::unordered_map<TString, TPqMetaExtractor::TPqMetaExtractorLambda> PlainExtractorsMap = {
    {
        "create_time", [](const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
            using TDataType = NUdf::TDataType<NUdf::TTimestamp>;
            return std::make_pair(
                NUdf::TUnboxedValuePod(static_cast<TDataType::TLayout>(message.GetCreateTime().MicroSeconds())),
                NUdf::GetDataTypeInfo(TDataType::Slot).FixedSize
            );
        }
    },
    {
        "write_time", [](const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
            using TDataType = NUdf::TDataType<NUdf::TTimestamp>;
            return std::make_pair(
                NUdf::TUnboxedValuePod(static_cast<TDataType::TLayout>(message.GetWriteTime().MicroSeconds())),
                NUdf::GetDataTypeInfo(TDataType::Slot).FixedSize
            );
        }
    },
    {
        "partition_id", [](const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
            using TDataType = NUdf::TDataType<ui64>;
            return std::make_pair(
                NUdf::TUnboxedValuePod(static_cast<TDataType::TLayout>(message.GetPartitionSession()->GetPartitionId())),
                NUdf::GetDataTypeInfo(TDataType::Slot).FixedSize
            );
        }
    },
    {
        "offset", [](const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
            using TDataType = NUdf::TDataType<ui64>;
            return std::make_pair(
                NUdf::TUnboxedValuePod(static_cast<TDataType::TLayout>(message.GetOffset())),
                NUdf::GetDataTypeInfo(TDataType::Slot).FixedSize);
        }
    },
    {
        "message_group_id", [](const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
            const auto& data = message.GetMessageGroupId();
            return std::make_pair(
                NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(data.data(), data.size())),
                static_cast<i64>(data.size())
            );
        }
    },
    {
        "seq_no", [](const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
            using TDataType = NUdf::TDataType<ui64>;
            return std::make_pair(
                NUdf::TUnboxedValuePod(static_cast<TDataType::TLayout>(message.GetSeqNo())),
                NUdf::GetDataTypeInfo(TDataType::Slot).FixedSize
            );
        }
    },
};

} // anonymous namespace

TPqMetaExtractor::TPqMetaExtractor(const NKikimr::NMiniKQL::THolderFactory& holderFactory, const NKikimr::NMiniKQL::TType* messageMetaDictType)
    : HolderFactory_(holderFactory)
    , MessageMetaDictType_(messageMetaDictType)
{
    for (const auto& sysColumn : GetAllowedPqMetaSysColumns(true)) {
        const auto key = SkipPqSystemPrefix(sysColumn);
        Y_ENSURE(key, sysColumn);
        Y_ENSURE(*key == "message_meta" || PlainExtractorsMap.contains(*key),
            "Pq metadata field " << *key << " hasn't valid runtime extractor. You should add it.");
    }
}

TPqMetaExtractor::TPqMetaExtractorLambda TPqMetaExtractor::FindExtractorLambda(const TString& sysColumn) const {
    const auto key = SkipPqSystemPrefix(sysColumn);
    Y_ENSURE(key, sysColumn);

    if (*key == "message_meta") {
        return [this](const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
            auto dictBuilder = HolderFactory_.NewDict(MessageMetaDictType_, 0);
            i64 usedSpace = 0;
            if (const auto& metaPtr = message.GetMessageMeta()) {
                for (const auto& [k, v] : metaPtr->Fields) {
                    auto ks = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(k.data(), k.size()));
                    auto vs = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(v.data(), v.size()));
                    usedSpace += static_cast<i64>(k.size() + v.size());
                    dictBuilder->Add(std::move(ks), std::move(vs));
                }
            }
            NUdf::TUnboxedValue dictValue = dictBuilder->Build();
            return std::make_pair(dictValue.Release(), usedSpace);
        };
    }

    const auto iter = PlainExtractorsMap.find(*key);
    Y_ENSURE(iter != PlainExtractorsMap.end(), sysColumn);

    return iter->second;
}

} // namespace NYql::NDq
