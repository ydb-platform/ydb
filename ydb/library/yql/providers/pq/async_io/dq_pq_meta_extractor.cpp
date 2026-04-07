#include "dq_pq_meta_extractor.h"

#include <yql/essentials/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>

namespace NYql::NDq {

namespace {

const std::unordered_map<TString, TPqMetaExtractor::TPqMetaExtractorLambda> ExtractorsMap = {
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
                data.size()
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

TPqMetaExtractor::TPqMetaExtractor() {
    for (const auto& sysColumn : AllowedPqMetaSysColumns(true)) {
        const auto key = SkipPqSystemPrefix(sysColumn);
        Y_ENSURE(key, sysColumn);
        Y_ENSURE(ExtractorsMap.contains(*key), "Pq metadata field " << *key << " hasn't valid runtime extractor. You should add it.");
    }
}

TPqMetaExtractor::TPqMetaExtractorLambda TPqMetaExtractor::FindExtractorLambda(const TString& sysColumn) const {
    const auto key = SkipPqSystemPrefix(sysColumn);
    Y_ENSURE(key, sysColumn);

    const auto iter = ExtractorsMap.find(*key);
    Y_ENSURE(iter != ExtractorsMap.end(), sysColumn);

    return iter->second;
}

} // namespace NYql::NDq
