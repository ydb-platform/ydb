#include "dq_pq_meta_extractor.h"

#include <optional>

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>

#include <util/generic/string.h>

namespace {
    const std::unordered_map<TString, NYql::NDq::TPqMetaExtractor::TPqMetaExtractorLambda> ExtractorsMap = {
        {
            "_yql_sys_create_time", [](const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TMessage& message){
                return NYql::NUdf::TUnboxedValuePod(static_cast<NYql::NUdf::TDataType<NYql::NUdf::TTimestamp>::TLayout>(message.GetCreateTime().MicroSeconds()));
            }
        },
        {
            "_yql_sys_write_time", [](const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TMessage& message){
                return NYql::NUdf::TUnboxedValuePod(static_cast<NYql::NUdf::TDataType<NYql::NUdf::TTimestamp>::TLayout>(message.GetWriteTime().MicroSeconds()));
            }
        },
        {
            "_yql_sys_partition_id", [](const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TMessage& message){
                return NYql::NUdf::TUnboxedValuePod(message.GetPartitionStream()->GetPartitionId());
            }
        },
        {
            "_yql_sys_offset", [](const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TMessage& message){
                return NYql::NUdf::TUnboxedValuePod(message.GetOffset());
            }
        },
        {
            "_yql_sys_message_group_id", [](const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TMessage& message){
                const auto& data = message.GetMessageGroupId();
                return NKikimr::NMiniKQL::MakeString(NYql::NUdf::TStringRef(data.Data(), data.Size()));
            }
        },
        {
            "_yql_sys_seq_no", [](const NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent::TMessage& message){
                return NYql::NUdf::TUnboxedValuePod(static_cast<NYql::NUdf::TDataType<NYql::NUdf::TTimestamp>::TLayout>(message.GetSeqNo()));
            }
        },
    };
}

namespace NYql::NDq {

TPqMetaExtractor::TPqMetaExtractor() {
    for (auto key : AllowedPqMetaSysColumns()) {
        Y_ENSURE(
            ExtractorsMap.contains(key),
            "Pq metadata field " << key << " hasn't valid runtime extractor. You should add it.");
    }
}

TPqMetaExtractor::TPqMetaExtractorLambda TPqMetaExtractor::FindExtractorLambda(TString sysColumn) const {
    auto iter = ExtractorsMap.find(sysColumn);
    Y_ENSURE(iter != ExtractorsMap.end(), sysColumn);

    return iter->second;
}

}
