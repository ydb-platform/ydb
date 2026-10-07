#include "dq_pq_meta_extractor.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>

namespace NYql::NDq {

namespace {

const std::unordered_map<TString, TPqMetaExtractorLambda> ExtractorsMap = {
    {
        "create_time", [](const NFq::TMessageStreamRecord& record, const TString& /*cluster*/) {
            if (!record.CreateTime) {
                ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::Unsupported)
                    << "Backend does not provide message creation time";
            }
            using TDataType = NUdf::TDataType<NUdf::TTimestamp>;
            return std::make_pair(
                NUdf::TUnboxedValuePod(static_cast<TDataType::TLayout>(record.CreateTime->MicroSeconds())),
                NUdf::GetDataTypeInfo(TDataType::Slot).FixedSize
            );
        }
    },
    {
        "write_time", [](const NFq::TMessageStreamRecord& record, const TString& /*cluster*/) {
            if (!record.WriteTime) {
                ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::Unsupported)
                    << "Backend does not provide message write time";
            }
            using TDataType = NUdf::TDataType<NUdf::TTimestamp>;
            return std::make_pair(
                NUdf::TUnboxedValuePod(static_cast<TDataType::TLayout>(record.WriteTime->MicroSeconds())),
                NUdf::GetDataTypeInfo(TDataType::Slot).FixedSize
            );
        }
    },
    {
        "partition_id", [](const NFq::TMessageStreamRecord& record, const TString& /*cluster*/) {
            using TDataType = NUdf::TDataType<ui64>;
            return std::make_pair(
                NUdf::TUnboxedValuePod(static_cast<TDataType::TLayout>(record.Id.PartitionId.Value)),
                NUdf::GetDataTypeInfo(TDataType::Slot).FixedSize
            );
        }
    },
    {
        "offset", [](const NFq::TMessageStreamRecord& record, const TString& /*cluster*/) {
            using TDataType = NUdf::TDataType<ui64>;
            return std::make_pair(
                NUdf::TUnboxedValuePod(static_cast<TDataType::TLayout>(record.Id.Offset)),
                NUdf::GetDataTypeInfo(TDataType::Slot).FixedSize);
        }
    },
    {
        "message_group_id", [](const NFq::TMessageStreamRecord& record, const TString& /*cluster*/) {
            if (!record.MessageGroupId) {
                ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::Unsupported)
                    << "Backend does not provide message group ID";
            }
            const auto& data = *record.MessageGroupId;
            return std::make_pair(
                NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(data.data(), data.size())),
                static_cast<i64>(data.size())
            );
        }
    },
    {
        "seq_no", [](const NFq::TMessageStreamRecord& record, const TString& /*cluster*/) {
            if (!record.SeqNo) {
                ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::Unsupported)
                    << "Backend does not provide message sequence number";
            }
            using TDataType = NUdf::TDataType<ui64>;
            return std::make_pair(
                NUdf::TUnboxedValuePod(static_cast<TDataType::TLayout>(*record.SeqNo)),
                NUdf::GetDataTypeInfo(TDataType::Slot).FixedSize
            );
        }
    },
};

} // anonymous namespace

TPqMetaExtractorLambda CreatePqMetaExtractorLambda(
    const TString& columnName,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv)
{
    auto key = SkipPqSystemPrefix(columnName);
    if (!key) {
        key = SkipYdbSystemPrefix(columnName);
    }
    Y_ENSURE(key, columnName);

    if (*key == "user_attributes") {
        NKikimr::NMiniKQL::TTypeBuilder typeBuilder(typeEnv);
        NKikimr::NMiniKQL::TType* stringDataType = typeBuilder.NewDataType(NUdf::EDataSlot::String);
        NKikimr::NMiniKQL::TType* messageMetaDictType = typeBuilder.NewDictType(stringDataType, stringDataType, false);
        const auto* holderFactoryPtr = &holderFactory;
        return [holderFactoryPtr, messageMetaDictType](const NFq::TMessageStreamRecord& record, const TString& /*cluster*/) {
            auto dictBuilder = holderFactoryPtr->NewDict(messageMetaDictType, 0);
            i64 usedSpace = 0;
            for (const auto& [k, v] : record.Attributes) {
                auto ks = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(k.data(), k.size()));
                if (!v) {
                    ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::Unsupported)
                        << "PQ user_attributes does not support null attribute values";
                }
                auto vs = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(v->data(), v->size()));
                usedSpace += static_cast<i64>(k.size() + v->size());
                dictBuilder->Add(std::move(ks), std::move(vs));
            }
            NUdf::TUnboxedValue dictValue = dictBuilder->Build();
            return std::make_pair(dictValue.Release(), usedSpace);
        };
    }

    if (*key == "cluster") {
        return [](const NFq::TMessageStreamRecord& /*message*/, const TString& cluster) {
            return std::make_pair(
                NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(cluster.data(), cluster.size())),
                static_cast<i64>(cluster.size())
            );
        };
    }

    const auto iter = ExtractorsMap.find(*key);
    Y_ENSURE(iter != ExtractorsMap.end(), columnName);

    return iter->second;
}

} // namespace NYql::NDq
