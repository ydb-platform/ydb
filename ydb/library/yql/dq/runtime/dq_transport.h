#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/common/dq_value.h>
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>


namespace NYql::NDq {

class TDqDataSerializer : private TNonCopyable {
public:
    TDqDataSerializer(const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory, NDqProto::EDataTransportVersion transportVersion)
        : TypeEnv(typeEnv)
        , HolderFactory(holderFactory)
        , TransportVersion(transportVersion) {}

    NDqProto::EDataTransportVersion GetTransportVersion() const;

    NDqProto::TData Serialize(const NUdf::TUnboxedValue& value, const NKikimr::NMiniKQL::TType* itemType) const;
    NDqProto::TData Serialize(NKikimr::NMiniKQL::TUnboxedValueVector& buffer, const NKikimr::NMiniKQL::TType* itemType) const;

    template <class TForwardIterator>
    NDqProto::TData Serialize(TForwardIterator first, TForwardIterator last, const NKikimr::NMiniKQL::TType* itemType) const {
        switch (TransportVersion) {
            case NDqProto::DATA_TRANSPORT_VERSION_UNSPECIFIED:
            case NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0:
            case NDqProto::DATA_TRANSPORT_UV_FAST_PICKLE_1_0: {
                auto count = std::distance(first, last);
                const auto listType = NKikimr::NMiniKQL::TListType::Create(
                    const_cast<NKikimr::NMiniKQL::TType*>(itemType), TypeEnv);
                const NUdf::TUnboxedValue listValue = HolderFactory.RangeAsArray(first, last);

                auto data = Serialize(listValue, listType);
                data.SetRows(count);
                return data;
            }
            case NDqProto::DATA_TRANSPORT_ARROW_1_0: {
                NKikimr::NMiniKQL::TUnboxedValueVector buffer(first, last);
                return Serialize(buffer, itemType);
            }
            default:
                YQL_ENSURE(false, "Unsupported TransportVersion");
        }
    }

    void Deserialize(const NDqProto::TData& data, const NKikimr::NMiniKQL::TType* itemType,
        NKikimr::NMiniKQL::TUnboxedValueVector& buffer) const;
    void Deserialize(const NDqProto::TData& data, const NKikimr::NMiniKQL::TType* itemType, NUdf::TUnboxedValue& value) const;

    ui64 CalcSerializedSize(NUdf::TUnboxedValue& value, const NKikimr::NMiniKQL::TType* type);

    struct TEstimateSizeSettings {
        bool WithHeaders;
        bool DiscardUnsupportedTypes;

        TEstimateSizeSettings() {
            WithHeaders = true;
            DiscardUnsupportedTypes = false;
        }
    };

    static ui64 EstimateSize(const NUdf::TUnboxedValue& value, const NKikimr::NMiniKQL::TType* type, bool* fixed = nullptr, TEstimateSizeSettings = {});

    static void DeserializeParam(const NDqProto::TData& data, const NKikimr::NMiniKQL::TType* type,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory, NKikimr::NUdf::TUnboxedValue& value);

    static NDqProto::TData SerializeParamValue(const NKikimr::NMiniKQL::TType* type, const NUdf::TUnboxedValuePod& value);

public:
    const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    const NDqProto::EDataTransportVersion TransportVersion;
};

} // namespace NYql::NDq
