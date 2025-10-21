#pragma once

#include <ydb/library/yql/dq/proto/dq_transport.pb.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>

namespace NYql::NDq {

inline NKikimr::NMiniKQL::EValuePackerVersion FromProto(NYql::NDqProto::EValuePackerVersion packerVersion) {
    switch (packerVersion) {
        case NDqProto::VALUE_PACKER_VERSION_UNSPECIFIED:
            return NKikimr::NMiniKQL::EValuePackerVersion::V0;
        case NDqProto::VALUE_PACKER_VERSION_V0:
            return NKikimr::NMiniKQL::EValuePackerVersion::V0;
        case NDqProto::VALUE_PACKER_VERSION_V1:
            return NKikimr::NMiniKQL::EValuePackerVersion::V1;
        default:
            Y_ENSURE(false, "This packer version is not supported for current binary.");
            break;
    }
}

inline NYql::NDqProto::EValuePackerVersion ToProto(NKikimr::NMiniKQL::EValuePackerVersion packerVersion) {
    switch (packerVersion) {
        case NKikimr::NMiniKQL::EValuePackerVersion::V0:
            return NYql::NDqProto::EValuePackerVersion::VALUE_PACKER_VERSION_V0;
        case NKikimr::NMiniKQL::EValuePackerVersion::V1:
            return NYql::NDqProto::EValuePackerVersion::VALUE_PACKER_VERSION_V1;
    }
}

} // namespace NYql::NDq
