#pragma once

#include <ydb/library/yql/dq/proto/dq_transport.pb.h>
#include <ydb/library/yql/minikql/mkql_buffer.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/actors/util/rope.h>

#include <util/generic/buffer.h>

namespace NYql::NDq {

inline bool IsOOBTransport(NDqProto::EDataTransportVersion version) {
    return version == NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0 ||
           version == NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_PICKLE_1_0;
}

struct TDqSerializedBatch {
    NDqProto::TData Proto;
    TRope Payload;

    size_t Size() const {
        return Proto.GetRaw().size() + Payload.size();
    }

    ui32 RowCount() const {
        return Proto.GetRows();
    }

    void Clear() {
        Payload.clear();
        Proto.Clear();
    }

    bool IsOOB() const {
        const bool oob = IsOOBTransport((NDqProto::EDataTransportVersion)Proto.GetTransportVersion());
        YQL_ENSURE(oob || Payload.IsEmpty());
        return oob;
    }

    void SetPayload(TRope&& payload);

    TRope PullPayload() {
        TRope result;
        if (IsOOB()) {
            result = std::move(Payload);
        } else {
            result = TRope(std::move(*Proto.MutableRaw()));
        }
        Clear();
        return result;
    }

    void ConvertToNoOOB();
};

TRope SaveForSpilling(TDqSerializedBatch&& batch);
TDqSerializedBatch LoadSpilled(TBuffer&& blob);

}
