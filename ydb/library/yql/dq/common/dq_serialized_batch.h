#pragma once

#include <ydb/library/yql/dq/proto/dq_transport.pb.h>
#include <yql/essentials/minikql/mkql_buffer.h>
#include <yql/essentials/utils/chunked_buffer.h>
#include <yql/essentials/utils/yql_panic.h>

#include <ydb/library/actors/util/rope.h>

#include <util/generic/buffer.h>

namespace NYql::NDq {

inline bool IsOOBTransport(NDqProto::EDataTransportVersion version) {
    return version == NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0 ||
           version == NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_PICKLE_1_0;
}

struct TDqSerializedBatch {
    NDqProto::TData Proto;
    TChunkedBuffer Payload;

    size_t Size() const {
        return Proto.GetRaw().size() + Payload.Size();
    }

    ui32 ChunkCount() const {
        return Proto.GetChunks();
    }

    ui32 RowCount() const {
        return Proto.GetRows() ? Proto.GetRows() : Proto.GetChunks();
    }

    void Clear() {
        Payload.Clear();
        Proto.Clear();
    }

    bool IsOOB() const {
        const bool oob = IsOOBTransport((NDqProto::EDataTransportVersion)Proto.GetTransportVersion());
        YQL_ENSURE(oob || Payload.Empty());
        return oob;
    }

    void SetPayload(TChunkedBuffer&& payload);

    TChunkedBuffer PullPayload() {
        TChunkedBuffer result;
        if (IsOOB()) {
            result = std::move(Payload);
        } else {
            result = TChunkedBuffer(std::move(*Proto.MutableRaw()));
        }
        Clear();
        return result;
    }

    void ConvertToNoOOB();
};

TChunkedBuffer SaveForSpilling(TDqSerializedBatch&& batch);
TDqSerializedBatch LoadSpilled(TBuffer&& blob);

}
