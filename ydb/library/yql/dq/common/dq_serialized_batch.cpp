#include "dq_serialized_batch.h"

#include <ydb/library/yql/utils/rope_over_buffer.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/system/unaligned_mem.h>

namespace NYql::NDq {

namespace {

template<typename T>
void AppendNumber(TRope& rope, T data) {
    static_assert(std::is_integral_v<T>);
    rope.Insert(rope.End(), TRope(TString(reinterpret_cast<const char*>(&data), sizeof(T))));
}

template<typename T>
T ReadNumber(TStringBuf& src) {
    static_assert(std::is_integral_v<T>);
    YQL_ENSURE(src.size() >= sizeof(T), "Premature end of spilled data");
    T result = ReadUnaligned<T>(src.data());
    src.Skip(sizeof(T));
    return result;
}

}

void TDqSerializedBatch::SetPayload(TRope&& payload) {
    Proto.ClearRaw();
    if (IsOOBTransport((NDqProto::EDataTransportVersion)Proto.GetTransportVersion())) {
        Payload = std::move(payload);
    } else {
        Payload.clear();
        Proto.MutableRaw()->reserve(payload.size());
        while (!payload.IsEmpty()) {
            auto it = payload.Begin();
            Proto.MutableRaw()->append(it.ContiguousData(), it.ContiguousSize());
            payload.Erase(it, it + it.ContiguousSize());
        }
    }
}

void TDqSerializedBatch::ConvertToNoOOB() {
    if (!IsOOB()) {
        return;
    }

    YQL_ENSURE(Proto.GetRaw().empty());
    Proto.SetRaw(Payload.ConvertToString());
    Payload.clear();
    switch ((NDqProto::EDataTransportVersion)Proto.GetTransportVersion()) {
    case NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_FAST_PICKLE_1_0:
        Proto.SetTransportVersion(NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_FAST_PICKLE_1_0);
        break;
    case NDqProto::EDataTransportVersion::DATA_TRANSPORT_OOB_PICKLE_1_0:
        Proto.SetTransportVersion(NDqProto::EDataTransportVersion::DATA_TRANSPORT_UV_PICKLE_1_0);
        break;
    default:
        YQL_ENSURE(false, "Unexpected transport version" << Proto.GetTransportVersion());
    }
}

TRope SaveForSpilling(TDqSerializedBatch&& batch) {
    TRope result;

    ui32 transportversion = batch.Proto.GetTransportVersion();
    ui32 rowCount = batch.Proto.GetRows();

    TRope protoPayload(std::move(*batch.Proto.MutableRaw()));

    AppendNumber(result, transportversion);
    AppendNumber(result, rowCount);
    AppendNumber(result, protoPayload.size());
    result.Insert(result.End(), std::move(protoPayload));
    AppendNumber(result, batch.Payload.GetSize());
    result.Insert(result.End(), std::move(batch.Payload));

    return result;
}

TDqSerializedBatch LoadSpilled(TBuffer&& blob) {
    auto sharedBuf = std::make_shared<TBuffer>(std::move(blob));

    TStringBuf source(sharedBuf->Data(), sharedBuf->Size());
    TDqSerializedBatch result;
    result.Proto.SetTransportVersion(ReadNumber<ui32>(source));
    result.Proto.SetRows(ReadNumber<ui32>(source));

    size_t protoSize = ReadNumber<size_t>(source);
    YQL_ENSURE(source.size() >= protoSize, "Premature end of spilled data");

    result.Proto.SetRaw(source.data(), protoSize);
    source.Skip(protoSize);

    size_t ropeSize = ReadNumber<size_t>(source);
    YQL_ENSURE(ropeSize == source.size(), "Spilled data is corrupted");
    result.Payload = MakeReadOnlyRope(sharedBuf, source.data(), source.size());
    return result;
}

}
