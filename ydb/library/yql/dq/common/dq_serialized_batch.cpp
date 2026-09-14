#include "dq_serialized_batch.h"

#include <yql/essentials/utils/yql_panic.h>

#include <util/system/unaligned_mem.h>

namespace NYql::NDq {

namespace {

template<typename T>
void AppendNumber(TChunkedBuffer& rope, T data) {
    static_assert(std::is_integral_v<T>);
    rope.Append(TString(reinterpret_cast<const char*>(&data), sizeof(T)));
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

void TDqSerializedBatch::SetPayload(TChunkedBuffer&& payload) {
    Proto.ClearRaw();
    if (IsOOBTransport((NDqProto::EDataTransportVersion)Proto.GetTransportVersion())) {
        Payload = std::move(payload);
    } else {
        Payload.Clear();
        Proto.MutableRaw()->reserve(payload.Size());
        TStringOutput sout(*Proto.MutableRaw());
        payload.CopyTo(sout);
        payload.Clear();
    }
}

void TDqSerializedBatch::ConvertToNoOOB() {
    if (!IsOOB()) {
        return;
    }

    YQL_ENSURE(Proto.GetRaw().empty());
    Proto.MutableRaw()->reserve(Payload.Size());
    TStringOutput sout(*Proto.MutableRaw());
    Payload.CopyTo(sout);
    Payload.Clear();
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

TChunkedBuffer SaveForSpilling(TDqSerializedBatch&& batch) {
    TChunkedBuffer result;

    ui32 transportVersion = batch.Proto.GetTransportVersion();
    ui32 chunkCount = batch.Proto.GetChunks();
    ui32 rowCount = batch.Proto.GetRows();

    TChunkedBuffer protoPayload(std::move(*batch.Proto.MutableRaw()));

    AppendNumber(result, transportVersion);
    AppendNumber(result, chunkCount);
    AppendNumber(result, rowCount);
    AppendNumber(result, protoPayload.Size());
    result.Append(std::move(protoPayload));
    AppendNumber(result, batch.Payload.Size());
    result.Append(std::move(batch.Payload));

    return result;
}

TDqSerializedBatch LoadSpilled(TBuffer&& blob) {
    auto sharedBuf = std::make_shared<TBuffer>(std::move(blob));

    TStringBuf source(sharedBuf->Data(), sharedBuf->Size());
    TDqSerializedBatch result;
    result.Proto.SetTransportVersion(ReadNumber<ui32>(source));
    result.Proto.SetChunks(ReadNumber<ui32>(source));
    result.Proto.SetRows(ReadNumber<ui32>(source));

    size_t protoSize = ReadNumber<size_t>(source);
    YQL_ENSURE(source.size() >= protoSize, "Premature end of spilled data");

    result.Proto.SetRaw(source.data(), protoSize);
    source.Skip(protoSize);

    size_t ropeSize = ReadNumber<size_t>(source);
    YQL_ENSURE(ropeSize == source.size(), "Spilled data is corrupted");
    result.Payload = TChunkedBuffer(source, sharedBuf);
    return result;
}

}
