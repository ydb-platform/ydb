#pragma once

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>

namespace NKikimr::NPQ::NBatching {

using TReadResult = NKikimrClient::TCmdReadResult::TResult;

struct TBatchCutterData {
    NKikimrPQClient::TDataChunk DataChunk;
    const TReadResult& ReadResult;

    TBatchCutterData(const TReadResult& readResult, NKikimrPQClient::TDataChunk&& dataChunk) : DataChunk(dataChunk), ReadResult(readResult) {}
};

class IBatchCutter {
public:
    virtual ~IBatchCutter() = default;

    virtual TVector<TReadResult> Cut(const TBatchCutterData& data, ui64 readStartOffset) const = 0;
};

class TKafkaBatchCutter : public IBatchCutter {
public:
    TKafkaBatchCutter() = default;
    ~TKafkaBatchCutter() = default;

    TVector<TReadResult> Cut(const TBatchCutterData& data, ui64 readStartOffset) const override final;
};

} // namespace NKikimr::NPQ::NBatching
