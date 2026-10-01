#pragma once

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <expected>

namespace NKikimr::NPQ::NBatching {

using TReadResult = NKikimrClient::TCmdReadResult::TResult;

struct TBatchCutterData {
    NKikimrPQClient::TDataChunk DataChunk;
    const TReadResult& ReadResult;

    TBatchCutterData(const TReadResult& readResult, NKikimrPQClient::TDataChunk&& dataChunk)
        : ReadResult(readResult)
    {
        DataChunk.Swap(&dataChunk);
    }

    TBatchCutterData(TReadResult&&, NKikimrPQClient::TDataChunk&&) = delete;
};

class IBatchCutter {
public:
    virtual ~IBatchCutter() = default;

    virtual std::expected<TVector<TReadResult>, TString> Cut(const TBatchCutterData& data, ui64 readStartOffset) const = 0;
    virtual std::expected<THashMap<TString, ui64>, TString> GetKeys(const TBatchCutterData& data, ui64 readStartOffset) const = 0;
};

class TKafkaBatchCutter : public IBatchCutter {
public:
    std::expected<TVector<TReadResult>, TString> Cut(const TBatchCutterData& data, ui64 readStartOffset) const override final;
    std::expected<THashMap<TString, ui64>, TString> GetKeys(const TBatchCutterData& data, ui64 readStartOffset) const override final;
};

} // namespace NKikimr::NPQ::NBatching
