#pragma once

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

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

struct TCutOutcome {
    TVector<TReadResult> Records;
    TString Error;

    bool Ok() const {
        return Error.empty();
    }
};

struct TKeysOutcome {
    THashMap<TString, ui64> Keys;
    TString Error;

    bool Ok() const {
        return Error.empty();
    }
};

class IBatchCutter {
public:
    virtual ~IBatchCutter() = default;

    virtual TCutOutcome Cut(const TBatchCutterData& data, ui64 readStartOffset) const = 0;
    virtual TKeysOutcome GetKeys(const TBatchCutterData& data, ui64 readStartOffset) const = 0;
};

class TKafkaBatchCutter : public IBatchCutter {
public:
    TCutOutcome Cut(const TBatchCutterData& data, ui64 readStartOffset) const override final;
    TKeysOutcome GetKeys(const TBatchCutterData& data, ui64 readStartOffset) const override final;
};

} // namespace NKikimr::NPQ::NBatching
