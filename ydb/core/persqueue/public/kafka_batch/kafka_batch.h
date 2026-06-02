#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <optional>

namespace NKikimr::NPQ::NKafkaBatch {

struct TKafkaHeader {
    std::optional<TString> Key;
    std::optional<TString> Value;
};

struct TKafkaBatchRecord {
    ui64 Offset = 0;
    ui64 Sequence = 0;
    i64 Timestamp = 0;
    i8 Attributes = 0;
    std::optional<TString> Key;
    std::optional<TString> Value;
    TVector<TKafkaHeader> Headers;
};

struct TKafkaBatch {
    ui64 BaseOffset = 0;
    i32 LastOffsetDelta = 0;
    i64 BaseTimestamp = 0;
    i64 MaxTimestamp = 0;
    i64 ProducerId = -1;
    i16 ProducerEpoch = -1;
    i32 BaseSequence = -1;
    i16 Attributes = 0;
    TVector<TKafkaBatchRecord> Records;
};

TKafkaBatch ParseKafkaBatch(TStringBuf data);
TVector<TKafkaBatchRecord> ParseKafkaBatchRecords(TStringBuf data);

} // namespace NKikimr::NPQ::NKafkaBatch
