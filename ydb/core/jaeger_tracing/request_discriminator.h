#pragma once

#include <cstddef>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr::NJaegerTracing {

enum class ERequestType: size_t {
    UNSPECIFIED,

    KEYVALUE_ACQUIRELOCK,
    KEYVALUE_EXECUTETRANSACTION,
    KEYVALUE_READ,
    KEYVALUE_READRANGE,
    KEYVALUE_LISTRANGE,
    KEYVALUE_GETSTORAGECHANNELSTATUS,

    REQUEST_TYPES_CNT, // Add new types above this line
};

static constexpr size_t kRequestTypesCnt = static_cast<size_t>(ERequestType::REQUEST_TYPES_CNT);

static const THashMap<TStringBuf, ERequestType> NameToRequestType = {
    {"KeyValue.AcquireLock", ERequestType::KEYVALUE_ACQUIRELOCK},
    {"KeyValue.ExecuteTransaction", ERequestType::KEYVALUE_EXECUTETRANSACTION},
    {"KeyValue.Read", ERequestType::KEYVALUE_READ},
    {"KeyValue.ReadRange", ERequestType::KEYVALUE_READRANGE},
    {"KeyValue.ListRange", ERequestType::KEYVALUE_LISTRANGE},
    {"KeyValue.GetStorageChannelStatus", ERequestType::KEYVALUE_GETSTORAGECHANNELSTATUS},
};

struct TRequestDiscriminator {
    ERequestType RequestType = ERequestType::UNSPECIFIED;
    TMaybe<TString> Database = NothingObject;

    static const TRequestDiscriminator EMPTY;
};

} // namespace NKikimr::NJaegerTracing
