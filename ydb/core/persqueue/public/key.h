#pragma once

#include <util/generic/hash.h>


namespace NKikimr::NPQ {

struct TReadSessionKey {
    TString SessionId;
    ui64 PartitionSessionId = 0;
    bool operator ==(const TReadSessionKey& rhs) const {
        return SessionId == rhs.SessionId && PartitionSessionId == rhs.PartitionSessionId;
    }
};

struct TDirectReadKey {
    TString SessionId;
    ui64 PartitionSessionId = 0;
    ui64 ReadId = 0;
    bool operator ==(const TDirectReadKey& rhs) const {
        return SessionId == rhs.SessionId && PartitionSessionId == rhs.PartitionSessionId && ReadId == rhs.ReadId;
    }
};

}


template <>
struct THash<NKikimr::NPQ::TReadSessionKey> {
public:
    inline size_t operator()(const NKikimr::NPQ::TReadSessionKey& key) const {
        size_t res = 0;
        res += THash<TString>()(key.SessionId);
        res += THash<ui64>()(key.PartitionSessionId);
        return res;
    }
};
