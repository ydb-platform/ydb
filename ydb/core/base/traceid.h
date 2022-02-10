#pragma once
#include "defs.h"
#include <ydb/core/protos/tracing.pb.h>

namespace NKikimr {
namespace NTracing {

struct TTraceID {
    ui64 CreationTime;
    ui64 RandomID;

    TTraceID();
    TTraceID(ui64 randomId, ui64 creationTime);

    static TTraceID GenerateNew();
    TString ToString() const;
    void Out(IOutputStream &o) const;

    bool operator<(const TTraceID &x) const;
    bool operator>(const TTraceID &x) const;
    bool operator<=(const TTraceID &x) const;
    bool operator>=(const TTraceID &x) const;
    bool operator==(const TTraceID &x) const;
    bool operator!=(const TTraceID &x) const;
};

void TraceIDFromTraceID(const TTraceID& src, NKikimrTracing::TTraceID* dest);
TTraceID TraceIDFromTraceID(const NKikimrTracing::TTraceID &proto);

}
}

template<>
struct THash<NKikimr::NTracing::TTraceID> {
    inline ui64 operator()(const NKikimr::NTracing::TTraceID& x) const noexcept {
        return x.RandomID;
    }
};

template<>
inline void Out<NKikimr::NTracing::TTraceID>(IOutputStream& o, const NKikimr::NTracing::TTraceID &x) {
    return x.Out(o);
}
