#pragma once

#include <util/stream/output.h>
#include <util/generic/fwd.h>

namespace NKikimr::NKqp {
    
    struct TUserRequestContext : public TAtomicRefCount<TUserRequestContext> {
        TString TraceId;
        TString Database;
        TString SessionId;

        TUserRequestContext(const TString& traceId, const TString& database, const TString& sessionId)
            : TraceId(traceId)
            , Database(database)
            , SessionId(sessionId) {}


        void Out(IOutputStream& o) const {
            o << "{" << " TraceId: " << TraceId << ", Database: " << Database << ", SessionId: " << SessionId << "}";
        }
    };
}

template<>
inline void Out<NKikimr::NKqp::TUserRequestContext>(IOutputStream& o, const NKikimr::NKqp::TUserRequestContext &x) {
    return x.Out(o);
}
