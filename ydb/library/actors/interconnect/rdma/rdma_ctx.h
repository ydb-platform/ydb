#pragma once

#include <util/generic/noncopyable.h>

extern "C" {

struct ibv_context;
struct ibv_pd;

}

namespace NInterconnect::NRdma {

class TRdmaCtx : public NNonCopyable::TNonCopyable {
public:
    TRdmaCtx(ibv_context* ctx, ibv_pd* pd)
        : Context(ctx)
        , ProtDomain(pd)
    {}

    ~TRdmaCtx();

    ibv_context* GetContext() const {
        return Context;
    }

    ibv_pd* GetProtDomain() const {
        return ProtDomain;
    }
private:
    ibv_context* const Context;
    ibv_pd* const ProtDomain;
};


}
