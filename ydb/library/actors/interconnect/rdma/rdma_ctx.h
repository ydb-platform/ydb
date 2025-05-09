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
private:
    ibv_context* Context;
    ibv_pd* ProtDomain;
};


}
