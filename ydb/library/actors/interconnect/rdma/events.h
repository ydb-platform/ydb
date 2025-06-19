#pragma once

#include "ctx.h"
#include "rdma.h"
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>

namespace NInterconnect::NRdma {

    enum class ERdma : ui32 {
        Start = EventSpaceBegin(NActors::TEvents::ES_INTERCONNECT_RDMA),
        EvGetCqHandle = Start,
        EvRdmaIoDone,
    };

    struct TEvGetCqHandle: public NActors::TEventLocal<TEvGetCqHandle, ui32(ERdma::EvGetCqHandle)> {
        TEvGetCqHandle(const TRdmaCtx* ctx)
            : Ctx(ctx)
        {}
        const TRdmaCtx* const Ctx;
        ICq::TPtr CqPtr;
    };

    struct TEvRdmaIoDone : public NActors::TEventLocal<TEvRdmaIoDone, (ui32)ERdma::EvRdmaIoDone> {
        struct TSuccess {
        };

        struct TWcErr {
            int Code;
        };

        struct TCqErr {
        };

        static TEvRdmaIoDone* Success() {
            return new TEvRdmaIoDone();
        }

        static TEvRdmaIoDone* WcError(int code) {
            return new TEvRdmaIoDone(code); 
        }

        static TEvRdmaIoDone* CqError() {
            return new TEvRdmaIoDone(TCqErr());
        } 

        TEvRdmaIoDone()
            : Record(TSuccess())
        {}

        TEvRdmaIoDone(int errCode)
            : Record(TWcErr {
                .Code = errCode,
            })
        {}

        TEvRdmaIoDone(TCqErr err)
            : Record(err)
        {}

        bool IsSuccess() const noexcept {
            return Record.index() == 0; 
        }

        bool IsWcError() const noexcept {
            return Record.index() == 1; 
        }

        bool IsCqError() const noexcept {
            return Record.index() == 2;
        }
        
        int GetErrCode() const noexcept {
            if (IsWcError()) {
                return std::get<1>(Record).Code;
            } else {
                return -1;
            }
        }

        std::variant<TSuccess, TWcErr, TCqErr> Record;
    };
}
