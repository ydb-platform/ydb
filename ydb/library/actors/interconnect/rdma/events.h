#pragma once

#include "rdma.h"
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>

namespace NInterconnect::NRdma {

    enum class ERdma : ui32 {
        Start = EventSpaceBegin(NActors::TEvents::ES_INTERCONNECT_RDMA),
        EvGetCqHandle = Start,
        EvRdmaIoDone,
        EvRdmaReadDone,
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

        // Error during work completion, i.e. rdma read timeout
        struct TWcErr {
            int Code;
        };

        // Error of whole CQ
        struct TCqErr {
        };

        // Post wr error
        struct TWrErr {
            int Code;
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

        static TEvRdmaIoDone* WrError(int code) {
            return new TEvRdmaIoDone(TWrErr(code));
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

        TEvRdmaIoDone(TWrErr err)
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

        bool IsWrError() const noexcept {
            return Record.index() == 3;
        }

        int GetErrCode() const noexcept {
            if (IsWcError()) {
                return std::get<TWcErr>(Record).Code;
            } else if (IsWrError()) {
                return std::get<TWrErr>(Record).Code;
            } else {
                return -1;
            }
        }

        std::string_view GetErrSource() const noexcept {
            switch (Record.index()) {
                case 0: return "Ok";
                case 1: return "TWcErr";
                case 2: return "TCqErr";
                case 3: return "TWrErr";
            }
            return "unknown";
        }

        std::variant<TSuccess, TWcErr, TCqErr, TWrErr> Record;
    };

    struct TEvRdmaReadDone : NActors::TEventLocal<TEvRdmaReadDone, (ui32)ERdma::EvRdmaReadDone> {
        std::unique_ptr<NInterconnect::NRdma::TEvRdmaIoDone> Event;
        const NActors::TMonotonic ReadScheduledTs;
        const ui16 Channel;

        TEvRdmaReadDone(
            std::unique_ptr<NInterconnect::NRdma::TEvRdmaIoDone> event,
            NActors::TMonotonic readScheduledTs,
            ui16 channel
        )
            : Event(std::move(event))
            , ReadScheduledTs(readScheduledTs)
            , Channel(channel)
        {}
    };
}
