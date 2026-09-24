#pragma once

#include "rdma.h"
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/util/rc_buf.h>

#include <memory>
#include <string_view>
#include <utility>
#include <variant>

namespace NInterconnect::NRdma {

    enum class ERdma : ui32 {
        Start = EventSpaceBegin(NActors::TEvents::ES_INTERCONNECT_RDMA),
        EvGetCqHandle = Start,
        EvRdmaIoDone,
        EvRdmaReadDone,
        EvRdmaIoReceiveDone,
    };

    struct TEvGetCqHandle: public NActors::TEventLocal<TEvGetCqHandle, ui32(ERdma::EvGetCqHandle)> {
        TEvGetCqHandle(const TRdmaCtx* ctx)
            : Ctx(ctx)
        {}
        const TRdmaCtx* const Ctx;
        ICq::TPtr CqPtr;
    };

    struct TSuccess {
    };

    struct TSuccessReceive {
        TRcBuf Buf;
    };

    template <typename TDerived, typename TSuccessRecord, ERdma EventId>
    struct TEvRdmaIoDoneCommon : public NActors::TEventLocal<TDerived, (ui32)EventId> {
        using TSuccess = TSuccessRecord;

        // Error during work completion, i.e. rdma read timeout
        struct TWcErr {
            int Code;
        };

        // Error of whole CQ
        struct TCqErr {
        };

        // Post WR error
        struct TWrErr {
            int Code;
        };

        static TDerived* Success(TSuccess success) {
            return new TDerived(std::move(success));
        }

        template <typename... TArgs>
        static TDerived* Success(TArgs&&... args) {
            return new TDerived(TSuccess{std::forward<TArgs>(args)...});
        }

        static TDerived* WcError(int code) {
            return new TDerived(TWcErr{code});
        }

        static TDerived* CqError() {
            return new TDerived(TCqErr());
        }

        static TDerived* WrError(int code) {
            return new TDerived(TWrErr{code});
        }

        TEvRdmaIoDoneCommon()
            : Record(TSuccess())
        {}

        TEvRdmaIoDoneCommon(TSuccess success)
            : Record(std::move(success))
        {}

        TEvRdmaIoDoneCommon(int errCode)
            : Record(TWcErr {
                .Code = errCode,
            })
        {}

        TEvRdmaIoDoneCommon(TWcErr err)
            : Record(err)
        {}

        TEvRdmaIoDoneCommon(TCqErr err)
            : Record(err)
        {}

        TEvRdmaIoDoneCommon(TWrErr err)
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

    struct TEvRdmaIoDone : public TEvRdmaIoDoneCommon<TEvRdmaIoDone, TSuccess, ERdma::EvRdmaIoDone> {
        using TBase = TEvRdmaIoDoneCommon<TEvRdmaIoDone, TSuccess, ERdma::EvRdmaIoDone>;
        using TBase::TBase;
    };

    struct TEvRdmaIoReceiveDone : public TEvRdmaIoDoneCommon<TEvRdmaIoReceiveDone, TSuccessReceive, ERdma::EvRdmaIoReceiveDone> {
        using TBase = TEvRdmaIoDoneCommon<TEvRdmaIoReceiveDone, TSuccessReceive, ERdma::EvRdmaIoReceiveDone>;
        using TBase::TBase;
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
