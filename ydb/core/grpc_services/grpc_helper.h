#pragma once
#include "defs.h"
#include "grpc_mon.h"

#include <ydb/core/control/lib/immediate_control_board_impl.h>
#include <ydb/core/grpc_services/counters/counters.h>

#include <ydb/library/grpc/server/grpc_request.h>

namespace NKikimr {
namespace NGRpcService {

class TInFlightLimiterRegistry : public TThrRefBase {
private:
    TMutex Lock;
    THashMap<TString, NYdbGrpc::IGRpcRequestLimiterPtr> PerTypeLimiters;

public:
    NYdbGrpc::IGRpcRequestLimiterPtr RegisterRequestType(const TString& name, THotSwap<TControl>& icbControl, i64 limit);
};

class TCreateLimiterCB {
public:
    explicit TCreateLimiterCB(TIntrusivePtr<TInFlightLimiterRegistry> limiterRegistry)
        : LimiterRegistry(limiterRegistry)
    {}

    NYdbGrpc::IGRpcRequestLimiterPtr operator()(const TString& controlName, THotSwap<TControl>& icbControl, i64 limit) const;

private:
    TIntrusivePtr<TInFlightLimiterRegistry> LimiterRegistry;
};

inline TCreateLimiterCB CreateLimiterCb(TIntrusivePtr<TInFlightLimiterRegistry> limiterRegistry) {
    return TCreateLimiterCB(limiterRegistry);
}

template <typename TIn, typename TOut, typename TService, typename TInProtoPrinter=google::protobuf::TextFormat::Printer, typename TOutProtoPrinter=google::protobuf::TextFormat::Printer>
using TGRpcRequest = NYdbGrpc::TGRpcRequest<TIn, TOut, TService, TInProtoPrinter, TOutProtoPrinter>;

} // namespace NGRpcService
} // namespace NKikimr
