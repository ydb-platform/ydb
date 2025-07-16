#pragma once

#include "interconnect_common.h"

#include "event_holder_pool.h"

#include <list>

namespace NInterconnect {

class IZcGuard {
public:
    virtual ~IZcGuard() = default;
    virtual void ExtractToSafeTermination(std::list<TEventHolder>& queue) noexcept = 0;
    virtual void Terminate(std::unique_ptr<NActors::TEventHolderPool>&& pool, TIntrusivePtr<NInterconnect::TStreamSocket> socket, const NActors::TActorContext &ctx) = 0;
};

class TInterconnectZcProcessor {
public:
    TInterconnectZcProcessor(bool enabled);
    ~TInterconnectZcProcessor() = default;

    // Enables ability to use ZC for given socket
    void ApplySocketOption(TStreamSocket& socket);

    // Perform send as ZC if front message is sutiable for ZC transfer.
    // Othervise send us usial number of messages to leave ZC ready for next call 
    ssize_t ProcessSend(std::span<TConstIoVec> wbuf, TStreamSocket& socket, std::span<TOutgoingStream::TBufController> ctrl);

    // Process notification queue to track actual send position
    void ProcessNotification(NInterconnect::TStreamSocket& socket) {
        if (ZcState == ZC_OK || ZcState == ZC_CONGESTED) {
            DoProcessNotification(socket);
        }
    }

    // Return guerd to start termination handling
    std::unique_ptr<IZcGuard> GetGuard();

    // Mon parts
    ui64 GetConfirmed() const { return Confirmed; }
    ui64 GetConfirmedWithCopy() const { return ConfirmedWithCopy; }
    ui64 GetZcLag() const { return SendAsZc - Confirmed; }
    TString GetCurrentState() const;
    const TString& GetErrReason() const { return LastErr; }

    // Do not try to use ZC for small events
    constexpr static ui32 ZcThreshold = 16384;
private:
    ui64 SendAsZc = 0;
    ui64 Confirmed = 0;
    ui64 ConfirmedWithCopy = 0;

    TString LastErr;

    enum {
        ZC_DISABLED,            // ZeroCopy featute is disabled by used
        ZC_DISABLED_ERR,        // We got some errors and unable to use ZC for this connection
        ZC_DISABLED_HIDDEN_COPY, // The socket associated with loopback, or unsupported nic
                                // real ZC send is not possible in this case and cause hiden copy inside kernel.
        ZC_OK,                  // OK, data can be send using zero copy
        ZC_CONGESTED,           // We got ENUBUF and temporary disable ZC send

    } ZcState;

    bool ZcStateIsOk() { return ZcState == ZC_OK; }
    void DoProcessNotification(NInterconnect::TStreamSocket& socket);
    void ResetState();
};

}