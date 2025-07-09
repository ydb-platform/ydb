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

    // Perform ZC send if front message is suitable for ZC transfer.
    // Otherwise, send usual number of messages to prepare for ZC send for next call
    ssize_t ProcessSend(std::span<TConstIoVec> wbuf, TStreamSocket& socket, std::span<TOutgoingStream::TBufController> ctrl);

    // Process notification queue to track actual send position
    void ProcessNotification(NInterconnect::TStreamSocket& socket) {
        if (SendAsZc > Confirmed && (ZcState == ZC_OK || ZcState == ZC_CONGESTED || ZcState == ZC_DISABLED_HIDDEN_COPY)) {
            DoProcessNotification(socket);
        }
    }

    // Return guerd to start termination handling
    std::unique_ptr<IZcGuard> GetGuard();

    // Mon parts
    ui64 GetConfirmed() const { return Confirmed; }
    ui64 GetConfirmedWithCopy() const { return ConfirmedWithCopy; }
    ui64 GetZcLag() const { return SendAsZc - Confirmed; }
    ui64 GetSendAsZcBytes() const { return SendAsZcBytes; }
    TString GetCurrentStateName() const;
    const TString& GetErrReason() const { return LastErr; }

    bool ZcStateIsOk() const { return ZcState == ZC_OK; }
    bool ZcStateIsDisabled() const { return ZcState == ZC_DISABLED; }
    TString ExtractErrText();

    // Do not try to use ZC for small events
    constexpr static ui32 ZcThreshold = 16384;
private:
    ui64 SendAsZcBytes = 0;
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

    void DoProcessNotification(NInterconnect::TStreamSocket& socket);
    void ResetState();
    void AddErr(const TString& err);
};

}
