#include "msgbus_player.h"
#include <util/system/sem.h>
#include <util/system/fstat.h>
#include <util/stream/file.h>
#include <util/random/random.h>
#include <util/string/printf.h>
#include <util/system/condvar.h>

namespace NKikimr {
namespace NMessageBusTracer {

class TDarwinFastSemaphore : public TSemaphore {
public:
    TDarwinFastSemaphore(ui32 maxCount)
        : TSemaphore(GenerateUniqueName().c_str(), maxCount)
    {}

protected:
    static TString GenerateUniqueName() {
        ui64 number = TInstant::Now().MicroSeconds();
        return Sprintf("darwin-sem-%" PRIu64, number);
    }
};

#if defined(_darwin_)
typedef TDarwinFastSemaphore TUniversalFastSemaphore;
#else
typedef TFastSemaphore TUniversalFastSemaphore;
#endif

class TAutoSemaphore : public TUniversalFastSemaphore {
protected:
    ui32 MaxCount;
public:
    TAutoSemaphore(ui32 count) : TUniversalFastSemaphore(count), MaxCount(count) {}
    ~TAutoSemaphore() {
        // waiting for async pending work to be done
        for( ui32 i = 0; i < MaxCount; ++i)
            Acquire();
    }
};

template <typename TType> class TInFlightMonitor {
public:
    TInFlightMonitor(ui32 maxInFlight)
        : MaxInFlight(maxInFlight)
        , ComputedMaxInFlight(0)
    {
        Y_ABORT_UNLESS(maxInFlight > 0);
    }

    void Start(TType item) {
        MaxInFlight.Acquire();
        auto guard = Guard(InFlightLock);
        Y_UNUSED(guard);
        InFlight.insert(item);
        ComputedMaxInFlight = std::max(ComputedMaxInFlight, static_cast<ui32>(InFlight.size()));
    }

    void Finish(TType item) {
        {
            auto guard = Guard(InFlightLock);
            Y_UNUSED(guard);
            InFlight.erase(item);
            InFlightFinished.Signal();
        }
        MaxInFlight.Release();
    }

    bool WaitForFinish(TType item) {
        auto guard = Guard(InFlightLock);
        Y_UNUSED(guard);
        while(InFlight.count(item) != 0) {
            InFlightFinished.WaitI(InFlightLock);
        }
        return true;
    }

    ui32 GetComputedMaxInFlight() const {
        return ComputedMaxInFlight;
    }

protected:
    THashSet<TType> InFlight;
    TMutex InFlightLock;
    TCondVar InFlightFinished;
    TAutoSemaphore MaxInFlight;
    ui32 ComputedMaxInFlight;
};

TMsgBusPlayer::TMsgBusPlayer(TMsgBusPlayer::TMsgBusClient &msgBusClient)
    : MsgBusClient(msgBusClient)
{}

TString DumpMessageHeader(const NBus::TBusHeader& messageHeader) {
    return Sprintf("Message Id=%lx,Time=%s,Size=%d,Type=%x", messageHeader.Id, TInstant::MicroSeconds(messageHeader.SendTime).ToString().c_str(), messageHeader.Size, messageHeader.Type);
}

ui32 TMsgBusPlayer::PlayTrace(const TString &traceFile, ui32 maxInFlight, std::function<void(int)> progressReporter) {
    using NBus::TBusKey;
    TAutoPtr<IInputStream> stream = new TUnbufferedFileInput(traceFile);
    i64 fileLength = GetFileLength(traceFile);
    i64 filePos = 0;
    int lastPercent = -1;
    NMsgBusProxy::TProtocol protocol(NMsgBusProxy::TProtocol::DefaultPort);
    TInFlightMonitor<TBusKey> monitor(maxInFlight);
    TBuffer messageBuffer;
    for(;;) {
        TBusKey replyKey;
        size_t read = stream->Load(&replyKey, sizeof(replyKey));
        if (read != sizeof(replyKey)) {
            if (read != 0)
                ythrow yexception() << "Error reading message replay id";
            break;  // looks like end of the file
        }
        filePos += read;
        messageBuffer.Resize(sizeof(NBus::TBusHeader));
        read = stream->Load(messageBuffer.Data(), sizeof(NBus::TBusHeader));
        if (read != sizeof(NBus::TBusHeader))
            ythrow yexception() << "Error reading message header";
        filePos += read;
        NBus::TBusHeader messageHeader(::TArrayRef<char>(messageBuffer.Data(), messageBuffer.Size()));
        messageBuffer.Resize(messageHeader.Size - sizeof(NBus::TBusHeader));
        if (!messageBuffer.Empty()) {
            read = stream->Load(messageBuffer.Data(), messageBuffer.Size());
            if (read != messageBuffer.Size())
                ythrow yexception() << "Error reading message content";
            filePos += read;
        }
        if (replyKey == YBUS_KEYINVALID) {
            TAutoPtr<NBus::TBusMessage> request = protocol.Deserialize(messageHeader.Type, ::TArrayRef<char>(messageBuffer.Data(), messageBuffer.Size()));
            if (request == nullptr)
                ythrow yexception() << "Error deserializing message (" << DumpMessageHeader(messageHeader) << ")";
            TBusKey requestKey = messageHeader.Id;
            monitor.Start(requestKey);
            NBus::EMessageStatus status = MsgBusClient.AsyncCall(request, [&,requestKey](NBus::EMessageStatus, TAutoPtr<NBus::TBusMessage>){ monitor.Finish(requestKey); });
            if (status != NBus::MESSAGE_OK) {
                monitor.Finish(requestKey);
                ythrow yexception() << NBus::MessageStatusDescription(status) << " (" << DumpMessageHeader(messageHeader) << ")";
            }
        } else {
            monitor.WaitForFinish(replyKey);
        }
        if (progressReporter) {
            int currentPercent = filePos * 100 / fileLength;
            if (currentPercent != lastPercent) {
                progressReporter(currentPercent);
                lastPercent = currentPercent;
            }
        }
    }
    return monitor.GetComputedMaxInFlight();
}



}
}
