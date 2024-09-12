#include <queue>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_coroutine.h>
#include <ydb/library/yql/providers/s3/compressors/factory.h>
#include <ydb/library/yql/providers/s3/events/events.h>

#if defined(_linux_) || defined(_darwin_)
#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
#endif

namespace NYql::NDq {

using namespace ::NActors;

namespace {

class TS3DecompressorCoroImpl : public TActorCoroImpl {
public:
    TS3DecompressorCoroImpl(const TActorId& parent, const TString& compression)
        : TActorCoroImpl(256_KB)
        , Compression(compression)
        , Parent(parent)
    {}

private:
    class TCoroReadBuffer : public NDB::ReadBuffer {
    public:
        TCoroReadBuffer(TS3DecompressorCoroImpl* coro)
            : NDB::ReadBuffer(nullptr, 0ULL)
            , Coro(coro)
        { }
    private:
        bool nextImpl() final {
            while (!Coro->InputFinished || !Coro->Requests.empty()) {
                Coro->ProcessOneEvent();
                if (Coro->InputBuffer) {
                    RawDataBuffer.swap(Coro->InputBuffer);
                    Coro->InputBuffer.clear();
                    auto rawData = const_cast<char*>(RawDataBuffer.data());
                    working_buffer = NDB::BufferBase::Buffer(rawData, rawData + RawDataBuffer.size());
                    return true;
                }
            }
            return false;
        }
        TS3DecompressorCoroImpl *const Coro;
        TString RawDataBuffer;
    };

    STRICT_STFUNC(StateFunc,
        hFunc(TEvS3Provider::TEvDecompressDataRequest, Handle);
        hFunc(NActors::TEvents::TEvPoison, Handle);
    )

    void Handle(TEvS3Provider::TEvDecompressDataRequest::TPtr& ev) {
        Requests.push(std::move(ev->Release()));
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& ev) {
        if (ev->Cookie) {
            ythrow yexception() << "S3 decompressor actor abort";
        }
        InputFinished = true;
    }

    void Run() final {
        try {
            std::unique_ptr<NDB::ReadBuffer> coroBuffer = std::make_unique<TCoroReadBuffer>(this);
            NDB::ReadBuffer* buffer = coroBuffer.get();
            auto decompressorBuffer = MakeDecompressor(*buffer, Compression);
            YQL_ENSURE(decompressorBuffer, "Unsupported " << Compression << " compression.");
            while (!decompressorBuffer->eof()) {
                decompressorBuffer->nextIfAtEnd();
                TString data{decompressorBuffer->available(), ' '};
                decompressorBuffer->read(&data.front(), decompressorBuffer->available());
                Send(Parent, new TEvS3Provider::TEvDecompressDataResult(std::move(data)));
            }
        } catch (const TDtorException&) {
            // Stop any activity instantly
            return;
        } catch (...) {
            Send(Parent, new TEvS3Provider::TEvDecompressDataResult(std::current_exception()));
        }
        Send(Parent, new TEvS3Provider::TEvDecompressDataFinish());
    }

    void ProcessOneEvent() {
        if (!Requests.empty()) {
            ExtractDataPart(*Requests.front());
            Requests.pop();
            return;
        }
        TAutoPtr<::NActors::IEventHandle> ev(WaitForEvent().Release());
        StateFunc(ev);
    }

    void ExtractDataPart(TEvS3Provider::TEvDecompressDataRequest& event) {
        InputBuffer = std::move(event.Data);
    }

private:
    TString InputBuffer;
    TString Compression;
    TActorId Parent;
    bool InputFinished = false;
    std::queue<THolder<TEvS3Provider::TEvDecompressDataRequest>> Requests;
};

class TS3DecompressorCoroActor : public TActorCoro {
public:
    TS3DecompressorCoroActor(THolder<TS3DecompressorCoroImpl> impl)
        : TActorCoro(THolder<TS3DecompressorCoroImpl>(impl.Release()))
    {}
private:
    void Registered(TActorSystem* actorSystem, const TActorId& parent) override {
        TActorCoro::Registered(actorSystem, parent); // Calls TActorCoro::OnRegister and sends bootstrap event to ourself.
    }
};

}

NActors::IActor* CreateS3DecompressorActor(const NActors::TActorId& parent, const TString& compression) {
    return new TS3DecompressorCoroActor(MakeHolder<TS3DecompressorCoroImpl>(parent, compression));
}

} // namespace NYql::NDq
