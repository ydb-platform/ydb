#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/events.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <memory>

namespace {

using namespace NActors;

constexpr size_t MaxHandles = 16;
constexpr size_t MaxOperations = 256;
constexpr size_t MaxBlobSize = 96;

TActorId MakeActorId(ui32 node, ui64 local) {
    return TActorId(node + 1, 1, local + 1, 1);
}

TString ConsumeBlob(FuzzedDataProvider& fdp) {
    const size_t maxSize = Min(MaxBlobSize, fdp.remaining_bytes());
    const size_t size = fdp.ConsumeIntegralInRange<size_t>(0, maxSize);
    std::string bytes = fdp.ConsumeBytesAsString(size);
    return TString(bytes.data(), bytes.size());
}

ui32 ConsumeFlags(FuzzedDataProvider& fdp) {
    ui32 flags = 0;
    if (fdp.ConsumeBool()) {
        flags |= IEventHandle::FlagTrackDelivery;
    }
    if (fdp.ConsumeBool()) {
        flags |= IEventHandle::FlagForwardOnNondelivery;
    }
    if (fdp.ConsumeBool()) {
        flags |= IEventHandle::FlagSubscribeOnSession;
    }
    if (fdp.ConsumeBool()) {
        flags |= IEventHandle::FlagGenerateUnsureUndelivered;
    }
    if (fdp.ConsumeBool()) {
        flags |= IEventHandle::FlagExtendedFormat;
    }
    return IEventHandle::MakeFlags(fdp.ConsumeIntegralInRange<ui32>(0, 7), flags);
}

std::unique_ptr<IEventHandle> MakeLocalHandle(FuzzedDataProvider& fdp, ui64 cookie) {
    const TActorId recipient = MakeActorId(fdp.ConsumeIntegralInRange<ui32>(0, 3), fdp.ConsumeIntegralInRange<ui64>(0, 256));
    const TActorId sender = MakeActorId(fdp.ConsumeIntegralInRange<ui32>(0, 3), fdp.ConsumeIntegralInRange<ui64>(257, 512));
    TMaybe<TActorId> forwardTo;
    if (fdp.ConsumeBool()) {
        forwardTo = MakeActorId(fdp.ConsumeIntegralInRange<ui32>(0, 3), fdp.ConsumeIntegralInRange<ui64>(513, 768));
    }
    return std::make_unique<IEventHandle>(
        recipient,
        sender,
        new TEvents::TEvBlob(ConsumeBlob(fdp)),
        ConsumeFlags(fdp),
        cookie,
        forwardTo ? &*forwardTo : nullptr);
}

std::unique_ptr<IEventHandle> MakeBufferedHandle(FuzzedDataProvider& fdp, ui64 cookie) {
    const TString blob = ConsumeBlob(fdp);
    TEventSerializationInfo info;
    if (fdp.ConsumeBool()) {
        info.IsExtendedFormat = fdp.ConsumeBool();
        size_t left = blob.size();
        while (left && info.Sections.size() < 4 && fdp.ConsumeBool()) {
            const size_t size = fdp.ConsumeIntegralInRange<size_t>(1, left);
            info.Sections.push_back(TEventSectionInfo{
                .Headroom = fdp.ConsumeIntegralInRange<size_t>(0, 8),
                .Size = size,
                .Tailroom = fdp.ConsumeIntegralInRange<size_t>(0, 8),
                .Alignment = fdp.ConsumeIntegralInRange<size_t>(0, 16),
                .IsInline = fdp.ConsumeBool(),
                .IsRdmaCapable = fdp.ConsumeBool(),
            });
            left -= size;
        }
        if (left) {
            info.Sections.push_back(TEventSectionInfo{.Size = left, .IsInline = true});
        }
    }

    TMaybe<TActorId> forwardTo;
    if (fdp.ConsumeBool()) {
        forwardTo = MakeActorId(fdp.ConsumeIntegralInRange<ui32>(0, 3), fdp.ConsumeIntegralInRange<ui64>(769, 1024));
    }

    return std::make_unique<IEventHandle>(
        TEvents::TEvBlob::EventType,
        ConsumeFlags(fdp),
        MakeActorId(fdp.ConsumeIntegralInRange<ui32>(0, 3), fdp.ConsumeIntegralInRange<ui64>(0, 256)),
        MakeActorId(fdp.ConsumeIntegralInRange<ui32>(0, 3), fdp.ConsumeIntegralInRange<ui64>(257, 512)),
        MakeIntrusive<TEventSerializedData>(blob, std::move(info)),
        cookie,
        forwardTo ? &*forwardTo : nullptr);
}

std::unique_ptr<IEventHandle> MakeHandle(FuzzedDataProvider& fdp, ui64 cookie) {
    return fdp.ConsumeBool() ? MakeLocalHandle(fdp, cookie) : MakeBufferedHandle(fdp, cookie);
}

template <typename TEv>
TEv* GetLocalEventByActualType(IEventHandle& handle) {
    if (!handle.HasEvent()) {
        return nullptr;
    }
    IEventBase* event = handle.GetBase();
    return event && event->Type() == TEv::EventType ? static_cast<TEv*>(event) : nullptr;
}

bool IsUnsureUndelivered(IEventHandle& handle) {
    auto* undelivered = GetLocalEventByActualType<TEvents::TEvUndelivered>(handle);
    return undelivered && undelivered->Unsure;
}

void CheckHandle(IEventHandle& handle) {
    Y_ABORT_UNLESS(handle.GetTypeRewrite() == TEvents::TEvBlob::EventType ||
        handle.GetTypeRewrite() == TEvents::TEvUndelivered::EventType);
    Y_ABORT_UNLESS(handle.GetRecipientRewrite() == handle.Recipient ||
        handle.GetRecipientRewrite().NodeId() || !handle.GetRecipientRewrite());
    Y_ABORT_UNLESS(handle.GetChannel() < (1u << IEventHandle::ChannelBits));

    if (handle.HasEvent()) {
        Y_ABORT_UNLESS(handle.GetBase());
        handle.ToString();
        handle.GetTypeName();
    }
    handle.GetSize();
}

void RunEventHandleFuzz(FuzzedDataProvider& fdp) {
    TVector<std::unique_ptr<IEventHandle>> handles;
    ui64 nextCookie = 1;

    for (size_t step = 0; step < MaxOperations && fdp.remaining_bytes(); ++step) {
        if (handles.empty() || (handles.size() < MaxHandles && fdp.ConsumeIntegralInRange<unsigned>(0, 3) == 0)) {
            handles.push_back(MakeHandle(fdp, nextCookie++));
            CheckHandle(*handles.back());
            continue;
        }

        const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, handles.size() - 1);
        auto& handle = handles[index];
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 9)) {
            case 0:
                handle->Rewrite(
                    fdp.ConsumeBool() ? TEvents::TEvBlob::EventType : TEvents::TEvUndelivered::EventType,
                    MakeActorId(fdp.ConsumeIntegralInRange<ui32>(0, 3), fdp.ConsumeIntegralInRange<ui64>(1025, 2048)));
                break;

            case 1:
                handle->DropRewrite();
                break;

            case 2:
                handles.push_back(IEventHandle::Forward(std::move(handle), MakeActorId(0, fdp.ConsumeIntegralInRange<ui64>(2049, 4096))));
                handles.erase(handles.begin() + index);
                break;

            case 3: {
                auto forwarded = IEventHandle::ForwardOnNondelivery(
                    std::move(handle),
                    fdp.ConsumeBool() ? TEvents::TEvUndelivered::ReasonActorUnknown : TEvents::TEvUndelivered::Disconnected,
                    fdp.ConsumeBool());
                handles.erase(handles.begin() + index);
                if (forwarded) {
                    handles.push_back(std::move(forwarded));
                }
                break;
            }

            case 4: {
                if (IsUnsureUndelivered(*handle)) {
                    break;
                }
                auto buffer = handle->GetChainBuffer();
                Y_ABORT_UNLESS(buffer);
                buffer->GetSize();
                buffer->GetString();
                break;
            }

            case 5: {
                if (IsUnsureUndelivered(*handle)) {
                    break;
                }
                auto buffer = handle->ReleaseChainBuffer();
                Y_ABORT_UNLESS(buffer);
                handles.erase(handles.begin() + index);
                handles.push_back(std::make_unique<IEventHandle>(
                    TEvents::TEvBlob::EventType,
                    ConsumeFlags(fdp),
                    MakeActorId(0, fdp.ConsumeIntegralInRange<ui64>(0, 256)),
                    MakeActorId(0, fdp.ConsumeIntegralInRange<ui64>(257, 512)),
                    std::move(buffer),
                    nextCookie++));
                break;
            }

            case 6:
                if (auto* ev = GetLocalEventByActualType<TEvents::TEvBlob>(*handle)) {
                    Y_ABORT_UNLESS(ev->Type() == TEvents::TEvBlob::EventType);
                }
                break;

            case 7:
                if (handle->HasEvent()) {
                    auto base = handle->ReleaseBase();
                    Y_ABORT_UNLESS(base);
                    handles.erase(handles.begin() + index);
                    handles.push_back(std::make_unique<IEventHandle>(
                        MakeActorId(0, fdp.ConsumeIntegralInRange<ui64>(0, 256)),
                        MakeActorId(0, fdp.ConsumeIntegralInRange<ui64>(257, 512)),
                        base.Release(),
                        ConsumeFlags(fdp),
                        nextCookie++));
                }
                break;

            case 8:
                handle->GetForwardOnNondeliveryRecipient();
                break;

            case 9:
                handles.erase(handles.begin() + index);
                break;
        }

        if (!handles.empty()) {
            CheckHandle(*handles[fdp.ConsumeIntegralInRange<size_t>(0, handles.size() - 1)]);
        }
        if (handles.size() > MaxHandles) {
            handles.erase(handles.begin());
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    RunEventHandleFuzz(fdp);
    return 0;
}
