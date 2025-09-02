#pragma once
#include "defs.h"
#include "vdisk_context.h"
#include "vdisk_mongroups.h"

namespace NKikimr {

template <typename TEv, typename = void>
struct THasGetHandleClass : std::false_type {};
template <typename TEv>
struct THasGetHandleClass<TEv, std::void_t<decltype(std::declval<TEv>().GetHandleClass())>> : std::true_type {};

template <typename TEv, typename = void>
struct THasRecordWithGetHandleClass : std::false_type {};
template <typename TEv>
struct THasRecordWithGetHandleClass<TEv, std::void_t<decltype(std::declval<TEv>().Record.GetHandleClass())>> : std::true_type {};

struct TCommonHandleClass {
    TCommonHandleClass() = default;
    
    template <typename TEv>
    TCommonHandleClass(const TEv& ev) {
        if constexpr (THasRecordWithGetHandleClass<TEv>::value) {
            HandleClass = ev.Record.GetHandleClass();
        } else if constexpr (THasGetHandleClass<TEv>::value) {
            HandleClass = ev.GetHandleClass();
        }
    }

    TCommonHandleClass(const NKikimrBlobStorage::EPutHandleClass& putHandleClass) {
        HandleClass = putHandleClass;
    }
    TCommonHandleClass(const NKikimrBlobStorage::EGetHandleClass& getHandleClass) {
        HandleClass = getHandleClass;
    }

    std::variant<std::monostate, NKikimrBlobStorage::EPutHandleClass, NKikimrBlobStorage::EGetHandleClass>  HandleClass;
};

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, const TIntrusivePtr<TVDiskContext>& vCtx, const TCommonHandleClass& handleClass);
void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, ui32 channel, const TIntrusivePtr<TVDiskContext>& vCtx, const TCommonHandleClass& handleClass);

}//NKikimr
