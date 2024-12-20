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
            TCommonHandleClass(ev.Record.GetHandleClass());
        } else if constexpr (THasGetHandleClass<TEv>::value) {
            TCommonHandleClass(ev.GetHandleClass());
        }
    }
    template <>
    TCommonHandleClass(const NKikimrBlobStorage::EPutHandleClass& putHandleClass) {
        PutHandleClass = putHandleClass;
    }
    template <>
    TCommonHandleClass(const NKikimrBlobStorage::EGetHandleClass& getHandleClass) {
        GetHandleClass = getHandleClass;
    }
    

    std::optional<NKikimrBlobStorage::EPutHandleClass> PutHandleClass;
    std::optional<NKikimrBlobStorage::EGetHandleClass> GetHandleClass;
};

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, const TIntrusivePtr<TVDiskContext>& vCtx, const TCommonHandleClass& handleClass);

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, ui32 channel, const TIntrusivePtr<TVDiskContext>& vCtx, const TCommonHandleClass& handleClass);

}//NKikimr
