#pragma once

#include "defs.h"

#include <ydb/core/base/events.h>

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>

namespace NKikimr::NDDisk {

    struct TEv {
        enum {
            EvDDiskConnect = EventSpaceBegin(TKikimrEvents::ES_DDISK),
            EvDDiskConnectResult,
            EvDDiskDisconnect,
            EvDDiskDisconnectResult,
            EvDDiskWrite,
            EvDDiskWriteResult,
            EvDDiskRead,
            EvDDiskReadResult,
            EvDDiskWritePersistentBuffer,
            EvDDiskWritePersistentBufferResult,
            EvDDiskReadPersistentBuffer,
            EvDDiskReadPersistentBufferResult,
            EvDDiskFlushPersistentBuffer,
            EvDDiskFlushPersistentBufferResult,
            EvDDiskListPersistentBuffer,
            EvDDiskListPersistentBufferResult,
        };
    };

#define DECLARE_DDISK_EVENT(NAME) \
    struct TEvDDisk##NAME : TEventPB<TEvDDisk##NAME, NKikimrBlobStorage::TEvDDisk##NAME, TEv::EvDDisk##NAME>

    DECLARE_DDISK_EVENT(Connect) {
    };

    DECLARE_DDISK_EVENT(ConnectResult) {
    };

    DECLARE_DDISK_EVENT(Disconnect) {
    };

    DECLARE_DDISK_EVENT(DisconnectResult) {
    };

    DECLARE_DDISK_EVENT(Write) {
    };

    DECLARE_DDISK_EVENT(WriteResult) {
    };

    DECLARE_DDISK_EVENT(Read) {
    };

    DECLARE_DDISK_EVENT(ReadResult) {
    };

    DECLARE_DDISK_EVENT(WritePersistentBuffer) {
    };

    DECLARE_DDISK_EVENT(WritePersistentBufferResult) {
    };

    DECLARE_DDISK_EVENT(ReadPersistentBuffer) {
    };

    DECLARE_DDISK_EVENT(ReadPersistentBufferResult) {
    };

    DECLARE_DDISK_EVENT(FlushPersistentBuffer) {
    };

    DECLARE_DDISK_EVENT(FlushPersistentBufferResult) {
    };

    DECLARE_DDISK_EVENT(ListPersistentBuffer) {
    };

    DECLARE_DDISK_EVENT(ListPersistentBufferResult) {
    };

    IActor *CreateDDiskActor(TVDiskConfig::TBaseInfo&& baseInfo, TIntrusivePtr<TBlobStorageGroupInfo> info,
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

} // NKikimr::NDDisk
