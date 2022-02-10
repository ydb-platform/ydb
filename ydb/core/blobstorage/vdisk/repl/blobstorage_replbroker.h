#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr {

    // Replication token. Token is requested from replication broker using TEvQueryReplToken message. Token is granted
    // via TEvReplToken message, after reception of this message VDisk has to start its replication session. When session
    // is finished, granted token is returned via TEvReleaseReplToken message. Token is identified by the sender actor,
    // so the sender for Query and Release messages must be the same.

    struct TEvQueryReplToken : public TEventLocal<TEvQueryReplToken, TEvBlobStorage::EvQueryReplToken> {
        ui32 PDiskId; // PDisk it resides on

        TEvQueryReplToken(ui32 pdiskId)
            : PDiskId(pdiskId)
        {}
    };

    struct TEvReplToken : public TEventLocal<TEvReplToken, TEvBlobStorage::EvReplToken>
    {};

    struct TEvReleaseReplToken : public TEventLocal<TEvReleaseReplToken, TEvBlobStorage::EvReleaseReplToken>
    {};

    // Memory usage token. When replication proxy wants to keep N bytes in memory/in flight of interconnect queue, then
    // it sends TEvQueryReplMemToken request to replication broker. When granted via TEvReplMemToken, proxy issues
    // operation that consumes specified amount of memory. When amount changes, it can be corrected by sending
    // TEvUpdateReplMemToken message to broker. When memory is released, proxy sends TEvReleaseReplMemToken to broker.

    using TReplMemTokenId = ui64;

    struct TEvQueryReplMemToken : public TEventLocal<TEvQueryReplMemToken, TEvBlobStorage::EvQueryReplMemToken> {
        ui64 Bytes;

        TEvQueryReplMemToken(ui64 bytes)
            : Bytes(bytes)
        {}
    };

    struct TEvReplMemToken : public TEventLocal<TEvReplMemToken, TEvBlobStorage::EvReplMemToken> {
        TReplMemTokenId Token;

        TEvReplMemToken(TReplMemTokenId token)
            : Token(token)
        {}
    };

    struct TEvUpdateReplMemToken : public TEventLocal<TEvUpdateReplMemToken, TEvBlobStorage::EvUpdateReplMemToken> {
        TReplMemTokenId Token;
        ui64 ActualBytes;

        TEvUpdateReplMemToken(TReplMemTokenId token, ui64 actualBytes)
            : Token(token)
            , ActualBytes(actualBytes)
        {}
    };

    struct TEvReleaseReplMemToken : public TEventLocal<TEvReleaseReplMemToken, TEvBlobStorage::EvReleaseReplMemToken> {
        TReplMemTokenId Token;

        TEvReleaseReplMemToken(TReplMemTokenId token)
            : Token(token)
        {}
    };

    extern IActor *CreateReplBrokerActor(ui64 maxMemBytes);

} // NKikimr
