#pragma once

#include <ydb/core/blobstorage/vdisk/common/vdisk_hulllogctx.h>

namespace NKikimr {

    class TEvHullHugeBlobLogged;

    /////////////////////////////////////////////////////////////////////////////////////////
    // These function are used for construction of NPDisk::TEvLog messages, that
    // update Hull database. Resulting TEvLog messages contains callback that
    // notifies SyncLog and Skeleton after successful commit of these records
    // to recovery log
    /////////////////////////////////////////////////////////////////////////////////////////
    std::unique_ptr<NPDisk::TEvLog> CreateHullUpdate(const std::shared_ptr<THullLogCtx> &hullLogCtx,
                                             TLogSignature signature,
                                             const TString &data,
                                             TLsnSeg seg,
                                             void *cookie,
                                             std::unique_ptr<IEventBase> syncLogMsg,
                                             std::unique_ptr<TEvHullHugeBlobLogged> hugeKeeperNotice);

    std::unique_ptr<NPDisk::TEvLog> CreateHullUpdate(const std::shared_ptr<THullLogCtx> &hullLogCtx,
                                             TLogSignature signature,
                                             const NPDisk::TCommitRecord &commitRecord,
                                             const TString &data,
                                             TLsnSeg seg,
                                             void *cookie,
                                             std::unique_ptr<IEventBase> syncLogMsg);

    std::unique_ptr<NPDisk::TEvLog> CreateHullUpdate(const std::shared_ptr<THullLogCtx> &hullLogCtx,
                                             TLogSignature signature,
                                             const TRcBuf &data,
                                             TLsnSeg seg,
                                             void *cookie,
                                             std::unique_ptr<IEventBase> syncLogMsg,
                                             std::unique_ptr<TEvHullHugeBlobLogged> hugeKeeperNotice);

    std::unique_ptr<NPDisk::TEvLog> CreateHullUpdate(const std::shared_ptr<THullLogCtx> &hullLogCtx,
                                             TLogSignature signature,
                                             const NPDisk::TCommitRecord &commitRecord,
                                             const TRcBuf &data,
                                             TLsnSeg seg,
                                             void *cookie,
                                             std::unique_ptr<IEventBase> syncLogMsg);


} // NKikimr
