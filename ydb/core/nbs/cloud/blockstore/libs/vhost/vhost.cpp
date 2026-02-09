#include "vhost.h"

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <ydb/core/nbs/cloud/contrib/vhost/include/vhost/blockdev.h>
#include <ydb/core/nbs/cloud/contrib/vhost/include/vhost/server.h>

#include <util/generic/singleton.h>
#include <util/system/mutex.h>

namespace NYdb::NBS::NBlockStore::NVhost {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TLog VhostLog;

ELogPriority GetLogPriority(LogLevel level)
{
    switch (level) {
        case LOG_ERROR:
            return TLOG_ERR;
        case LOG_WARNING:
            return TLOG_WARNING;
        case LOG_INFO:
            return TLOG_INFO;
        case LOG_DEBUG:
            return TLOG_DEBUG;
    }
}

void vhd_log(LogLevel level, const char* format, ...)
{
    va_list params;
    va_start(params, format);

    ELogPriority priority = GetLogPriority(level);
    if (priority <= VhostLog.FiltrationLevel()) {
        Printf(VhostLog << priority << ": ", format, params);
    }

    va_end(params);
}

////////////////////////////////////////////////////////////////////////////////
class TVhostRequestImpl final: public TVhostRequest
{
private:
    vhd_io* VhdIo;

public:
    TVhostRequestImpl(vhd_io* vhdIo, EBlockStoreRequest type, ui64 from,
                      ui64 length, TSgList sgList, void* cookie)
        : TVhostRequest(type, from, length, std::move(sgList), cookie)
        , VhdIo(vhdIo)
    {}

    void Complete(EResult result) override
    {
        SgList.Close();
        vhd_complete_bio(VhdIo, GetVhostResult(result));
    }

private:
    vhd_bdev_io_result GetVhostResult(EResult result)
    {
        switch (result) {
            case SUCCESS:
                return VHD_BDEV_SUCCESS;
            case IOERR:
                return VHD_BDEV_IOERR;
            case CANCELLED:
                return VHD_BDEV_CANCELED;
        }
        Y_ABORT("Unexpected vhost result");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnregisterCompletion
{
private:
    TPromise<NProto::TError> Result;

public:
    TUnregisterCompletion(TPromise<NProto::TError> result)
        : Result(std::move(result))
    {}

    static void Callback(void* ctx)
    {
        std::unique_ptr<TUnregisterCompletion> completion(
            reinterpret_cast<TUnregisterCompletion*>(ctx));

        completion->OnCompletion();
    }

private:
    void OnCompletion()
    {
        Result.SetValue(NProto::TError());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVhostDevice final: public IVhostDevice
{
private:
    vhd_request_queue* const VhdQueue;
    const TString SocketPath;
    const TString DeviceName;
    void* const Cookie;

    vhd_bdev_info VhdBdevInfo;
    vhd_vdev* VhdVdev = nullptr;

    TMutex Lock;

public:
    TVhostDevice(vhd_request_queue* vhdQueue, TString socketPath,
                 TString deviceName, ui32 blockSize, ui64 blocksCount,
                 ui32 queuesCount, bool discardEnabled, ui32 optimalIoSize,
                 void* cookie, const TVhostCallbacks& callbacks)
        : VhdQueue(vhdQueue)
        , SocketPath(std::move(socketPath))
        , DeviceName(std::move(deviceName))
        , Cookie(cookie)
    {
        Zero(VhdBdevInfo);
        VhdBdevInfo.serial = DeviceName.c_str();
        VhdBdevInfo.socket_path = SocketPath.c_str();
        VhdBdevInfo.block_size = blockSize;
        VhdBdevInfo.total_blocks = blocksCount;
        VhdBdevInfo.num_queues = queuesCount;
        VhdBdevInfo.map_cb = callbacks.MapMemory;
        VhdBdevInfo.unmap_cb = callbacks.UnmapMemory;
        VhdBdevInfo.optimal_io_size = optimalIoSize;
        if (discardEnabled) {
            VhdBdevInfo.features |=
                VHD_BDEV_F_DISCARD | VHD_BDEV_F_WRITE_ZEROES;
        }
    }

    ~TVhostDevice() override
    {
        Stop();
    }

    bool Start() override
    {
        vhd_request_queue* queues[1] = {VhdQueue};

        VhdVdev = vhd_register_blockdev(&VhdBdevInfo, queues, 1, Cookie);

        return VhdVdev != nullptr;
    }

    TFuture<NProto::TError> Stop() override
    {
        if (!VhdVdev) {
            return MakeFuture(MakeError(S_ALREADY));
        }

        auto result = NewPromise<NProto::TError>();

        auto& Log = VhostLog;
        STORAGE_INFO("vhd_unregister_blockdev starting: " << SocketPath);
        result.GetFuture().Apply(
            [socketPath = SocketPath](const auto& future)
            {
                auto& Log = VhostLog;
                STORAGE_INFO(
                    "vhd_unregister_blockdev completed: " << socketPath);
                return future;
            });

        auto completion = std::make_unique<TUnregisterCompletion>(result);

        with_lock (Lock) {
            vhd_unregister_blockdev(VhdVdev, TUnregisterCompletion::Callback,
                                    completion.release());
            VhdVdev = nullptr;
        }

        return result.GetFuture();
    }

    void Update(ui64 blocksCount) override
    {
        with_lock (Lock) {
            if (!VhdVdev) {
                return;
            }
            vhd_blockdev_set_total_blocks(VhdVdev, blocksCount);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVhostQueue final: public IVhostQueue
{
private:
    TLog Log;
    vhd_request_queue* VhdRequestQueue;

public:
    TVhostQueue()
        : Log(VhostLog)
    {
        VhdRequestQueue = vhd_create_request_queue();
    }

    ~TVhostQueue() override
    {
        vhd_release_request_queue(VhdRequestQueue);
    }

    int Run() override
    {
        return vhd_run_queue(VhdRequestQueue);
    }

    void Stop() override
    {
        vhd_stop_queue(VhdRequestQueue);
    }

    IVhostDevicePtr CreateDevice(TString socketPath, TString deviceName,
                                 ui32 blockSize, ui64 blocksCount,
                                 ui32 queuesCount, bool discardEnabled,
                                 ui32 optimalIoSize, void* cookie,
                                 const TVhostCallbacks& callbacks) override
    {
        return std::make_shared<TVhostDevice>(
            VhdRequestQueue, std::move(socketPath), std::move(deviceName),
            blockSize, blocksCount, queuesCount, discardEnabled, optimalIoSize,
            cookie, callbacks);
    }

    TVhostRequestPtr DequeueRequest() override
    {
        vhd_request vhdRequest;

        while (vhd_dequeue_request(VhdRequestQueue, &vhdRequest)) {
            auto vhostRequest = CreateVhostRequest(vhdRequest);
            if (!vhostRequest) {
                vhd_complete_bio(vhdRequest.io, VHD_BDEV_IOERR);
                continue;
            }

            return vhostRequest;
        }

        return nullptr;
    }

private:
    TVhostRequestPtr CreateVhostRequest(const vhd_request& vhdRequest)
    {
        void* cookie = vhd_vdev_get_priv(vhdRequest.vdev);
        auto* vhdBdevIo = vhd_get_bdev_io(vhdRequest.io);

        EBlockStoreRequest type;
        switch (vhdBdevIo->type) {
            case VHD_BDEV_READ:
                type = EBlockStoreRequest::ReadBlocks;
                break;
            case VHD_BDEV_WRITE:
                type = EBlockStoreRequest::WriteBlocks;
                break;
            case VHD_BDEV_DISCARD:
            case VHD_BDEV_WRITE_ZEROES:
                type = EBlockStoreRequest::ZeroBlocks;
                break;
            default:
                STORAGE_ERROR(
                    "Unexpected vhost request type: "
                    << static_cast<int>(vhdBdevIo->type));
                return nullptr;
        }

        return std::make_shared<TVhostRequestImpl>(
            vhdRequest.io, type, vhdBdevIo->first_sector * VHD_SECTOR_SIZE,
            vhdBdevIo->total_sectors * VHD_SECTOR_SIZE,
            ConvertVhdSgList(vhdBdevIo->sglist), cookie);
    }

    static TSgList ConvertVhdSgList(const vhd_sglist& vhdSglist)
    {
        TSgList sgList;
        sgList.reserve(vhdSglist.nbuffers);
        for (ui32 i = 0; i < vhdSglist.nbuffers; ++i) {
            const auto& buffer = vhdSglist.buffers[i];
            sgList.emplace_back(reinterpret_cast<char*>(buffer.base), buffer.len);
        }
        return sgList;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVhostQueueFactory final: public IVhostQueueFactory
{
public:
    TVhostQueueFactory()
    {
        int res = vhd_start_vhost_server(vhd_log);
        Y_ABORT_UNLESS(res == 0, "Error starting vhost server");
    }

    ~TVhostQueueFactory() override
    {
        vhd_stop_vhost_server();
    }

    IVhostQueuePtr CreateQueue() override
    {
        return std::make_shared<TVhostQueue>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVhostRequest::TVhostRequest(EBlockStoreRequest type, ui64 from, ui64 length,
                             TSgList sgList, void* cookie)
    : Type(type)
    , From(from)
    , Length(length)
    , Cookie(cookie)
    , SgList(std::move(sgList))
{}

void InitVhostLog(ILoggingServicePtr logging)
{
    VhostLog = logging->CreateLog("BLOCKSTORE_VHOST");
}

IVhostQueueFactoryPtr CreateVhostQueueFactory()
{
    return std::make_shared<TVhostQueueFactory>();
}

}   // namespace NYdb::NBS::NBlockStore::NVhost
