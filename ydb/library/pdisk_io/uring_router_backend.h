#pragma once

#include <util/system/types.h>

struct io_uring;
struct io_uring_params;
struct io_uring_cqe;
struct __kernel_timespec;
struct iovec;

namespace NKikimr::NPDisk::NUringPrivate {

// Instance-local boundary for fallible liburing calls. Tests supply SQ/CQ
// storage and scripted kernel progress without requiring an io_uring kernel.
class IUringRouterBackend {
public:
    virtual ~IUringRouterBackend() = default;
    virtual int Init(unsigned entries, io_uring* ring, io_uring_params* params) = 0;
    virtual int Enable(io_uring* ring) = 0;
    virtual int RegisterFiles(io_uring* ring, const int* files, unsigned count) = 0;
    virtual int RegisterBuffers(io_uring* ring, const iovec* buffers, unsigned count) = 0;
    virtual int Submit(io_uring* ring) = 0;
    virtual int PeekCqe(io_uring* ring, io_uring_cqe** cqe) = 0;
    virtual int WaitCqeTimeout(io_uring* ring, io_uring_cqe** cqe, __kernel_timespec* timeout) = 0;
    virtual void Exit(io_uring* ring) = 0;
    virtual void Backoff(ui32 micros) = 0;
};

} // namespace NKikimr::NPDisk::NUringPrivate
