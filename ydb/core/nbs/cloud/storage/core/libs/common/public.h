#pragma once

#include <util/generic/ptr.h>
#include <util/generic/size_literals.h>

#include <memory>

////////////////////////////////////////////////////////////////////////////////

#if !defined(NDEBUG) && defined(NBS_FORCE_MOVE)
namespace std {
    template <typename T>
    T&& move(const T&) = delete;
}
#endif

namespace NYdb::NBS {
namespace NProbeParam {

////////////////////////////////////////////////////////////////////////////////

#if !defined(PLATFORM_PAGE_SIZE)
#   define PLATFORM_PAGE_SIZE 4096
#endif

#if !defined(PLATFORM_CACHE_LINE)
#   define PLATFORM_CACHE_LINE 64
#endif

#if !defined(Y_CACHE_ALIGNED)
#   define Y_CACHE_ALIGNED alignas(PLATFORM_CACHE_LINE)
#endif

////////////////////////////////////////////////////////////////////////////////

constexpr const char* MediaKind = "mediaKind";
constexpr const char* RequestId = "requestId";
constexpr const char* RequestType = "requestType";
constexpr const char* StartBlock = "startBlock";
constexpr const char* RequestSize = "requestSize";
constexpr const char* RequestTime = "requestTime";
constexpr const char* RequestExecutionTime = "requestExecutionTime";
constexpr const char* DiskId = "diskId";
constexpr const char* FsId = "fsId";

}   // namespace NProbeParam

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4_KB;
constexpr ui32 DefaultLocalSSDBlockSize = 512_B;

////////////////////////////////////////////////////////////////////////////////

struct TCallContextBase;
using TCallContextBasePtr = TIntrusivePtr<TCallContextBase>;

struct IScheduler;
using ISchedulerPtr = std::shared_ptr<IScheduler>;

struct IStartable;
using IStartablePtr = std::shared_ptr<IStartable>;

struct ITask;
using ITaskPtr = std::unique_ptr<ITask>;

struct ITaskQueue;
using ITaskQueuePtr = std::shared_ptr<ITaskQueue>;

struct ITimer;
using ITimerPtr = std::shared_ptr<ITimer>;

struct TFileIOCompletion;

struct IFileIOService;
using IFileIOServicePtr = std::shared_ptr<IFileIOService>;

struct IFileIOServiceFactory;
using IFileIOServiceFactoryPtr = std::shared_ptr<IFileIOServiceFactory>;

}   // namespace NYdb::NBS
