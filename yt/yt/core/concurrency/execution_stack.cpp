#include "execution_stack.h"
#include "private.h"

#if defined(_unix_)
#   include <sys/mman.h>
#   include <limits.h>
#   include <unistd.h>
#   if !defined(__x86_64__) && !defined(__arm64__) && !defined(__aarch64__)
#       error Unsupported platform
#   endif
#endif

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/object_pool.h>

#include <library/cpp/yt/memory/ref.h>
#include <library/cpp/yt/memory/ref_tracked.h>

#include <library/cpp/yt/misc/tls.h>

#include <library/cpp/yt/system/exit.h>

#include <util/system/sanitizers.h>

namespace NYT::NConcurrency {

static constexpr auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

// Stack sizes.
#if defined(_asan_enabled_) || defined(_msan_enabled_)
    static constexpr size_t SmallExecutionStackSize = 2_MB;
    static constexpr size_t LargeExecutionStackSize = 64_MB;
#else
    static constexpr size_t SmallExecutionStackSize = 256_KB;
    static constexpr size_t LargeExecutionStackSize = 8_MB;
#endif

////////////////////////////////////////////////////////////////////////////////

TExecutionStackBase::TExecutionStackBase(size_t size)
    : Stack_(nullptr)
    , Size_(RoundUpToPage(size))
{
    auto cookie = GetRefCountedTypeCookie<TExecutionStack>();
    TRefCountedTrackerFacade::AllocateSpace(cookie, Size_);
}

TExecutionStackBase::~TExecutionStackBase()
{
    auto cookie = GetRefCountedTypeCookie<TExecutionStack>();
    TRefCountedTrackerFacade::FreeSpace(cookie, Size_);
}

void* TExecutionStackBase::GetStack() const
{
    return Stack_;
}

size_t TExecutionStackBase::GetSize() const
{
    return Size_;
}

////////////////////////////////////////////////////////////////////////////////

#if defined(_unix_)

TExecutionStack::TExecutionStack(size_t size)
    : TExecutionStackBase(size)
{
    const size_t guardSize = GuardPageCount * GetPageSize();

    int flags =
#if defined(_darwin_)
        MAP_ANON | MAP_PRIVATE;
#else
        MAP_ANONYMOUS | MAP_PRIVATE;
#endif

    Base_ = reinterpret_cast<char*>(::mmap(
        0,
        guardSize * 2 + Size_,
        PROT_READ | PROT_WRITE,
        flags,
        -1,
        0));

    auto checkOom = [] {
        if (LastSystemError() == ENOMEM) {
            fprintf(stderr, "Out-of-memory condition detected while allocating execution stack; terminating\n");
            AbortProcess(ToUnderlying(EProcessExitCode::OutOfMemory));
        }
    };

    if (Base_ == MAP_FAILED) {
        checkOom();
        YT_LOG_FATAL(TError::FromSystem(), "Failed to allocate execution stack (Size: %v)", Size_);
    }

    if (::mprotect(Base_, guardSize, PROT_NONE) == -1) {
        checkOom();
        YT_LOG_FATAL(TError::FromSystem(), "Failed to protect execution stack from below (GuardSize: %v)", guardSize);
    }

    if (::mprotect(Base_ + guardSize + Size_, guardSize, PROT_NONE) == -1) {
        checkOom();
        YT_LOG_FATAL(TError::FromSystem(), "Failed to protect execution stack from above (GuardSize: %v)", guardSize);
    }

    Stack_ = Base_ + guardSize;
    YT_VERIFY((reinterpret_cast<uintptr_t>(Stack_)& 15) == 0);
}

TExecutionStack::~TExecutionStack()
{
    const size_t guardSize = GuardPageCount * GetPageSize();
    ::munmap(Base_, guardSize * 2 + Size_);
}

#elif defined(_win_)

TExecutionStack::TExecutionStack(size_t size)
    : TExecutionStackBase(size)
    , Handle_(::CreateFiber(Size_, &FiberTrampoline, this))
    , Trampoline_(nullptr)
{ }

TExecutionStack::~TExecutionStack()
{
    ::DeleteFiber(Handle_);
}

YT_DEFINE_THREAD_LOCAL(void*, FiberTrampolineOpaque);

void TExecutionStack::SetOpaque(void* opaque)
{
    FiberTrampolineOpaque() = opaque;
}

void* TExecutionStack::GetOpaque()
{
    return FiberTrampolineOpaque();
}

void TExecutionStack::SetTrampoline(void (*trampoline)(void*))
{
    YT_ASSERT(!Trampoline_);
    Trampoline_ = trampoline;
}

VOID CALLBACK TExecutionStack::FiberTrampoline(PVOID opaque)
{
    auto* stack = reinterpret_cast<TExecutionStack*>(opaque);
    stack->Trampoline_(FiberTrampolineOpaque());
}

#else
#   error Unsupported platform
#endif

////////////////////////////////////////////////////////////////////////////////

template <EExecutionStackKind Kind, size_t Size>
class TPooledExecutionStack
    : public TExecutionStack
    , public TRefTracked<TPooledExecutionStack<Kind, Size>>
{
public:
    TPooledExecutionStack()
        : TExecutionStack(Size)
    { }
};

std::shared_ptr<TExecutionStack> CreateExecutionStack(EExecutionStackKind kind)
{
    switch (kind) {
        case EExecutionStackKind::Small:
            return ObjectPool<TPooledExecutionStack<EExecutionStackKind::Small, SmallExecutionStackSize>>().Allocate();
        case EExecutionStackKind::Large:
            return ObjectPool<TPooledExecutionStack<EExecutionStackKind::Large, LargeExecutionStackSize>>().Allocate();
        default:
            YT_ABORT();
    }
}

/////////////////////////////////////////////////////////////////////////////

static std::atomic<int> SmallFiberStackPoolSize = {1024};
static std::atomic<int> LargeFiberStackPoolSize = {1024};

int GetFiberStackPoolSize(EExecutionStackKind stackKind)
{
    switch (stackKind) {
        case EExecutionStackKind::Small: return SmallFiberStackPoolSize.load(std::memory_order::relaxed);
        case EExecutionStackKind::Large: return LargeFiberStackPoolSize.load(std::memory_order::relaxed);
        default:                         YT_ABORT();
    }
}

void SetFiberStackPoolSize(EExecutionStackKind stackKind, int poolSize)
{
    if (poolSize < 0) {
        YT_LOG_FATAL("Invalid fiber stack pool size (Size: %v, Kind: %v)",
            poolSize,
            stackKind);
    }
    switch (stackKind) {
        case EExecutionStackKind::Small: SmallFiberStackPoolSize = poolSize; break;
        case EExecutionStackKind::Large: LargeFiberStackPoolSize = poolSize; break;
        default:                         YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <NConcurrency::EExecutionStackKind Kind, size_t Size>
struct TPooledObjectTraits<NConcurrency::TPooledExecutionStack<Kind, Size>, void>
    : public TPooledObjectTraitsBase<NConcurrency::TPooledExecutionStack<Kind, Size>>
{
    using TStack = NConcurrency::TPooledExecutionStack<Kind, Size>;

    static void Clean(TStack* stack)
    {
#if defined(_asan_enabled_)
        if (stack->GetStack()) {
            NSan::Poison(stack->GetStack(), stack->GetSize());
        }
#else
        Y_UNUSED(stack);
#endif
    }

    static int GetMaxPoolSize()
    {
        return NConcurrency::GetFiberStackPoolSize(Kind);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
