#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

// CancelToken is an entity which you can ask whether cancellation has been
// requested or not. If it has been requested, you can ask for cancelation error.
template <class T>
concept CCancelationToken = requires (T& t) {
    { t.IsCancelationRequested() } -> std::same_as<bool>;
    { t.GetCancelationError() } -> std::same_as<const TError&>;
} && std::copyable<T>;

////////////////////////////////////////////////////////////////////////////////

// We need to read/write global variable which satisfies concept CCancelationToken.
class TAnyCancelationToken
{
private:
    static constexpr size_t SmallTokenSize = sizeof(void*) * 2;
    static constexpr size_t SmallTokenAlign = SmallTokenSize;

    template <class T>
    static constexpr bool SmallToken =
        (sizeof(T) <= SmallTokenSize) &&
        (alignof(T) <= SmallTokenAlign);

    using TAlloc = std::allocator<std::byte>;
    using TTraits = std::allocator_traits<TAlloc>;

    class TStorage
    {
    public:
        TStorage() = default;

        /*constexpr in C++23*/ void Set(void* ptr) noexcept;
        /*constexpr in C++23*/ void Set() noexcept;

        template <class TDecayedConcrete>
        constexpr TDecayedConcrete& AsConcrete() & noexcept;

        template <class TDecayedConcrete>
        constexpr const TDecayedConcrete& AsConcrete() const & noexcept;

        template <class TDecayedConcrete>
        constexpr TDecayedConcrete&& AsConcrete() && noexcept;

    private:
        alignas(SmallTokenAlign) std::byte Storage_[SmallTokenSize];
    };

    class TVTable
    {
    private:
        using TDtor = void(*)(TStorage& what);
        using TCopyCtor = void (*)(TStorage& where, const TStorage& what);
        using TMoveCtor = void (*)(TStorage& where, TStorage&& what);
        using TIsCancelationRequested = bool (*)(const TStorage& what);
        using TCancellationError = const TError& (*)(const TStorage& what);

    public:
        template <CCancelationToken TDecayedConcrete>
        static TVTable Create() noexcept;

        TDtor Dtor() const noexcept;
        TCopyCtor CopyCtor() const noexcept;
        TMoveCtor MoveCtor() const noexcept;
        TIsCancelationRequested IsCancelationRequested() const noexcept;
        TCancellationError GetCancelationError() const noexcept;

    private:
        TDtor Dtor_ = nullptr;
        TCopyCtor CopyCtor_ = nullptr;
        TMoveCtor MoveCtor_ = nullptr;
        TIsCancelationRequested IsCancelationRequested_ = nullptr;
        TCancellationError CancellationError_ = nullptr;

        TVTable() = default;
    };

    // Consider inline vtable storage.
    template <CCancelationToken TToken>
    static inline TVTable StaticTable = TVTable::template Create<TToken>();

public:
    TAnyCancelationToken() = default;

    template <class TToken>
        requires (!std::same_as<TAnyCancelationToken, std::remove_cvref_t<TToken>> &&
                    CCancelationToken<std::remove_cvref_t<TToken>>)
    TAnyCancelationToken(TToken&& token);

    TAnyCancelationToken(TAnyCancelationToken&& other) noexcept;
    TAnyCancelationToken& operator= (TAnyCancelationToken&& other) noexcept;

    TAnyCancelationToken(const TAnyCancelationToken& other) noexcept;
    TAnyCancelationToken& operator= (const TAnyCancelationToken& other) noexcept;

    explicit operator bool() const noexcept;

    ~TAnyCancelationToken();

    bool IsCancelationRequested() const noexcept;
    const TError& GetCancelationError() const noexcept;

private:
    TStorage Storage_ = {};
    TVTable* VTable_ = nullptr;

    void Reset() noexcept;

    template <class TToken>
    void Set(TToken&& token);
};

////////////////////////////////////////////////////////////////////////////////

class TCurrentCancelationTokenGuard
{
public:
    explicit TCurrentCancelationTokenGuard(TAnyCancelationToken nextToken);
    ~TCurrentCancelationTokenGuard();

    TCurrentCancelationTokenGuard(const TCurrentCancelationTokenGuard& other) = delete;
    TCurrentCancelationTokenGuard& operator= (const TCurrentCancelationTokenGuard& other) = delete;

private:
    TAnyCancelationToken PrevToken_;
};

////////////////////////////////////////////////////////////////////////////////

TCurrentCancelationTokenGuard MakeFutureCurrentTokenGuard(void* opaqueFutureState);
TCurrentCancelationTokenGuard MakeCancelableContextCurrentTokenGuard(const TCancelableContextPtr& context);

////////////////////////////////////////////////////////////////////////////////

const TAnyCancelationToken& GetCurrentCancelationToken();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail

#define CANCELATION_TOKEN_INL_H_
#include "cancelation_token-inl.h"
#undef CANCELATION_TOKEN_INL_H_
