#ifndef CANCELATION_TOKEN_INL_H_
#error "Direct inclusion of this file is not allowed, include cancelation_token.h"
// For the sake of sane code completion.
#include "cancelation_token.h"
#endif

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TDecayedConcrete>
constexpr TDecayedConcrete& TAnyCancelationToken::TStorage::AsConcrete() & noexcept
{
    using TPtr = TDecayedConcrete*;

    if constexpr (SmallToken<TDecayedConcrete>) {
        return *std::launder(reinterpret_cast<TPtr>(&Storage_));
    } else {
        return *static_cast<TPtr>(*std::launder(reinterpret_cast<void**>(&Storage_)));
    }
}

template <class TDecayedConcrete>
constexpr const TDecayedConcrete& TAnyCancelationToken::TStorage::AsConcrete() const & noexcept
{
    using TPtr = const TDecayedConcrete*;

    if constexpr (SmallToken<TDecayedConcrete>) {
        return *std::launder(reinterpret_cast<TPtr>(&Storage_));
    } else {
        return *static_cast<TPtr>(*std::launder(reinterpret_cast<void const* const*>(&Storage_)));
    }
}

template <class TDecayedConcrete>
constexpr TDecayedConcrete&& TAnyCancelationToken::TStorage::AsConcrete() && noexcept
{
    using TPtr = TDecayedConcrete*;

    if constexpr (SmallToken<TDecayedConcrete>) {
        return std::move(*std::launder(reinterpret_cast<TPtr>(&Storage_)));
    } else {
        return std::move(*static_cast<TPtr>(*std::launder(reinterpret_cast<void**>(&Storage_))));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <CCancelationToken TDecayedConcrete>
TAnyCancelationToken::TVTable TAnyCancelationToken::TVTable::Create() noexcept
{
    TVTable table = {};
    table.Dtor_ = +[] (TStorage& what) {
        TAlloc allocator = {};

        auto* ptr = &what.template AsConcrete<TDecayedConcrete>();
        std::destroy_at(ptr);

        if constexpr (!SmallToken<TDecayedConcrete>) {
            TTraits::deallocate(allocator, reinterpret_cast<std::byte*>(ptr), sizeof(TDecayedConcrete));
        }
    };

    table.CopyCtor_ = +[] (TStorage& where, const TStorage& what) -> void {
        TAlloc allocator = {};

        if constexpr (SmallToken<TDecayedConcrete>) {
            where.Set();
        } else {
            where.Set(TTraits::allocate(allocator, sizeof(TDecayedConcrete)));
        }

        TTraits::template construct<TDecayedConcrete>(
            allocator,
            &where.template AsConcrete<TDecayedConcrete>(),
            what.template AsConcrete<TDecayedConcrete>());
    };

    table.MoveCtor_ = +[] (TStorage& where, TStorage&& what) -> void {
        if constexpr (SmallToken<TDecayedConcrete>) {
            TAlloc allocator = {};

            where.Set();

            TTraits::template construct<TDecayedConcrete>(
                allocator,
                &where.template AsConcrete<TDecayedConcrete>(),
                std::move(what).template AsConcrete<TDecayedConcrete>());
            TTraits::template destroy<TDecayedConcrete>(
                allocator,
                &what.template AsConcrete<TDecayedConcrete>());
        } else {
            where.Set(static_cast<void*>(&what));
        }
    };

    table.IsCancelationRequested_ = +[] (const TStorage& what) -> bool {
        return what.template AsConcrete<TDecayedConcrete>().IsCancelationRequested();
    };

    table.CancellationError_ = +[] (const TStorage& what) -> const TError& {
        return what.template AsConcrete<TDecayedConcrete>().GetCancelationError();
    };

    return table;
}

////////////////////////////////////////////////////////////////////////////////

template <class TToken>
    requires (!std::same_as<TAnyCancelationToken, std::remove_cvref_t<TToken>> &&
                CCancelationToken<std::remove_cvref_t<TToken>>)
TAnyCancelationToken::TAnyCancelationToken(TToken&& token)
{
    Set<TToken>(std::forward<TToken>(token));
}

template <class TToken>
void TAnyCancelationToken::Set(TToken&& token)
{
    using TDecayed = std::remove_cvref_t<TToken>;
    TAlloc allocator = {};

    Reset();

    VTable_ = &StaticTable<TDecayed>;

    if constexpr (SmallToken<TDecayed>) {
        Storage_.Set();
    } else {
        Storage_.Set(TTraits::allocate(allocator, sizeof(TDecayed)));
    }

    TTraits::template construct<TDecayed>(
        allocator,
        &Storage_.template AsConcrete<TDecayed>(),
        std::forward<TToken>(token));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
