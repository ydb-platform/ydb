#pragma once

#include <concepts>
#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T, class TSignature>
struct TIsInvocable;

template <class T, class TResult, bool NoExcept, class... TArgs>
struct TIsInvocable<T, TResult(TArgs...) noexcept(NoExcept)>
{
private:
    static constexpr bool IsInvocable_ = requires (T&& t, TArgs&&... args) {
        { std::forward<T>(t)(std::forward<TArgs>(args)...) } -> std::same_as<TResult>;
    };

    static constexpr bool IsNoThrowInvocable_ = requires (T&& t, TArgs&&... args) {
        { std::forward<T>(t)(std::forward<TArgs>(args)...) } noexcept -> std::same_as<TResult>;
    };

public:
    static constexpr bool Value =
        IsInvocable_ &&
        (!NoExcept || IsNoThrowInvocable_);
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TObject, class TScalar>
concept CScalable = requires (TObject object, TScalar scalar)
{
    { object * scalar } -> std::same_as<TObject>;
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class TSignature>
concept CInvocable = NDetail::TIsInvocable<T, TSignature>::Value;

////////////////////////////////////////////////////////////////////////////////

template <class V>
concept CStdVector = requires (V& vec) {
    [] <class... T> (std::vector<T...>&) { } (vec);
};

////////////////////////////////////////////////////////////////////////////////

template <class M>
concept CAnyMap = requires {
    typename M::mapped_type;
    typename M::key_type;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
