#pragma once
#include <util/system/yassert.h>
#include <exception>
#include <variant>

namespace NActors {

    namespace NDetail {

        struct TVoid {};

        template<class T>
        struct TReplaceVoid {
            using TType = T;
        };

        template<>
        struct TReplaceVoid<void> {
            using TType = TVoid;
        };

        template<class T>
        struct TLValue {
            using TType = T&;
        };

        template<>
        struct TLValue<void> {
            using TType = void;
        };

        template<class T>
        struct TRValue {
            using TType = T&&;
        };

        template<>
        struct TRValue<void> {
            using TType = void;
        };

    } // namespace NDetail

    /**
     * Wrapper for the task result
     */
    template<class T>
    class TTaskResult {
    public:
        void SetValue()
            requires (std::same_as<T, void>)
        {
            Result.template emplace<1>();
        }

        template<class TResult>
        void SetValue(TResult&& result)
            requires (!std::same_as<T, void>)
        {
            Result.template emplace<1>(std::forward<TResult>(result));
        }

        void SetException(std::exception_ptr&& e) noexcept {
            Result.template emplace<2>(std::move(e));
        }

        typename NDetail::TLValue<T>::TType Value() & {
            switch (Result.index()) {
                case 0: {
                    Y_ABORT("Task result has no value");
                }
                case 1: {
                    if constexpr (std::same_as<T, void>) {
                        return;
                    } else {
                        return std::get<1>(Result);
                    }
                }
                case 2: {
                    std::exception_ptr& e = std::get<2>(Result);
                    Y_DEBUG_ABORT_UNLESS(e, "Task exception missing");
                    std::rethrow_exception(e);
                }
            }
            Y_ABORT("Task result has an invalid state");
        }

        typename NDetail::TRValue<T>::TType Value() && {
            switch (Result.index()) {
                case 0: {
                    Y_ABORT("Task result has no value");
                }
                case 1: {
                    if constexpr (std::same_as<T, void>) {
                        return;
                    } else {
                        return std::get<1>(std::move(Result));
                    }
                }
                case 2: {
                    std::exception_ptr& e = std::get<2>(Result);
                    Y_DEBUG_ABORT_UNLESS(e, "Task exception missing");
                    std::rethrow_exception(std::move(e));
                }
            }
            Y_ABORT("Task result has an invalid state");
        }

    private:
        std::variant<std::monostate, typename NDetail::TReplaceVoid<T>::TType, std::exception_ptr> Result;
    };

} // namespace NActors
