#pragma once
#include <exception>
#include <stdexcept>
#include <type_traits>
#include <variant>

namespace NActors {

    /**
     * A generic wrapper for async results.
     *
     * It contains either no result, the result of type T, or an exception.
     */
    template<class T>
    class TAsyncResult {
    public:
        void SetValue()
            requires (std::is_void_v<T>)
        {
            Value_.template emplace<1>();
        }

        template<class U>
        void SetValue(U&& value)
            requires (!std::is_void_v<T> && std::is_convertible_v<U&&, T>)
        {
            Value_.template emplace<1>(std::forward<U>(value));
        }

        void SetException(std::exception_ptr e) {
            Value_.template emplace<2>(std::move(e));
        }

        explicit operator bool() const {
            return Value_.index() != 0;
        }

        bool HasValue() const {
            return Value_.index() == 1;
        }

        bool HasException() const {
            return Value_.index() == 2;
        }

        T ExtractValue() {
            switch (Value_.index()) {
                case 1: {
                    if constexpr (std::is_void_v<T>) {
                        return;
                    } else {
                        return std::get<1>(std::move(Value_));
                    }
                }
                case 2: {
                    std::rethrow_exception(std::get<2>(std::move(Value_)));
                }
            }
            throw std::logic_error("result has neither value nor exception");
        }

        std::exception_ptr ExtractException() {
            if (Value_.index() != 2) [[unlikely]] {
                throw std::logic_error("result does not have an exception");
            }
            return std::get<2>(std::move(Value_));
        }

    private:
        struct TVoid {};
        using TValue = std::conditional_t<std::is_void_v<T>, TVoid, T>;
        std::variant<std::monostate, TValue, std::exception_ptr> Value_;
    };

} // namespace NActors
