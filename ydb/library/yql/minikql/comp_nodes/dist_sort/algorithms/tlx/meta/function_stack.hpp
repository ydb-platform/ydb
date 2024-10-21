/*******************************************************************************
 * tlx/meta/function_stack.hpp
 *
 * A FunctionStack stores a sequence of lambdas or functors which _emit_ items
 * to the next stage. Each lambda/functor is called with (item, emit) where emit
 * is the chain of subsequent functors. This "push-down items" method is used by
 * Thrill to enable expanding of one item to many in FlatMap().
 *
 * Given functors f_1, f_2, ... f_n, the FunctionStack calls f_1(x, f_2), for x,
 * and f_1 calls f_2(y, f_3) for each y it emits.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2015-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_FUNCTION_STACK_HEADER
#define TLX_META_FUNCTION_STACK_HEADER

#include <tlx/meta/index_sequence.hpp>

#include <tuple>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

namespace meta_detail {

/*!
 * Base case for the chaining of functors.  The last functor receives an input
 * element, no emitter and should have no return type.  It should therefore
 * store the input parameter externally.
 *
 * \param functor Functor that represents the chain end.
 */
template <typename Functor>
auto call_stack(const Functor& functor) {
    // the functor object is captured by non-const copy so that we can use
    // functors with non-const operator(), i.e. stateful functors (e.g. for
    // sampling)
    return [=, functor = functor](const auto& input) mutable -> void {
               functor(input);
    };
}

/*!
 * Recursive case for the chaining of functors.  The given functor receives an
 * input element, an emitter and should have no return type.  The emitter will
 * be built using the chain of remaining functors.
 *
 * \param functor Current functor to be chained.
 *
 * \param rest Remaining functors.
 */
template <typename Functor, typename... MoreFunctors>
auto call_stack(const Functor& functor, const MoreFunctors& ... rest) {
    // the functor object is captured by non-const copy so that we can use
    // functors with non-const operator(), i.e. stateful functors (e.g. for
    // sampling)
    return [=, functor = functor](const auto& input) mutable -> void {
               functor(input, call_stack(rest...));
    };
}

} // namespace meta_detail

/*!
 * A FunctionStack is a chain of functor that can be folded to a single functor
 * (which is usually optimize by the compiler).
 *
 * All functors within the chain receive a single input value and a emitter
 * function.  The emitter function is used for chaining functors together by
 * enabling items to pushed down the sequence.  The single exception to this is
 * the last functor, which receives no emitter, and hence usually stores the
 * items somehow.
 *
 * The FunctionStack basically consists of a tuple that contains functor objects
 * of varying types.
 *
 * \tparam Input_ Input to first functor.
 *
 * \tparam Functors Types of the different functor.
 */
template <typename Input_, typename... Functors>
class FunctionStack
{
public:
    using Input = Input_;

    //! Construct an empty FunctionStack
    FunctionStack() = default;

    /*!
     * Initialize the function stack with a given tuple of functors.
     *
     * \param stack Tuple of functor.
     */
    explicit FunctionStack(const std::tuple<Functors...>& stack)
        : stack_(stack) { }

    /*!
     * Add a functor function to the end of the stack.
     *
     * \tparam Functor Type of the functor.
     *
     * \param functor Functor that should be added to the stack.
     *
     * \return New stack containing the previous and new functor(s).
     */
    template <typename Functor>
    auto push(const Functor& functor) const {
        // append to function stack's type the new function.
        return FunctionStack<Input, Functors..., Functor>(
            std::tuple_cat(stack_, std::make_tuple(functor)));
    }

    /*!
     * Add a functor to the end of the stack. Alias for push().
     *
     * \tparam Functor Type of the functors.
     *
     * \param functor functor that should be added to the stack.
     *
     * \return New stack containing the previous and new functor(s).
     */
    template <typename Functor>
    auto operator & (const Functor& functor) const { return push(functor); }

    /*!
     * Build a single functor by "folding" the stack.  Folding means that the
     * stack is processed from front to back and each emitter is composed using
     * previous functors.
     *
     * \return Single "folded" functor representing the whole stack.
     */
    auto fold() const {
        return fold_stack(make_index_sequence<sizeof ... (Functors)>{ });
    }

    //! Is true if the FunctionStack is empty.
    static constexpr bool empty = (sizeof ... (Functors) == 0);

    //! Number of functors on the FunctionStack
    static constexpr size_t size = sizeof ... (Functors);

private:
    //! Tuple of varying type that stores all functor objects functions.
    std::tuple<Functors...> stack_;

    /*!
     * Auxilary function for "folding" the stack.  This is needed to send all
     * functors as parameters to the function that folds them together.
     *
     * \return Single "folded" functor representing the stack.
     */
    template <size_t... Is>
    auto fold_stack(index_sequence<Is...>) const {
        return meta_detail::call_stack(std::get<Is>(stack_) ...);
    }
};

//! Function-style construction of a FunctionStack
template <typename Input, typename Functor>
static inline
auto make_function_stack(const Functor& functor) {
    return FunctionStack<Input, Functor>(std::make_tuple(functor));
}

//! \}

} // namespace tlx

#endif // !TLX_META_FUNCTION_STACK_HEADER

/******************************************************************************/
