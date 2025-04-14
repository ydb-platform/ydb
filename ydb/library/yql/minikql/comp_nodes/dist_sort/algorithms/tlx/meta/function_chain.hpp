/*******************************************************************************
 * tlx/meta/function_chain.hpp
 *
 * A FunctionChain stores a sequence of lambdas or functors f_1, f_2, ... f_n,
 * which are composed together as f_n(... f_2(f_1(x))). Each lambda/functor is
 * called with the result of the previous. This basically implements
 * compile-time function composition.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2015-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_FUNCTION_CHAIN_HEADER
#define TLX_META_FUNCTION_CHAIN_HEADER

#include <tlx/meta/index_sequence.hpp>

#include <tuple>

namespace tlx {

//! \addtogroup tlx_meta
//! \{

namespace meta_detail {

/*!
 * Base case for the chaining of functors: zero functors, returns the identity.
 */
static inline auto call_chain() {
    return [](const auto& input) mutable -> auto {
               return input;
    };
}

/*!
 * Base case for the chaining of functors.  The one functor receives an input
 * element
 *
 * \param functor functor that represents the chain end.
 */
template <typename Functor>
auto call_chain(const Functor& functor) {
    // the functor is captured by non-const copy so that we can use functors
    // with non-const operator(), i.e. stateful functors (e.g. for sampling)
    return [functor = functor](const auto& input) mutable -> auto {
               return functor(input);
    };
}

/*!
 * Recursive case for the chaining of functors.  The first functor receives an
 * input element, and the remaining chain is applied to the result.
 *
 * \param functor Current functor to be chained.
 *
 * \param rest Remaining functors.
 */
template <typename Functor, typename... MoreFunctors>
auto call_chain(const Functor& functor, const MoreFunctors& ... rest) {
    // the functor is captured by non-const copy so that we can use functors
    // with non-const operator(), i.e. stateful functors (e.g. for sampling)
    return [=, functor = functor](const auto& input) mutable -> auto {
               return call_chain(rest...)(functor(input));
    };
}

} // namespace meta_detail

/*!
 * A FunctionChain is a chain of functors that can be folded to a single
 * functors.  All functors within the chain receive a single input value, which
 * is the result of all preceding functors in the chain.
 *
 * The FunctionChain basically consists of a tuple that contains functors of
 * varying types.
 *
 * \tparam Input_ Input to first functor functor.
 *
 * \tparam Functors Types of the different functors.
 */
template <typename... Functors>
class FunctionChain
{
public:
    //! default constructor: empty functor chain.
    FunctionChain() = default;

    /*!
     * Initialize the function chain with a given tuple of functions.
     *
     * \param chain Tuple of functors.
     */
    explicit FunctionChain(const std::tuple<Functors...>& chain)
        : chain_(chain) { }

    /*!
     * Add a functor to the end of the chain.
     *
     * \tparam Functor Type of the functors.
     *
     * \param functor functor that should be added to the chain.
     *
     * \return New chain containing the previous and new functor(s).
     */
    template <typename Functor>
    auto push(const Functor& functor) const {
        // append to function chain's type the new function.
        return FunctionChain<Functors..., Functor>(
            std::tuple_cat(chain_, std::make_tuple(functor)));
    }

    /*!
     * Add a functor to the end of the chain. Alias for fold().
     *
     * \tparam Functor Type of the functors.
     *
     * \param functor functor that should be added to the chain.
     *
     * \return New chain containing the previous and new functor(s).
     */
    template <typename Functor>
    auto operator & (const Functor& functor) const { return push(functor); }

    /*!
     * Build a single functor by "folding" the chain.  Folding means
     * that the chain is processed from front to back.
     *
     * \return Single "folded" functor representing the chain.
     */
    auto fold() const {
        return fold_chain(make_index_sequence<sizeof ... (Functors)>{ });
    }

    /*!
     * Directly call the folded function chain with a value.
     */
    template <typename... Input>
    auto operator () (Input&& ... value) const {
        return fold()(std::move(value...));
    }

    //! Is true if the FunctionChain is empty.
    static constexpr bool empty = (sizeof ... (Functors) == 0);

    //! Number of functors in the FunctionChain
    static constexpr size_t size = sizeof ... (Functors);

private:
    //! Tuple of varying type that stores all functors.
    std::tuple<Functors...> chain_;

    /*!
     * Auxilary function for "folding" the chain.  This is needed to send all
     * functors as parameters to the function that folds them together.
     *
     * \return Single "folded" functor representing the chain.
     */
    template <size_t... Is>
    auto fold_chain(index_sequence<Is...>) const {
        return meta_detail::call_chain(std::get<Is>(chain_) ...);
    }
};

//! Functor chain maker. Can also be called with a lambda function.
template <typename Functor>
static inline
auto make_function_chain(const Functor& functor) {
    return FunctionChain<Functor>(std::make_tuple(functor));
}

//! Construct and empty function chain.
static inline
auto make_function_chain() {
    return FunctionChain<>();
}

//! \}

} // namespace tlx

#endif // !TLX_META_FUNCTION_CHAIN_HEADER

/******************************************************************************/
