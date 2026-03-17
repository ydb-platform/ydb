#ifndef STAN_LANG_GRAMMARS_COMMON_ADAPTORS_DEF_HPP
#define STAN_LANG_GRAMMARS_COMMON_ADAPTORS_DEF_HPP

#include <boost/fusion/include/adapt_struct.hpp>

#include <stan/lang/ast.hpp>

BOOST_FUSION_ADAPT_STRUCT(stan::lang::range,
                          (stan::lang::expression, low_)
                          (stan::lang::expression, high_) )

#endif
