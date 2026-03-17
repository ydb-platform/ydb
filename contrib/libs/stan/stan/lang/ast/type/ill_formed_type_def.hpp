#ifndef STAN_LANG_AST_ILL_FORMED_TYPE_DEF_HPP
#define STAN_LANG_AST_ILL_FORMED_TYPE_DEF_HPP

#include <stan/lang/ast/type/ill_formed_type.hpp>
#include <string>

namespace stan {
  namespace lang {

    ill_formed_type::ill_formed_type() : is_data_(false) { }

    std::string ill_formed_type::oid() const {
      return "00_ill_formed_type";
    }
  }
}
#endif

