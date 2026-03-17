#ifndef STAN_LANG_AST_INT_TYPE_DEF_HPP
#define STAN_LANG_AST_INT_TYPE_DEF_HPP

#include <stan/lang/ast/type/int_type.hpp>
#include <string>

namespace stan {
  namespace lang {

    int_type::int_type(bool is_data) : is_data_(is_data) { }

    int_type::int_type() : int_type(false) { }

    std::string int_type::oid() const {
      return "02_int_type";
    }
  }
}
#endif

