#ifndef STAN_LANG_AST_DOUBLE_TYPE_DEF_HPP
#define STAN_LANG_AST_DOUBLE_TYPE_DEF_HPP

#include <stan/lang/ast/type/double_type.hpp>
#include <string>

namespace stan {
  namespace lang {

    double_type::double_type(bool is_data) : is_data_(is_data) { }

    double_type::double_type() : double_type(false) { }

    std::string double_type::oid() const {
      return "03_double_type";
    }
  }
}
#endif

