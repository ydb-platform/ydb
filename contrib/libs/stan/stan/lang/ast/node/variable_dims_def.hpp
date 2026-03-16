#ifndef STAN_LANG_AST_NODE_VARIABLE_DIMS_DEF_HPP
#define STAN_LANG_AST_NODE_VARIABLE_DIMS_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    variable_dims::variable_dims() { }

    variable_dims::variable_dims(const std::string& name,
                                 const std::vector<expression>& dims)
      : name_(name), dims_(dims) { }

  }
}
#endif
