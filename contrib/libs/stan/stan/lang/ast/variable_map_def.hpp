#ifndef STAN_LANG_AST_VARIABLE_MAP_DEF_HPP
#define STAN_LANG_AST_VARIABLE_MAP_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>
#include <iostream>

namespace stan {
  namespace lang {

    bool variable_map::exists(const std::string& name) const {
      return map_.find(name) != map_.end();
    }

    var_decl variable_map::get(const std::string& name) const {
      if (!exists(name))
        throw std::invalid_argument("variable does not exist");
      return map_.find(name)->second.first;
    }

    bare_expr_type variable_map::get_bare_type(const std::string& name) const {
      return get(name).bare_type_;
    }

    scope variable_map::get_scope(const std::string& name) const {
      if (!exists(name))
        throw std::invalid_argument("variable does not exist");
      return map_.find(name)->second.second;
    }

    void variable_map::add(const std::string& name,
                           const var_decl& decl,
                           const scope& scope_decl) {
      map_[name] = range_t(decl, scope_decl);
    }

    void variable_map::remove(const std::string& name) {
      map_.erase(name);
    }

    size_t variable_map::size() const {
      return map_.size();
    }
  }
}
#endif
