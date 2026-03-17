#ifndef STAN_LANG_AST_NODE_PRINTABLE_DEF_HPP
#define STAN_LANG_AST_NODE_PRINTABLE_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    printable::printable() : printable_(std::string()) { }

    printable::printable(const expression& expr) : printable_(expr) { }

    printable::printable(const std::string& msg) : printable_(msg) { }

    printable::printable(const printable_t& printable)
      : printable_(printable) { }

    printable::printable(const printable& printable)
      : printable_(printable.printable_) { }

  }
}
#endif
