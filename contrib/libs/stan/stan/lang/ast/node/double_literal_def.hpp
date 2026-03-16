#ifndef STAN_LANG_AST_NODE_DOUBLE_LITERAL_DEF_HPP
#define STAN_LANG_AST_NODE_DOUBLE_LITERAL_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
namespace lang {

double_literal::double_literal()
    : string_(std::string()), type_(double_type()) {}

double_literal::double_literal(double val)
    : val_(val), string_(std::string()), type_(double_type()) {}

}  // namespace lang
}  // namespace stan
#endif
