#ifndef STAN_LANG_GENERATOR_GENERATE_PARAM_VAR_HPP
#define STAN_LANG_GENERATOR_GENERATE_PARAM_VAR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_bare_type.hpp>
#include <stan/lang/generator/generate_expression.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/write_constraints_fn.hpp>
#include <stan/lang/generator/write_end_loop.hpp>
#include <stan/lang/generator/write_nested_resize_loop_begin.hpp>
#include <stan/lang/generator/write_resize_var_idx.hpp>
#include <iostream>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
namespace lang {

/**
 * Generate dynamic initializations for container parameter variables
 * or void statement for scalar parameters.
 *
 * @param[in] var_decl parameter block variable
 * @param[in] gen_decl_stmt if true, generate variable declaration
 * @param[in] indent indentation level
 * @param[in,out] o stream for generating
 */
void generate_param_var(const block_var_decl &var_decl, bool gen_decl_stmt,
                        int indent, std::ostream &o) {
  // setup - name, type, and var shape
  std::string var_name(var_decl.name());
  std::vector<expression> dims(var_decl.type().array_lens());

  block_var_type btype = var_decl.type().innermost_type();
  std::string constrain_str = write_constraints_fn(btype, "constrain");
  // lp__ is single or last arg to write_constraints_fn
  std::string lp_arg("lp__)");
  if (btype.has_def_bounds() || btype.has_def_offset_multiplier() ||
      !btype.bare_type().is_double_type())
    lp_arg = ", lp__)";

  // declare
  if (gen_decl_stmt) {
    generate_indent(indent, o);
    generate_bare_type(var_decl.type().bare_type(), "local_scalar_t__", o);
    o << " " << var_name << ";" << EOL;
  }

  // init
  write_nested_resize_loop_begin(var_name, dims, indent, o);

  // innermost loop stmt: read in param, apply jacobian
  generate_indent(indent + dims.size(), o);
  o << "if (jacobian__)" << EOL;

  generate_indent(indent + dims.size() + 1, o);
  if (dims.size() > 0) {
    o << var_name;
    write_resize_var_idx(dims.size(), o);
    o << ".push_back(in__." << constrain_str << lp_arg << ");" << EOL;
  } else {
    o << var_name << " = in__." << constrain_str << lp_arg << ";" << EOL;
  }

  generate_indent(indent + dims.size(), o);
  o << "else" << EOL;

  generate_indent(indent + dims.size() + 1, o);
  if (dims.size() > 0) {
    o << var_name;
    write_resize_var_idx(dims.size(), o);
    o << ".push_back(in__." << constrain_str << "));" << EOL;
  } else {
    o << var_name << " = in__." << constrain_str << ");" << EOL;
  }

  write_end_loop(dims.size(), indent, o);
}

}  // namespace lang
}  // namespace stan
#endif
