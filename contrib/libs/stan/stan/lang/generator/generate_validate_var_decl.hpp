#ifndef STAN_LANG_GENERATOR_GENERATE_VALIDATE_VAR_DECL_HPP
#define STAN_LANG_GENERATOR_GENERATE_VALIDATE_VAR_DECL_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_expression.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/write_begin_array_dims_loop.hpp>
#include <stan/lang/generator/write_end_loop.hpp>
#include <stan/lang/generator/write_var_idx_array_dims.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate code to validate the specified variable declaration
     * using the specified indentation level and stream.
     * Checks any defined bounds or constraints on specialized types.
     * NOTE:  bounded / specialized types are mutually exclusive
     *
     * @param[in] decl variable declaration
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_validate_var_decl(const block_var_decl decl,
                                    int indent, std::ostream& o) {
      std::string var_name(decl.name());
      std::vector<expression> ar_lens(decl.type().array_lens());

      block_var_type btype = decl.type().innermost_type();
      if (btype.has_def_bounds()) {
        range bounds = btype.bounds();
        write_begin_array_dims_loop(decl, true, indent, o);
        if (bounds.has_low()) {
          generate_indent(indent + ar_lens.size(), o);
          o << "check_greater_or_equal(function__, ";
          o << "\"" << var_name;
          write_var_idx_array_dims(ar_lens.size(), o);
          o << "\", " << var_name;
          write_var_idx_array_dims(ar_lens.size(), o);
          o << ", ";
          generate_expression(bounds.low_.expr_, NOT_USER_FACING, o);
          o << ");" << EOL;
        }
        if (bounds.has_high()) {
          generate_indent(indent + ar_lens.size(), o);
          o << "check_less_or_equal(function__, ";
          o << "\"" << var_name;
          write_var_idx_array_dims(ar_lens.size(), o);
          o << "\", " << var_name;
          write_var_idx_array_dims(ar_lens.size(), o);
          o << ", ";
          generate_expression(bounds.high_.expr_, NOT_USER_FACING, o);
          o << ");" << EOL;
        }
        write_end_loop(ar_lens.size(), indent, o);
      } else if (btype.is_specialized()) {
        write_begin_array_dims_loop(decl, true, indent, o);
        generate_indent(indent + ar_lens.size(), o);
        o << "stan::math::check_";
        // kludge - inconsistent naming specialized cholesky_factor types
        if (btype.name() == "cholesky_factor_cov")
          o << "cholesky_factor";
        else
          o << btype.name();
        o << "(function__, \"" << var_name;
        write_var_idx_array_dims(ar_lens.size(), o);
        o << "\", " << var_name;
        write_var_idx_array_dims(ar_lens.size(), o);
        o << ");" << EOL;
        write_end_loop(ar_lens.size(), indent, o);
      }
    }
  }
}
#endif
