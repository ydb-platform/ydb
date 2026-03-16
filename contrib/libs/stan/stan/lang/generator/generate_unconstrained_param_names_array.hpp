#ifndef STAN_LANG_GENERATOR_GENERATE_UNCONSTRAINED_PARAM_NAMES_ARRAY_HPP
#define STAN_LANG_GENERATOR_GENERATE_UNCONSTRAINED_PARAM_NAMES_ARRAY_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_expression.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/get_block_var_dims.hpp>
#include <stan/lang/generator/write_begin_param_elements_loop.hpp>
#include <stan/lang/generator/write_end_loop.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate the sequence of names for all elements of a parameter variable.
     *
     * @param[in] indent level of indentation
     * @param[in,out] o stream for generating
     * @param[in] var_decl block variable declaraton ast node
     */
    void
    generate_unconstrained_param_names_array(size_t indent, std::ostream& o,
                                             const block_var_decl& var_decl) {
      std::string var_name(var_decl.name());
      block_var_type vtype = var_decl.type();
      size_t num_array_dims = vtype.array_dims();
      size_t num_vector_dims = vtype.num_dims() - vtype.array_dims();
      if (var_decl.type().innermost_type().is_specialized()) {
        num_vector_dims = 1;
      }
      size_t num_loops = vtype.array_dims() + num_vector_dims;

      write_begin_param_elements_loop(var_decl, true, indent, o);

      generate_indent(indent + num_loops, o);
      o << "param_name_stream__.str(std::string());" << EOL;

      generate_indent(indent + num_loops, o);
      o << "param_name_stream__ << "
        << '"' << var_name << '"';

      for (size_t i = 0; i < num_array_dims; ++i)
        o << " << '.' << k_" << i << "__ + 1";
      if (num_vector_dims == 1)
        o << " << '.' << j_1__ + 1";
      else if (num_vector_dims == 2)
        o << " << '.' << j_1__ + 1 << '.' << j_2__ + 1";
      o << ';' << EOL;

      generate_indent(indent + num_loops, o);
      o << "param_names__.push_back(param_name_stream__.str());" << EOL;

      write_end_loop(num_loops, indent, o);
    }

  }
}
#endif
