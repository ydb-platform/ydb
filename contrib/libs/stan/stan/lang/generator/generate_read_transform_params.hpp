#ifndef STAN_LANG_GENERATOR_GENERATE_READ_TRANSFORM_PARAMS_HPP
#define STAN_LANG_GENERATOR_GENERATE_READ_TRANSFORM_PARAMS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_bare_type.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/write_constraints_fn.hpp>
#include <stan/lang/generator/write_begin_all_dims_col_maj_loop.hpp>
#include <stan/lang/generator/write_end_loop.hpp>
#include <stan/lang/generator/write_nested_resize_loop_begin.hpp>
#include <stan/lang/generator/write_resize_var_idx.hpp>
#include <stan/lang/generator/write_var_idx_all_dims.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate declarations for parameters on the constrained scale.
     *
     * @param[in] vs variable declarations
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void generate_read_transform_params(const std::vector<block_var_decl>& vs,
                                        int indent, std::ostream& o) {
      for (size_t i = 0; i < vs.size(); ++i) {
        std::string var_name(vs[i].name());
        block_var_type vtype = vs[i].type();
        block_var_type el_type = vtype.innermost_type();

        // declare
        generate_indent(indent, o);
        generate_bare_type(vtype.bare_type(), "double", o);
        o << " " << var_name;

        if (vtype.array_dims() == 0) {
          // read/transform
          o << " = in__."
            << write_constraints_fn(vtype, "constrain")
            << ");" << EOL;

        } else {
          o << ";" << EOL;

          write_nested_resize_loop_begin(var_name, vtype.array_lens(),
                                         indent, o);
          generate_indent(indent + vtype.array_dims(), o);
          o << var_name;
          write_resize_var_idx(vtype.array_dims(), o);
          o << ".push_back(in__."
            << write_constraints_fn(el_type, "constrain")
            << "));" << EOL;
          write_end_loop(vtype.array_dims(), indent, o);
        }

        // write to vars__ in col-major
        write_begin_all_dims_col_maj_loop(vs[i], true, indent, o);

        generate_indent(indent + vtype.num_dims(), o);
        o << "vars__.push_back(" << var_name;
        write_var_idx_all_dims(vtype.array_dims(),
                               vtype.num_dims() - vtype.array_dims(), o);
        o << ");" << EOL;

        write_end_loop(vtype.num_dims(), indent, o);

        o << EOL;
      }
    }

  }
}
#endif
