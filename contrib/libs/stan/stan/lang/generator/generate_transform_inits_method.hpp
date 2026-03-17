#ifndef STAN_LANG_GENERATOR_GENERATE_TRANSFORM_INITS_METHOD_HPP
#define STAN_LANG_GENERATOR_GENERATE_TRANSFORM_INITS_METHOD_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_bare_type.hpp>
#include <stan/lang/generator/generate_comment.hpp>
#include <stan/lang/generator/generate_initializer.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/generate_validate_context_size.hpp>
#include <stan/lang/generator/generate_validate_var_dims.hpp>
#include <stan/lang/generator/write_begin_all_dims_col_maj_loop.hpp>
#include <stan/lang/generator/write_begin_array_dims_loop.hpp>
#include <stan/lang/generator/write_constraints_fn.hpp>
#include <stan/lang/generator/write_end_loop.hpp>
#include <stan/lang/generator/write_var_idx_all_dims.hpp>
#include <stan/lang/generator/write_var_idx_array_dims.hpp>

#include <iostream>
#include <ostream>
#include <vector>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate the <code>transform_inits</code> method declaration
     * and variable decls.
     *
     * @param[in,out] o stream for generating
     */
    void generate_method_begin(std::ostream& o) {
      o << EOL;
      o << INDENT
        << "void transform_inits(const stan::io::var_context& context__,"
        << EOL;
      o << INDENT << "                     std::vector<int>& params_i__,"
        << EOL;
      o << INDENT << "                     std::vector<double>& params_r__,"
        << EOL;
      o << INDENT << "                     std::ostream* pstream__) const {"
        << EOL;
      o << INDENT2 << "typedef double local_scalar_t__;"
        << EOL;
      o << INDENT2 << "stan::io::writer<double> "
        << "writer__(params_r__, params_i__);"
        << EOL;

      o << INDENT2 << "size_t pos__;" << EOL;
      o << INDENT2 << "(void) pos__; // dummy call to supress warning" << EOL;
      o << INDENT2 << "std::vector<double> vals_r__;" << EOL;
      o << INDENT2 << "std::vector<int> vals_i__;" << EOL;
    }

    /**
     * Generate the <code>transform_inits</code> method declaration final statements
     * and close.
     *
     * @param[in,out] o stream for generating
     */
    void generate_method_end(std::ostream& o) {
      o << INDENT2 << "params_r__ = writer__.data_r();" << EOL;
      o << INDENT2 << "params_i__ = writer__.data_i();" << EOL;
      o << INDENT << "}" << EOL2;

      o << INDENT
        << "void transform_inits(const stan::io::var_context& context," << EOL;
      o << INDENT
        << "                     "
        << "Eigen::Matrix<double, Eigen::Dynamic, 1>& params_r," << EOL;
      o << INDENT
        << "                     std::ostream* pstream__) const {" << EOL;
      o << INDENT << "  std::vector<double> params_r_vec;" << EOL;
      o << INDENT << "  std::vector<int> params_i_vec;" << EOL;
      o << INDENT
        << "  transform_inits(context, params_i_vec, params_r_vec, pstream__);"
        << EOL;
      o << INDENT << "  params_r.resize(params_r_vec.size());" << EOL;
      o << INDENT << "  for (int i = 0; i < params_r.size(); ++i)" << EOL;
      o << INDENT << "    params_r(i) = params_r_vec[i];" << EOL;
      o << INDENT << "}" << EOL2;
    }

    /**
     * Generate the <code>transform_inits</code> method for the
     * specified parameter variable declarations to the specified stream.
     *
     * @param[in] vs variable declarations
     * @param[in,out] o stream for generating
     */
    void generate_transform_inits_method(const std::vector<block_var_decl>& vs,
                                         std::ostream& o) {
      int indent = 2;

      generate_method_begin(o);
      o << EOL;

      for (size_t i = 0; i < vs.size(); ++i) {
        std::string var_name(vs[i].name());
        block_var_type vtype = vs[i].type();
        block_var_type el_type = vs[i].type().innermost_type();

        // parser prevents this from happening -  double check to
        // avoid generating code that won't compile - flag/ignore int params
        if (vs[i].bare_type().is_int_type()) {
          std::stringstream ss;
          ss << "Found int-valued param: " << var_name
             << "; illegal - params must be real-valued" << EOL;
          generate_comment(ss.str(), indent, o);
          continue;
        }

        generate_indent(indent, o);
        o << "current_statement_begin__ = " <<  vs[i].begin_line_ << ";"
          << EOL;

        // check context
        generate_indent(indent, o);
        o << "if (!(context__.contains_r(\""
          << var_name << "\")))" << EOL;
        generate_indent(indent + 1, o);
        o << "stan::lang::rethrow_located("
          << "std::runtime_error(std::string(\"Variable "
          << var_name
          << " missing\")), current_statement_begin__, prog_reader__());"
          << EOL;
        // init context position
        generate_indent(indent, o);
        o << "vals_r__ = context__.vals_r(\""
          << var_name << "\");" << EOL;
        generate_indent(indent, o);
        o << "pos__ = 0U;" << EOL;

        // validate dims, match against input sizes
        generate_validate_var_dims(vs[i], indent, o);
        generate_validate_context_size(vs[i], "parameter initialization",
                                       indent, o);

        // instantiate
        generate_indent(indent, o);
        generate_bare_type(vtype.bare_type(), "double", o);
        o << " " << var_name;
        if (vtype.num_dims() == 0) {
          o << "(0);" << EOL;
        } else {
          generate_initializer(vs[i].type(), "double", o);
          o << ";" << EOL;
        }

        // fill vals_r__ loop
        write_begin_all_dims_col_maj_loop(vs[i], true, indent, o);
        generate_indent(indent + vtype.num_dims(), o);
        o << var_name;
        write_var_idx_all_dims(vtype.array_dims(),
                               vtype.num_dims() - vtype.array_dims(),
                               o);
        o << " = vals_r__[pos__++];" << EOL;
        write_end_loop(vtype.num_dims(), indent, o);

        // unconstrain var contents
        write_begin_array_dims_loop(vs[i], true, indent, o);
        generate_indent(indent + vtype.array_dims(), o);
        o << "try {" << EOL;

        generate_indent(indent + vtype.array_dims() + 1, o);
        o << "writer__." << write_constraints_fn(el_type, "unconstrain");
        o << var_name;
        write_var_idx_array_dims(vtype.array_dims(), o);
        o << ");" << EOL;
        generate_indent(indent + vtype.array_dims(), o);
        o << "} catch (const std::exception& e) {" << EOL;
        generate_indent(indent + vtype.array_dims() + 1, o);
        o << "stan::lang::rethrow_located("
          << "std::runtime_error(std::string(\"Error transforming variable "
          << var_name
          << ": \") + e.what()), current_statement_begin__, prog_reader__());"
          << EOL;
        generate_indent(indent + vtype.array_dims(), o);
        o << "}" << EOL;
        write_end_loop(vtype.array_dims(), indent, o);

        // all done, add blank line for readibility
        o << EOL;
      }
      generate_method_end(o);
    }

  }
}
#endif
