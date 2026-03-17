#ifndef STAN_LANG_GENERATOR_GENERATE_DIMS_METHOD_HPP
#define STAN_LANG_GENERATOR_GENERATE_DIMS_METHOD_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>
#include <vector>

namespace stan {
  namespace lang {

    void generate_var_dims(const block_var_decl& decl, std::ostream& o) {
      o << INDENT2 << "dims__.resize(0);" << EOL;
      std::vector<expression> ar_lens = decl.type().array_lens();
      for (size_t i = 0; i < ar_lens.size(); ++i) {
        o << INDENT2 << "dims__.push_back(";
        generate_expression(ar_lens[i], NOT_USER_FACING, o);
        o << ");" << EOL;
      }
      if (!is_nil(decl.type().innermost_type().arg1())) {
        o << INDENT2 << "dims__.push_back(";
        generate_expression(decl.type().innermost_type().arg1(),
                            NOT_USER_FACING, o);
        o << ");" << EOL;
      }
      if (!is_nil(decl.type().innermost_type().arg2())) {
        o << INDENT2 << "dims__.push_back(";
        generate_expression(decl.type().innermost_type().arg2(),
                            NOT_USER_FACING, o);
        o << ");" << EOL;
      }
      o << INDENT2 << "dimss__.push_back(dims__);" << EOL;
    }

    /**
     * Generate the <code>get_dims</code> method for the parameters,
     * transformed parameters, and generated quantities, using the
     * specified program and generating to the specified stream.
     *
     * @param[in] prog program from which to generate
     * @param[in,out] o stream for generating
     */
    void generate_dims_method(const program& prog, std::ostream& o) {
      o << EOL << INDENT
        << "void get_dims(std::vector<std::vector<size_t> >& dimss__) const {"
        << EOL;
      o << INDENT2 << "dimss__.resize(0);" << EOL;
      o << INDENT2 << "std::vector<size_t> dims__;" << EOL;
      for (size_t i = 0; i < prog.parameter_decl_.size(); ++i)
        generate_var_dims(prog.parameter_decl_[i], o);
      for (size_t i = 0; i < prog.derived_decl_.first.size(); ++i)
        generate_var_dims(prog.derived_decl_.first[i], o);
      for (size_t i = 0; i < prog.generated_decl_.first.size(); ++i)
        generate_var_dims(prog.generated_decl_.first[i], o);
      o << INDENT << "}" << EOL2;
    }

  }
}
#endif
