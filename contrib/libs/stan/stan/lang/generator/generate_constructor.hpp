#ifndef STAN_LANG_GENERATOR_GENERATE_CONSTRUCTOR_HPP
#define STAN_LANG_GENERATOR_GENERATE_CONSTRUCTOR_HPP

#include <stan/io/program_reader.hpp>
#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_data_var_ctor.hpp>
#include <stan/lang/generator/generate_data_var_init.hpp>
#include <stan/lang/generator/generate_catch_throw_located.hpp>
#include <stan/lang/generator/generate_comment.hpp>
#include <stan/lang/generator/generate_set_param_ranges.hpp>
#include <stan/lang/generator/generate_statements.hpp>
#include <stan/lang/generator/generate_try.hpp>
#include <stan/lang/generator/generate_validate_context_size.hpp>
#include <stan/lang/generator/generate_validate_var_decl.hpp>
#include <stan/lang/generator/generate_validate_var_dims.hpp>
#include <stan/lang/generator/generate_var_fill_define.hpp>
#include <stan/lang/generator/generate_void_statement.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate the constructor method initial boilerplate.
     *
     * @param[in] model_name name of model for class name
     * @param[in,out] o stream for generating
     */
    void generate_method_begin(const std::string& model_name, std::ostream& o) {
      // constructor without seed or template parameter
      o << INDENT << model_name << "(stan::io::var_context& context__," << EOL;
      o << INDENT << "    std::ostream* pstream__ = 0)" << EOL;
      o << INDENT2 << ": prob_grad(0) {" << EOL;
      o << INDENT2 << "ctor_body(context__, 0, pstream__);" << EOL;
      o << INDENT << "}" << EOL2;
      // constructor with specified seed
      o << INDENT << model_name << "(stan::io::var_context& context__," << EOL;
      o << INDENT << "    unsigned int random_seed__," << EOL;
      o << INDENT << "    std::ostream* pstream__ = 0)" << EOL;
      o << INDENT2 << ": prob_grad(0) {" << EOL;
      o << INDENT2 << "ctor_body(context__, random_seed__, pstream__);" << EOL;
      o << INDENT << "}" << EOL2;
      // body of constructor now in function
      o << INDENT << "void ctor_body(stan::io::var_context& context__," << EOL;
      o << INDENT << "               unsigned int random_seed__," << EOL;
      o << INDENT << "               std::ostream* pstream__) {" << EOL;
      o << INDENT2 << "typedef double local_scalar_t__;" << EOL2;

      o << INDENT2 << "boost::ecuyer1988 base_rng__ =" << EOL;
      o << INDENT2 << "  stan::services::util::create_rng(random_seed__, 0);"
        << EOL;
      o << INDENT2 << "(void) base_rng__;  // suppress unused var warning"
        << EOL2;
      o << INDENT2 << "current_statement_begin__ = -1;" << EOL2;

      o << INDENT2 << "static const char* function__ = \""
        << model_name << "_namespace::" << model_name << "\";" << EOL;
      generate_void_statement("function__", 2, o);
      o << INDENT2 << "size_t pos__;" << EOL;
      generate_void_statement("pos__", 2, o);
      o << INDENT2 << "std::vector<int> vals_i__;" << EOL;
      o << INDENT2 << "std::vector<double> vals_r__;" << EOL;
      o << INDENT2
        << "local_scalar_t__ DUMMY_VAR__"
        << "(std::numeric_limits<double>::quiet_NaN());"
        << EOL;
      o << INDENT2 << "(void) DUMMY_VAR__;  // suppress unused var warning"
        << EOL2;
    }

    /**
     * Generate the constructors for the specified program with the
     * specified model name to the specified stream.
     *
     * @param[in] prog program from which to generate
     * @param[in] model_name name of model for class name
     * @param[in,out] o stream for generating
     */
    void generate_constructor(const program& prog,
                              const std::string& model_name, std::ostream& o) {
      generate_method_begin(model_name, o);

      generate_try(2, o);
      generate_comment("initialize data block variables from context__", 3, o);
      // todo:  bundle into single function
      for (size_t i = 0; i < prog.data_decl_.size(); ++i) {
        generate_indent(3, o);
        o << "current_statement_begin__ = "
          <<  prog.data_decl_[i].begin_line_ << ";"
          << EOL;
        generate_validate_var_dims(prog.data_decl_[i], 3, o);
        generate_validate_context_size(prog.data_decl_[i],
                                       "data initialization", 3, o);
        generate_data_var_ctor(prog.data_decl_[i], 3, o);
        generate_data_var_init(prog.data_decl_[i], 3, o);
        generate_validate_var_decl(prog.data_decl_[i], 3, o);
        o << EOL;
      }
      o << EOL;

      generate_comment("initialize transformed data variables", 3, o);
      // todo:  bundle into single function
      for (size_t i = 0; i < prog.derived_data_decl_.first.size(); ++i) {
        generate_indent(3, o);
        o << "current_statement_begin__ = "
          <<  prog.derived_data_decl_.first[i].begin_line_ << ";" << EOL;
        generate_validate_var_dims(prog.derived_data_decl_.first[i], 3, o);
        generate_data_var_ctor(prog.derived_data_decl_.first[i], 3, o);
        generate_var_fill_define(prog.derived_data_decl_.first[i], 3, o);
        o << EOL;
      }

      generate_comment("execute transformed data statements", 3, o);
      generate_statements(prog.derived_data_decl_.second, 3, o);
      o << EOL;

      generate_comment("validate transformed data", 3, o);
      // todo:  bundle into single function
      for (size_t i = 0; i < prog.derived_data_decl_.first.size(); ++i) {
        if (prog.derived_data_decl_.first[i].type().innermost_type().
            is_constrained()) {
          generate_indent(3, o);
          o << "current_statement_begin__ = "
            <<  prog.derived_data_decl_.first[i].begin_line_ << ";" << EOL;
          generate_validate_var_decl(prog.derived_data_decl_.first[i], 3, o);
          o << EOL;
        }
      }
      o << EOL;

      generate_comment("validate, set parameter ranges", 3, o);
      generate_set_param_ranges(prog.parameter_decl_, 3, o);
      generate_catch_throw_located(2, o);
      o << INDENT << "}" << EOL;
    }

  }
}
#endif
