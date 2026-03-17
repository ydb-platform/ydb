#ifndef STAN_LANG_GENERATOR_GENERATE_LOG_PROB_HPP
#define STAN_LANG_GENERATOR_GENERATE_LOG_PROB_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_bare_type.hpp>
#include <stan/lang/generator/generate_block_var.hpp>
#include <stan/lang/generator/generate_catch_throw_located.hpp>
#include <stan/lang/generator/generate_comment.hpp>
#include <stan/lang/generator/generate_param_var.hpp>
#include <stan/lang/generator/generate_statement.hpp>
#include <stan/lang/generator/generate_statements.hpp>
#include <stan/lang/generator/generate_try.hpp>
#include <stan/lang/generator/generate_validate_tparam_inits.hpp>
#include <stan/lang/generator/generate_validate_var_decl.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the log_prob method for the model class for the
     * specified program on the specified stream.
     *
     * @param prog program node of ast
     * @param o stream for generating
     */
    void generate_log_prob(const program& prog, std::ostream& o) {
      o << EOL;
      o << INDENT << "template <bool propto__, bool jacobian__, typename T__>"
        << EOL;
      o << INDENT << "T__ log_prob(std::vector<T__>& params_r__," << EOL;
      o << INDENT << "             std::vector<int>& params_i__," << EOL;
      o << INDENT << "             std::ostream* pstream__ = 0) const {"
        << EOL2;
      o << INDENT2 << "typedef T__ local_scalar_t__;" << EOL2;

      // use this dummy for inits
      o << INDENT2
        << "local_scalar_t__ DUMMY_VAR__"
        << "(std::numeric_limits<double>::quiet_NaN());"
        << EOL;
      generate_void_statement("DUMMY_VAR__", 2, o);
      o << EOL;

      o << INDENT2 << "T__ lp__(0.0);"
        << EOL;
      o << INDENT2 << "stan::math::accumulator<T__> lp_accum__;"
        << EOL;

      bool gen_local_vars = true;

      generate_try(2, o);
      generate_indent(3, o);
      o << "stan::io::reader<local_scalar_t__> in__(params_r__, params_i__);"
        << EOL2;

      generate_comment("model parameters", 3, o);
      for (size_t i = 0; i < prog.parameter_decl_.size(); ++i) {
        generate_indent(3, o);
        o << "current_statement_begin__ = "
          <<  prog.parameter_decl_[i].begin_line_ << ";"
          << EOL;
        generate_param_var(prog.parameter_decl_[i], gen_local_vars, 3, o);
        o << EOL;
      }

      if (prog.derived_decl_.first.size() > 0) {
        generate_comment("transformed parameters", 3, o);
        for (size_t i = 0; i < prog.derived_decl_.first.size(); ++i) {
          generate_indent(3, o);
          o << "current_statement_begin__ = "
            <<  prog.derived_decl_.first[i].begin_line_ << ";"
            << EOL;
          generate_block_var(prog.derived_decl_.first[i], "local_scalar_t__",
                             3, o);
          o << EOL;
        }
      }

      if (prog.derived_decl_.second.size() > 0) {
        generate_comment("transformed parameters block statements", 3, o);
        generate_statements(prog.derived_decl_.second, 3, o);
        o << EOL;
      }

      if (prog.derived_decl_.first.size() > 0) {
        generate_comment("validate transformed parameters", 3, o);
        o << INDENT3
          << "const char* function__ = \"validate transformed params\";"
          << EOL;
        generate_void_statement("function__", 3, o);
        o << EOL;

        for (size_t i = 0; i < prog.derived_decl_.first.size(); ++i) {
          block_var_decl bvd = prog.derived_decl_.first[i];
          generate_indent(3, o);
          o << "current_statement_begin__ = "
            <<  bvd.begin_line_ << ";" << EOL;
          generate_validate_tparam_inits(bvd, 3, o);
          if (bvd.type().innermost_type().is_constrained()) {
            generate_validate_var_decl(bvd, 3, o);
            o << EOL;
          }
        }
        o << EOL;
      }

      generate_comment("model body", 3, o);
      generate_statement(prog.statement_, 3, o);
      o << EOL;

      generate_catch_throw_located(2, o);

      o << EOL;
      o << INDENT2 << "lp_accum__.add(lp__);" << EOL;
      o << INDENT2 << "return lp_accum__.sum();" << EOL2;
      o << INDENT << "} // log_prob()" << EOL2;

      o << INDENT
        << "template <bool propto, bool jacobian, typename T_>" << EOL;
      o << INDENT
        << "T_ log_prob(Eigen::Matrix<T_,Eigen::Dynamic,1>& params_r," << EOL;
      o << INDENT << "           std::ostream* pstream = 0) const {" << EOL;
      o << INDENT << "  std::vector<T_> vec_params_r;" << EOL;
      o << INDENT << "  vec_params_r.reserve(params_r.size());" << EOL;
      o << INDENT << "  for (int i = 0; i < params_r.size(); ++i)" << EOL;
      o << INDENT << "    vec_params_r.push_back(params_r(i));" << EOL;
      o << INDENT << "  std::vector<int> vec_params_i;" << EOL;
      o << INDENT
        << "  return log_prob<propto,jacobian,T_>(vec_params_r, "
        << "vec_params_i, pstream);" << EOL;
      o << INDENT << "}" << EOL2;
    }

  }
}
#endif
