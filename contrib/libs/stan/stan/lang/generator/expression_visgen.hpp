#ifndef STAN_LANG_GENERATOR_EXPRESSION_VISGEN_HPP
#define STAN_LANG_GENERATOR_EXPRESSION_VISGEN_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_bare_type.hpp>
#include <stan/lang/generator/generate_indexed_expr.hpp>
#include <stan/lang/generator/generate_real_var_type.hpp>
#include <stan/lang/generator/visgen.hpp>
#include <boost/lexical_cast.hpp>
#include <ostream>
#include <string>
#include <vector>

#include <iostream>

namespace stan {
  namespace lang {

    // forward declare recursive helper functions
    void generate_array_builder_adds(const std::vector<expression>& elements,
                                     bool user_facing, std::ostream& o);

    void generate_idxs(const std::vector<idx>& idxs, std::ostream& o);

    void generate_idxs_user(const std::vector<idx>& idxs, std::ostream& o);

    struct expression_visgen : public visgen {
      /**
         true when generated expression might be reported to user
      */
      const bool user_facing_;

      explicit expression_visgen(std::ostream& o, bool user_facing)
        : visgen(o),
          user_facing_(user_facing) {
      }

      void operator()(const nil& /*x*/) const {
        o_ << "nil";
      }

      void operator()(const int_literal& n) const {
        std::string num_str = boost::lexical_cast<std::string>(n.val_);
        o_ << num_str;
      }

      void operator()(const double_literal& x) const {
        o_ << x.string_;
        if (x.string_.find_first_of("eE.") == std::string::npos)
        o_ << ".0";  // trailing 0 to ensure C++ makes it a double
      }

      void operator()(const array_expr& x) const {
        std::stringstream ssRealType;
        generate_real_var_type(x.array_expr_scope_, x.has_var_, ssRealType);

        std::stringstream ssArrayType;
        generate_bare_type(x.type_, ssRealType.str(), ssArrayType);

        std::stringstream ssArrayElType;
        generate_bare_type(x.type_.array_element_type(),
                           ssRealType.str(), ssArrayElType);

        o_ << "static_cast<"
           << ssArrayType.str()
           << " >(stan::math::array_builder<"
           << ssArrayElType.str()
           << " >()";
        generate_array_builder_adds(x.args_, user_facing_, o_);
        o_ << ".array())";
      }

      void operator()(const matrix_expr& x) const {
        std::stringstream ssRealType;
        generate_real_var_type(x.matrix_expr_scope_, x.has_var_, ssRealType);
        // to_matrix arg is std::vector of row vectors (Eigen::Matrix<T, 1, C>)
        o_ << "stan::math::to_matrix(stan::math::array_builder<Eigen::Matrix<"
           << ssRealType.str()
           << ", 1, Eigen::Dynamic> >()";
        generate_array_builder_adds(x.args_, user_facing_, o_);
        o_ << ".array())";
      }

      void operator()(const row_vector_expr& x) const {
        std::stringstream ssRealType;
        generate_real_var_type(x.row_vector_expr_scope_, x.has_var_,
                               ssRealType);
        // to_row_vector arg is std::vector of type T
        o_ << "stan::math::to_row_vector(stan::math::array_builder<"
           << ssRealType.str()
           << " >()";
        generate_array_builder_adds(x.args_, user_facing_, o_);
        o_ << ".array())";
      }

      void operator()(const variable& v) const { o_ << v.name_; }

      void operator()(int n) const {   // NOLINT
        o_ << static_cast<long>(n);    // NOLINT
      }

      void operator()(double x) const { o_ << x; }

      void operator()(const std::string& x) const { o_ << x; }  // identifiers

      void operator()(const index_op& x) const {
        std::stringstream expr_o;
        generate_expression(x.expr_, user_facing_, expr_o);
        std::string expr_string = expr_o.str();
        std::vector<expression> indexes;
        for (size_t i = 0; i < x.dimss_.size(); ++i)
          for (size_t j = 0; j < x.dimss_[i].size(); ++j)
            indexes.push_back(x.dimss_[i][j]);  // wasteful copy, could use refs
        generate_indexed_expr<false>(expr_string, indexes,
                                     x.expr_.bare_type(),
                                     user_facing_, o_);
      }

      void operator()(const index_op_sliced& x) const {
        if (x.idxs_.size() == 0) {
          generate_expression(x.expr_, user_facing_, o_);
          return;
        }
        if (user_facing_) {
          generate_expression(x.expr_, user_facing_, o_);
          generate_idxs_user(x.idxs_, o_);
          return;
        }
        o_ << "stan::model::rvalue(";
        generate_expression(x.expr_, user_facing_, o_);
        o_ << ", ";
        generate_idxs(x.idxs_, o_);
        o_ << ", ";
        o_ << '"';
        generate_expression(x.expr_, USER_FACING, o_);
        o_ << '"';
        o_ << ")";
      }

      void operator()(const integrate_ode& fx) const {
        o_ << (fx.integration_function_name_ == "integrate_ode"
               ? "integrate_ode_rk45"
               : fx.integration_function_name_)
           << "("
           << fx.system_function_name_
           << "_functor__(), ";
        generate_expression(fx.y0_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.t0_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.ts_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.theta_, user_facing_, o_);
        o_ << ", ";
        generate_expression(fx.x_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.x_int_, NOT_USER_FACING, o_);
        o_ << ", pstream__)";
      }

      void operator()(const integrate_ode_control& fx) const {
        o_ << fx.integration_function_name_
           << "("
           << fx.system_function_name_
           << "_functor__(), ";
        generate_expression(fx.y0_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.t0_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.ts_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.theta_, user_facing_, o_);
        o_ << ", ";
        generate_expression(fx.x_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.x_int_, NOT_USER_FACING, o_);
        o_ << ", pstream__, ";
        generate_expression(fx.rel_tol_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.abs_tol_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.max_num_steps_, NOT_USER_FACING, o_);
        o_ << ")";
      }

      void operator()(const algebra_solver& fx) const {
        o_ << "algebra_solver"
           << "("
           << fx.system_function_name_
           << "_functor__(), ";
        generate_expression(fx.y_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.theta_, user_facing_, o_);
        o_ << ", ";
        generate_expression(fx.x_r_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.x_i_, NOT_USER_FACING, o_);
        o_ << ", pstream__)";
      }

      void operator()(const algebra_solver_control& fx) const {
        o_ << "algebra_solver"
           << "("
           << fx.system_function_name_
           << "_functor__(), ";
        generate_expression(fx.y_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.theta_, user_facing_, o_);
        o_ << ", ";
        generate_expression(fx.x_r_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.x_i_, NOT_USER_FACING, o_);
        o_ << ", pstream__, ";
        generate_expression(fx.rel_tol_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.fun_tol_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.max_num_steps_, NOT_USER_FACING, o_);
        o_ << ")";
      }

      void operator()(const map_rect& fx) const {
        o_ << "map_rect";
        o_ << "<" << fx.call_id_ << ", " << fx.fun_name_ << "_functor__>";
        o_ << "(";
        generate_expression(fx.shared_params_, user_facing_, o_);
        o_ << ", ";
        generate_expression(fx.job_params_, user_facing_, o_);
        o_ << ", ";
        generate_expression(fx.job_data_r_, NOT_USER_FACING, o_);
        o_ << ", ";
        generate_expression(fx.job_data_i_, NOT_USER_FACING, o_);
        o_ << ", pstream__)";
      }

      void operator()(const integrate_1d& fx) const {
        o_ << "integrate_1d("
           << fx.function_name_ << "_functor__(), ";
        generate_expression(fx.lb_, user_facing_, o_);
        o_ << ", ";
        generate_expression(fx.ub_, user_facing_, o_);
        o_ << ", ";
        generate_expression(fx.theta_, user_facing_, o_);
        o_ << ", ";
        generate_expression(fx.x_r_, user_facing_, o_);
        o_ << ", ";
        generate_expression(fx.x_i_, user_facing_, o_);
        o_ << ", *pstream__, ";
        generate_expression(fx.rel_tol_, user_facing_, o_);
        o_ << ")";
      }

      void operator()(const fun& fx) const {
        // first test if short-circuit op (binary && and || applied to
        // primitives; overloads are eager, not short-circuiting)
        if (fx.name_ == "logical_or" || fx.name_ == "logical_and") {
          o_ << "(primitive_value(";
          boost::apply_visitor(*this, fx.args_[0].expr_);
          o_ << ") " << ((fx.name_ == "logical_or") ? "||" : "&&")
             << " primitive_value(";
          boost::apply_visitor(*this, fx.args_[1].expr_);
          o_ << "))";
          return;
        }
        o_ << fx.name_ << "(";
        for (size_t i = 0; i < fx.args_.size(); ++i) {
          if (i > 0) o_ << ", ";
          boost::apply_visitor(*this, fx.args_[i].expr_);
        }
        if (fx.args_.size() > 0
            && (has_rng_suffix(fx.name_) || has_lp_suffix(fx.name_)))
          o_ << ", ";
        if (has_rng_suffix(fx.name_))
          o_ << "base_rng__";
        if (has_lp_suffix(fx.name_))
          o_ << "lp__, lp_accum__";
        if (is_user_defined(fx)) {
          if (fx.args_.size() > 0
              || has_rng_suffix(fx.name_)
              || has_lp_suffix(fx.name_))
            o_ << ", ";
          o_ << "pstream__";
        }
        o_ << ")";
      }

      void operator()(const conditional_op& expr) const {
        bool types_prim_match
          = (expr.type_.is_primitive() && expr.type_.is_int_type())
          || (!expr.has_var_ && expr.type_.is_primitive()
              && (expr.true_val_.bare_type()
                  == expr.false_val_.bare_type()));
        std::stringstream ss;
        generate_real_var_type(expr.scope_, expr.has_var_, ss);

        o_ << "(";
        boost::apply_visitor(*this, expr.cond_.expr_);
        o_ << " ? ";
        if (types_prim_match) {
          boost::apply_visitor(*this, expr.true_val_.expr_);
        } else {
          o_ << "stan::math::promote_scalar<"
             << ss.str()
             << ">(";
          boost::apply_visitor(*this, expr.true_val_.expr_);
          o_ << ")";
        }
        o_ << " : ";
        if (types_prim_match) {
          boost::apply_visitor(*this, expr.false_val_.expr_);
        } else {
          o_ << "stan::math::promote_scalar<"
             << ss.str()
             << ">(";
          boost::apply_visitor(*this, expr.false_val_.expr_);
          o_ << ")";
        }
        o_ << " )";
      }

      void operator()(const binary_op& expr) const {
        o_ << "(";
        boost::apply_visitor(*this, expr.left.expr_);
        o_ << " " << expr.op << " ";
        boost::apply_visitor(*this, expr.right.expr_);
        o_ << ")";
      }

      void operator()(const unary_op& expr) const {
        o_ << expr.op << "(";
        boost::apply_visitor(*this, expr.subject.expr_);
        o_ << ")";
      }
    };

  }
}
#endif
