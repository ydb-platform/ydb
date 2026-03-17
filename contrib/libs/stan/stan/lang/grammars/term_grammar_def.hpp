#ifndef STAN_LANG_GRAMMARS_TERM_GRAMMAR_DEF_HPP
#define STAN_LANG_GRAMMARS_TERM_GRAMMAR_DEF_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/expression_grammar.hpp>
#include <stan/lang/grammars/indexes_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <stan/lang/grammars/term_grammar.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/phoenix/phoenix.hpp>
#include <boost/version.hpp>
#include <string>
#include <sstream>
#include <vector>

BOOST_FUSION_ADAPT_STRUCT(stan::lang::index_op,
                          (stan::lang::expression, expr_)
                          (std::vector<std::vector<stan::lang::expression> >,
                           dimss_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::index_op_sliced,
                          (stan::lang::expression, expr_)
                          (std::vector<stan::lang::idx>, idxs_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::integrate_1d,
                          (std::string, function_name_)
                          (stan::lang::expression, lb_)
                          (stan::lang::expression, ub_)
                          (stan::lang::expression, theta_)
                          (stan::lang::expression, x_r_)
                          (stan::lang::expression, x_i_)
                          (stan::lang::expression, rel_tol_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::integrate_ode,
                          (std::string, integration_function_name_)
                          (std::string, system_function_name_)
                          (stan::lang::expression, y0_)
                          (stan::lang::expression, t0_)
                          (stan::lang::expression, ts_)
                          (stan::lang::expression, theta_)
                          (stan::lang::expression, x_)
                          (stan::lang::expression, x_int_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::integrate_ode_control,
                          (std::string, integration_function_name_)
                          (std::string, system_function_name_)
                          (stan::lang::expression, y0_)
                          (stan::lang::expression, t0_)
                          (stan::lang::expression, ts_)
                          (stan::lang::expression, theta_)
                          (stan::lang::expression, x_)
                          (stan::lang::expression, x_int_)
                          (stan::lang::expression, rel_tol_)
                          (stan::lang::expression, abs_tol_)
                          (stan::lang::expression, max_num_steps_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::algebra_solver,
                           (std::string, system_function_name_)
                           (stan::lang::expression, y_)
                           (stan::lang::expression, theta_)
                           (stan::lang::expression, x_r_)
                           (stan::lang::expression, x_i_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::algebra_solver_control,
                           (std::string, system_function_name_)
                           (stan::lang::expression, y_)
                           (stan::lang::expression, theta_)
                           (stan::lang::expression, x_r_)
                           (stan::lang::expression, x_i_)
                           (stan::lang::expression, rel_tol_)
                           (stan::lang::expression, fun_tol_)
                           (stan::lang::expression, max_num_steps_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::map_rect,
                          (std::string, fun_name_)
                          (stan::lang::expression, shared_params_)
                          (stan::lang::expression, job_params_)
                          (stan::lang::expression, job_data_r_)
                          (stan::lang::expression, job_data_i_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::fun,
                          (std::string, name_)
                          (std::vector<stan::lang::expression>, args_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::array_expr,
                          (std::vector<stan::lang::expression>, args_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::row_vector_expr,
                          (std::vector<stan::lang::expression>, args_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::int_literal,
                          (int, val_)
                          (stan::lang::bare_expr_type, type_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::double_literal,
                          (double, val_)
                          (stan::lang::bare_expr_type, type_) )


namespace stan {

  namespace lang {

    template <typename Iterator>
    term_grammar<Iterator>::term_grammar(variable_map& var_map,
                                         std::stringstream& error_msgs,
                                         expression_grammar<Iterator>& eg)
      : term_grammar::base_type(term_r),
        var_map_(var_map),
        error_msgs_(error_msgs),
        expression_g(eg),
        indexes_g(var_map, error_msgs, eg) {
      using boost::spirit::qi::_1;
      using boost::spirit::qi::_a;
      using boost::spirit::qi::_b;
      using boost::spirit::qi::_c;
      using boost::spirit::qi::_d;
      using boost::spirit::qi::char_;
      using boost::spirit::qi::double_;
      using boost::spirit::qi::eps;
      using boost::spirit::qi::int_;
      using boost::spirit::qi::hold;
      using boost::spirit::qi::lexeme;
      using boost::spirit::qi::lit;
      using boost::spirit::qi::no_skip;
      using boost::spirit::qi::string;
      using boost::spirit::qi::_pass;
      using boost::spirit::qi::_val;
      using boost::spirit::qi::raw;

      using boost::spirit::qi::labels::_r1;

      using boost::phoenix::begin;
      using boost::phoenix::end;

      term_r.name("expression");
      term_r
        = (negated_factor_r(_r1)[assign_lhs_f(_val, _1)]
            >> *((lit('*') > negated_factor_r(_r1)
                             [multiplication_f(_val, _1,
                                           boost::phoenix::ref(error_msgs_))])
                 | (lit('/') > negated_factor_r(_r1)
                               [division_f(_val, _1,
                                           boost::phoenix::ref(error_msgs_))])
                 | (lit('%') > negated_factor_r(_r1)
                               [modulus_f(_val, _1, _pass,
                                          boost::phoenix::ref(error_msgs_))])
                 | (lit('\\') > negated_factor_r(_r1)
                                [left_division_f(_val, _pass, _1,
                                         boost::phoenix::ref(error_msgs_))])
                 | (lit(".*") > negated_factor_r(_r1)
                                [elt_multiplication_f(_val, _1,
                                          boost::phoenix::ref(error_msgs_))])
                 | (lit("./") > negated_factor_r(_r1)
                                [elt_division_f(_val, _1,
                                        boost::phoenix::ref(error_msgs_))])));

      negated_factor_r.name("expression");
      negated_factor_r
        = lit('-') >> negated_factor_r(_r1)
                      [negate_expr_f(_val, _1, _pass,
                                     boost::phoenix::ref(error_msgs_))]
        | lit('!') >> negated_factor_r(_r1)
                      [logical_negate_expr_f(_val, _1,
                                             boost::phoenix::ref(error_msgs_))]
        | lit('+') >> negated_factor_r(_r1)[assign_lhs_f(_val, _1)]
        | exponentiated_factor_r(_r1)[assign_lhs_f(_val, _1)];

      exponentiated_factor_r.name("expression");
      exponentiated_factor_r
        = idx_factor_r(_r1)[assign_lhs_f(_val, _1)]
        >> -(lit('^')
             > negated_factor_r(_r1)
               [exponentiation_f(_val, _1, _r1, _pass,
                                 boost::phoenix::ref(error_msgs_))]);

      idx_factor_r.name("expression");
      idx_factor_r
        =  factor_r(_r1)[assign_lhs_f(_val, _1)]
        > *( ( (+dims_r(_r1))[assign_lhs_f(_a, _1)]
               > eps
               [add_expression_dimss_f(_val, _a, _pass,
                                       boost::phoenix::ref(error_msgs_) )] )
            | (indexes_g(_r1)[assign_lhs_f(_b, _1)]
               > eps[add_idxs_f(_val, _b, _pass,
                              boost::phoenix::ref(error_msgs_))])
            | (lit("'")
               > eps[transpose_f(_val, _pass,
                                 boost::phoenix::ref(error_msgs_))]) );

      integrate_ode_control_r.name("expression");
      integrate_ode_control_r
        %= ( (string("integrate_ode_rk45") >> no_skip[!char_("a-zA-Z0-9_")])
             | (string("integrate_ode_bdf") >> no_skip[!char_("a-zA-Z0-9_")])
             | (string("integrate_ode_adams") >> no_skip[!char_("a-zA-Z0-9_")]))
        >> lit('(')              // >> allows backtracking to non-control
        >> identifier_r          // 1) system function name (function only)
        >> lit(',')
        >> expression_g(_r1)     // 2) y0
        >> lit(',')
        >> expression_g(_r1)     // 3) t0 (data only)
        >> lit(',')
        >> expression_g(_r1)     // 4) ts (data only)
        >> lit(',')
        >> expression_g(_r1)     // 5) theta
        >> lit(',')
        >> expression_g(_r1)     // 6) x (data only)
        >> lit(',')
        >> expression_g(_r1)     // 7) x_int (data only)
        >> lit(',')
        >> expression_g(_r1)     // 8) relative tolerance (data only)
        >> lit(',')
        >> expression_g(_r1)     // 9) absolute tolerance (data only)
        >> lit(',')
        >> expression_g(_r1)     // 10) maximum number of steps (data only)
        > lit(')')
          [validate_integrate_ode_control_f(_val, boost::phoenix::ref(var_map_),
                                            _pass,
                                            boost::phoenix::ref(error_msgs_))];

      integrate_ode_r.name("expression");
      integrate_ode_r
        %= ( (string("integrate_ode_rk45") >> no_skip[!char_("a-zA-Z0-9_")])
             | (string("integrate_ode_bdf") >> no_skip[!char_("a-zA-Z0-9_")])
             | (string("integrate_ode_adams") >> no_skip[!char_("a-zA-Z0-9_")])
             | (string("integrate_ode") >> no_skip[!char_("a-zA-Z0-9_")])
               [deprecated_integrate_ode_f(boost::phoenix::ref(error_msgs_))] )
        > lit('(')
        > identifier_r          // 1) system function name (function only)
        > lit(',')
        > expression_g(_r1)     // 2) y0
        > lit(',')
        > expression_g(_r1)     // 3) t0 (data only)
        > lit(',')
        > expression_g(_r1)     // 4) ts (data only)
        > lit(',')
        > expression_g(_r1)     // 5) theta
        > lit(',')
        > expression_g(_r1)     // 6) x (data only)
        > lit(',')
        > expression_g(_r1)     // 7) x_int (data only)
        > lit(')')
          [validate_integrate_ode_f(_val, boost::phoenix::ref(var_map_),
                                    _pass, boost::phoenix::ref(error_msgs_))];

      algebra_solver_control_r.name("expression");
      algebra_solver_control_r
        %= lit("algebra_solver")
        >> lit('(')
        >> identifier_r          // 1) system function name (function only)
        >> lit(',')
        >> expression_g(_r1)     // 2) y
        >> lit(',')
        >> expression_g(_r1)     // 3) theta
        >> lit(',')
        >> expression_g(_r1)     // 4) x_r (data only)
        >> lit(',')
        >> expression_g(_r1)     // 5) x_i (data only)
        >> lit(',')
        >> expression_g(_r1)     // 6) relative tolerance (data only)
        >> lit(',')
        >> expression_g(_r1)     // 7) function tolerance (data only)
        >> lit(',')
        >> expression_g(_r1)     // 8) maximum number of steps (data only)
        > lit(')')
          [validate_algebra_solver_control_f(_val,
                                             boost::phoenix::ref(var_map_),
                                             _pass,
                                             boost::phoenix::ref(error_msgs_))];

      algebra_solver_r.name("expression");
      algebra_solver_r
        %= (lit("algebra_solver") >> no_skip[!char_("a-zA-Z0-9_")])
        > lit('(')
        > identifier_r          // 1) system function name (function only)
        > lit(',')
        > expression_g(_r1)     // 2) y
        > lit(',')
        > expression_g(_r1)     // 3) theta
        > lit(',')
        > expression_g(_r1)     // 4) x_r (data only)
        > lit(',')
        > expression_g(_r1)     // 5) x_i (data only)
        > lit(')')
          [validate_algebra_solver_f(_val, boost::phoenix::ref(var_map_),
                                     _pass, boost::phoenix::ref(error_msgs_))];

      map_rect_r.name("map_rect");
      map_rect_r
          %= (lit("map_rect") >> no_skip[!char_("a-zA-Z0-9_")])
          > lit('(')
          > identifier_r          // 1) mapped function name
          > lit(',')
          > expression_g(_r1)     // 2) shared param vector
          > lit(',')
          > expression_g(_r1)     // 3) job-specific param vector
          > lit(',')
          > expression_g(_r1)     // 4) job-specific real data vector
          > lit(',')
          > expression_g(_r1)     // 4) job-specific integer data vector
          > lit(')')
          [validate_map_rect_f(_val, boost::phoenix::ref(var_map_),
                               _pass, boost::phoenix::ref(error_msgs_))];

      integrate_1d_r.name("integrate_1d");
      integrate_1d_r
          %= (lit("integrate_1d") >> no_skip[!char_("a-zA-Z0-9_")])
          > lit('(')
          > identifier_r          // 1) integrated function name
          > lit(',')
          > expression_g(_r1)     // 2) integration lower bound
          > lit(',')
          > expression_g(_r1)     // 3) integration upper bound
          > lit(',')
          > expression_g(_r1)     // 4) parameters
          > lit(',')
          > expression_g(_r1)     // 5) real data
          > lit(',')
          > expression_g(_r1)     // 6) integer data
          > lit(',')
          > expression_g(_r1)     // 7) relative tolerance
          > lit(')')
          [validate_integrate_1d_f(_val, boost::phoenix::ref(var_map_),
                                   _pass, boost::phoenix::ref(error_msgs_))];

      factor_r.name("expression");
      factor_r =
          integrate_1d_r(_r1)[assign_lhs_f(_val, _1)]
        | integrate_ode_control_r(_r1)[assign_lhs_f(_val, _1)]
        | integrate_ode_r(_r1)[assign_lhs_f(_val, _1)]
        | algebra_solver_control_r(_r1)[assign_lhs_f(_val, _1)]
        | algebra_solver_r(_r1)[assign_lhs_f(_val, _1)]
        | map_rect_r(_r1)[assign_lhs_f(_val, _1)]
        | (fun_r(_r1)[assign_lhs_f(_b, _1)]
           > eps[set_fun_type_named_f(_val, _b, _r1, _pass,
                                      boost::phoenix::ref(var_map_),
                                      boost::phoenix::ref(error_msgs_))])
        | (variable_r[assign_lhs_f(_a, _1)]
           > eps[set_var_type_f(_a, _val, boost::phoenix::ref(var_map_),
                                boost::phoenix::ref(error_msgs_),
                                _pass)])
        | int_literal_r[assign_lhs_f(_val, _1)]
        | str_double_literal_r[assign_lhs_f(_val, _1)]
        | (array_expr_r(_r1)[assign_lhs_f(_c, _1)]
           > eps[infer_array_expr_type_f(_val, _c, _r1, _pass,
                                       boost::phoenix::ref(var_map_),
                                       boost::phoenix::ref(error_msgs_))])
        | (vec_expr_r(_r1)[assign_lhs_f(_d, _1)]
           > eps[infer_vec_or_matrix_expr_type_f(_val, _d, _r1, _pass,
                                     boost::phoenix::ref(var_map_),
                                     boost::phoenix::ref(error_msgs_))])
        | (lit('(')
           > expression_g(_r1)[assign_lhs_f(_val, _1)]
           > lit(')'));


      str_double_literal_r.name("double literal");
      str_double_literal_r
        = raw[double_literal_r][add_literal_string_f(_val, begin(_1), end(_1))];

      int_literal_r.name("integer literal");
      int_literal_r
        %= int_
        >> !(lit('.') | lit('e') | lit('E'));

      double_literal_r.name("real literal");
      double_literal_r
        %= double_;

      fun_r.name("function and argument expressions");
      fun_r
        %= (hold[identifier_r[is_prob_fun_f(_1, _pass)]]
            >> &lit('(')
            > prob_args_r(_r1))
        | (identifier_r >> args_r(_r1));

      identifier_r.name("identifier");
      identifier_r
        %= lexeme[char_("a-zA-Z")
                  >> *char_("a-zA-Z0-9_.")];

      prob_args_r.name("probability function argument");
      prob_args_r
        %= (lit('(') >> lit(')'))
        | hold[lit('(')
               >> expression_g(_r1)
               >> lit(')')]
        | (lit('(')
           >> expression_g(_r1)
           >> (lit(',')
               [require_vbar_f(_pass, boost::phoenix::ref(error_msgs_))]
               | (eps > lit('|')))
           >> (expression_g(_r1) % ',')
           >> lit(')'));

      args_r.name("function arguments");
      args_r
        %= (lit('(') >> lit(')'))
        | (lit('(') >> (expression_g(_r1) % ',') >> lit(')'));

      dim_r.name("array dimension (integer expression)");
      dim_r
        %= expression_g(_r1)
        >> eps[validate_int_expr_silent_f(_val, _pass)];

      dims_r.name("array dimensions");
      dims_r
        %= lit('[')
        >> (dim_r(_r1)
           % ',' )
        >> lit(']');

      variable_r.name("variable name");
      variable_r
        %= identifier_r
        > !lit('(');    // negative lookahead to prevent failure in
                        // fun to try to evaluate as variable [cleaner
                        // error msgs]

      array_expr_r.name("array expression");
      array_expr_r
        %=  lit('{')
        >> expression_g(_r1) % ','
        >> lit('}');

      vec_expr_r.name("row vector or matrix expression");
      vec_expr_r
        %=  lit('[')
        >> expression_g(_r1) % ','
        >> lit(']');
    }
  }
}
#endif
