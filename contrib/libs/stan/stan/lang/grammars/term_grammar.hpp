#ifndef STAN_LANG_GRAMMARS_TERM_GRAMMAR_HPP
#define STAN_LANG_GRAMMARS_TERM_GRAMMAR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/expression_grammar.hpp>
#include <stan/lang/grammars/indexes_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <boost/spirit/include/qi.hpp>
#include <string>
#include <sstream>
#include <vector>

namespace stan {

  namespace lang {

    template <typename Iterator>
    struct expression_grammar;

    template <typename Iterator>
    struct indexes_grammar;

    template <typename Iterator>
    struct term_grammar
      : public boost::spirit::qi::grammar<Iterator,
                                          expression(scope),
                                          whitespace_grammar<Iterator> > {
      term_grammar(variable_map& var_map, std::stringstream& error_msgs,
                   expression_grammar<Iterator>& eg);

      variable_map& var_map_;
      std::stringstream& error_msgs_;
      expression_grammar<Iterator>& expression_g;
      indexes_grammar<Iterator> indexes_g;

      boost::spirit::qi::rule<Iterator,
                              std::vector<expression>(scope),
                              whitespace_grammar<Iterator> >
      args_r;

      boost::spirit::qi::rule<Iterator,
                              array_expr(scope),
                              whitespace_grammar<Iterator> >
      array_expr_r;

      boost::spirit::qi::rule<Iterator,
                              row_vector_expr(scope),
                              whitespace_grammar<Iterator> >
      vec_expr_r;

      boost::spirit::qi::rule<Iterator,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      dim_r;

      boost::spirit::qi::rule<Iterator,
                              std::vector<expression>(scope),
                              whitespace_grammar<Iterator> >
      dims_r;


      boost::spirit::qi::rule<Iterator,
                              double_literal(),
                              whitespace_grammar<Iterator> >
      double_literal_r;

      boost::spirit::qi::rule<Iterator,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      exponentiated_factor_r;


      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<variable, fun,
                                                        array_expr,
                                                        row_vector_expr>,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      factor_r;


      boost::spirit::qi::rule<Iterator,
                              fun(scope),
                              whitespace_grammar<Iterator> >
      fun_r;

      boost::spirit::qi::rule<Iterator,
                              integrate_ode(scope),
                              whitespace_grammar<Iterator> >
      integrate_ode_r;

      boost::spirit::qi::rule<Iterator,
                              integrate_ode_control(scope),
                              whitespace_grammar<Iterator> >
      integrate_ode_control_r;

      boost::spirit::qi::rule<Iterator,
                              algebra_solver(scope),
                              whitespace_grammar<Iterator> >
      algebra_solver_r;

      boost::spirit::qi::rule<Iterator,
                              algebra_solver_control(scope),
                              whitespace_grammar<Iterator> >
      algebra_solver_control_r;

      boost::spirit::qi::rule<Iterator,
                              map_rect(scope),
                              whitespace_grammar<Iterator> >
      map_rect_r;

      boost::spirit::qi::rule<Iterator,
                              integrate_1d(scope),
                              whitespace_grammar<Iterator> >
      integrate_1d_r;

      boost::spirit::qi::rule<Iterator,
                              std::string(),
                              whitespace_grammar<Iterator> >
      identifier_r;

      boost::spirit::qi::rule<Iterator,
          expression(scope),
          boost::spirit::qi::locals<std::vector<std::vector<expression> >,
                                    std::vector<idx> >,
          whitespace_grammar<Iterator> >
      idx_factor_r;

      boost::spirit::qi::rule<Iterator,
                              int_literal(),
                              whitespace_grammar<Iterator> >
      int_literal_r;


      boost::spirit::qi::rule<Iterator,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      negated_factor_r;

      boost::spirit::qi::rule<Iterator,
                              std::vector<expression>(scope),
                              whitespace_grammar<Iterator> >
      prob_args_r;

      boost::spirit::qi::rule<Iterator,
                              double_literal(),
                              whitespace_grammar<Iterator> >
      str_double_literal_r;

      boost::spirit::qi::rule<Iterator,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      term_r;


      boost::spirit::qi::rule<Iterator,
                              variable(),
                              whitespace_grammar<Iterator> >
      variable_r;
    };

  }
}

#endif
