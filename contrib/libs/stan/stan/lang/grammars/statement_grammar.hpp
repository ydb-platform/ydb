#ifndef STAN_LANG_GRAMMARS_STATEMENT_GRAMMAR_HPP
#define STAN_LANG_GRAMMARS_STATEMENT_GRAMMAR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/expression_grammar.hpp>
#include <stan/lang/grammars/indexes_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <stan/lang/grammars/statement_2_grammar.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <stan/lang/grammars/local_var_decls_grammar.hpp>
#include <boost/spirit/include/qi.hpp>
#include <sstream>
#include <string>
#include <vector>

namespace stan {

  namespace lang {

    template <typename Iterator>
    struct statement_grammar
      : boost::spirit::qi::grammar<Iterator,
                                   statement(scope, bool),
                                   whitespace_grammar<Iterator> > {
      statement_grammar(variable_map& var_map,
                        std::stringstream& error_msgs);

      variable_map& var_map_;
      std::stringstream& error_msgs_;
      expression_grammar<Iterator> expression_g;
      local_var_decls_grammar<Iterator> local_var_decls_g;
      statement_2_grammar<Iterator> statement_2_g;
      indexes_grammar<Iterator> indexes_g;

      boost::spirit::qi::rule<Iterator,
                              assgn(scope),
                              whitespace_grammar<Iterator> >
      assgn_r;

      boost::spirit::qi::rule<Iterator,
                              std::string(),
                              whitespace_grammar<Iterator> >
      assignment_operator_r;

      boost::spirit::qi::rule<Iterator,
                              distribution(scope),
                              whitespace_grammar<Iterator> >
      distribution_r;


      boost::spirit::qi::rule<Iterator,
                              increment_log_prob_statement(scope),
                              whitespace_grammar<Iterator> >
      increment_log_prob_statement_r;

      boost::spirit::qi::rule<Iterator,
                              increment_log_prob_statement(scope),
                              whitespace_grammar<Iterator> >
      increment_target_statement_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<std::string>,
                              for_statement(scope),
                              whitespace_grammar<Iterator> >
      for_statement_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<std::string>,
                              for_array_statement(scope),
                              whitespace_grammar<Iterator> >
      for_array_statement_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<std::string>,
                              for_matrix_statement(scope),
                              whitespace_grammar<Iterator> >
      for_matrix_statement_r;

      boost::spirit::qi::rule<Iterator,
                              while_statement(scope),
                              whitespace_grammar<Iterator> >
      while_statement_r;

      boost::spirit::qi::rule<Iterator,
                              break_continue_statement(bool),  // NOLINT
                              whitespace_grammar<Iterator> >
      break_continue_statement_r;

      boost::spirit::qi::rule<Iterator,
                              print_statement(scope),
                              whitespace_grammar<Iterator> >
      print_statement_r;

      boost::spirit::qi::rule<Iterator,
                              reject_statement(scope),
                              whitespace_grammar<Iterator> >
      reject_statement_r;


      boost::spirit::qi::rule<Iterator,
                              return_statement(scope),
                              whitespace_grammar<Iterator> >
      return_statement_r;

      boost::spirit::qi::rule<Iterator,
                              return_statement(scope),
                              whitespace_grammar<Iterator> >
      void_return_statement_r;

      boost::spirit::qi::rule<Iterator,
                              printable(scope),
                              whitespace_grammar<Iterator> >
      printable_r;

      boost::spirit::qi::rule<Iterator,
                              std::string(),
                              whitespace_grammar<Iterator> >
      printable_string_r;


      boost::spirit::qi::rule<Iterator,
                              std::string(),
                              whitespace_grammar<Iterator> >
      identifier_r;

      boost::spirit::qi::rule<Iterator,
                              std::vector<local_var_decl>(scope),
                              whitespace_grammar<Iterator> >
      local_var_decls_r;

      boost::spirit::qi::rule<Iterator,
                              std::vector<idx>(scope),
                              whitespace_grammar<Iterator> >
      idxs_r;

      boost::spirit::qi::rule<Iterator,
                              no_op_statement(),
                              whitespace_grammar<Iterator> >
      no_op_statement_r;

      boost::spirit::qi::rule<Iterator,
                              std::vector<idx>(scope),
                              whitespace_grammar<Iterator> >
      opt_idxs_r;

      boost::spirit::qi::rule<Iterator,
                              range(scope),
                              whitespace_grammar<Iterator> >
      range_r;

      boost::spirit::qi::rule<Iterator,
                              sample(scope),
                              whitespace_grammar<Iterator> >
      sample_r;

      boost::spirit::qi::rule<Iterator,
                              statement(scope, bool),
                              whitespace_grammar<Iterator> >
      statement_r;

      boost::spirit::qi::rule<Iterator,
                              statement(scope, bool),
                              whitespace_grammar<Iterator> >
      statement_sub_r;

      boost::spirit::qi::rule<Iterator,
             boost::spirit::qi::locals<std::vector<local_var_decl>,
                                       scope>,
                              statements(scope, bool),
                              whitespace_grammar<Iterator> >
      statement_seq_r;

      boost::spirit::qi::rule<Iterator,
                              range(scope),
                              whitespace_grammar<Iterator> >
      truncation_range_r;

      boost::spirit::qi::rule<Iterator,
                              variable(scope),
                              whitespace_grammar<Iterator> >
      var_r;

      boost::spirit::qi::rule<Iterator,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      expression_rhs_r;
    };

  }
}
#endif
