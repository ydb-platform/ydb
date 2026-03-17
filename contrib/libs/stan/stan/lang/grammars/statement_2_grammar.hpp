#ifndef STAN_LANG_GRAMMARS_STATEMENT_2_GRAMMAR_HPP
#define STAN_LANG_GRAMMARS_STATEMENT_2_GRAMMAR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/expression_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <boost/spirit/include/qi.hpp>
#include <sstream>

namespace stan {

  namespace lang {

    template <typename Iterator>
    struct statement_grammar;

    //   _r1 var_scope
    //   _r2 true if in loop (allowing break/continue)
    template <typename Iterator>
    struct statement_2_grammar
      : boost::spirit::qi::grammar<Iterator,
                                   statement(scope, bool),
                                   whitespace_grammar<Iterator> > {
      variable_map& var_map_;
      std::stringstream& error_msgs_;
      expression_grammar<Iterator> expression_g;
      statement_grammar<Iterator>& statement_g;

      statement_2_grammar(variable_map& var_map,
                          std::stringstream& error_msgs,
                          statement_grammar<Iterator>& sg);

      boost::spirit::qi::rule<Iterator,
                              conditional_statement(scope, bool),
                              whitespace_grammar<Iterator> >
      conditional_statement_r;

      boost::spirit::qi::rule<Iterator,
                              statement(scope, bool),
                              whitespace_grammar<Iterator> >
      statement_2_r;
    };

  }
}
#endif
