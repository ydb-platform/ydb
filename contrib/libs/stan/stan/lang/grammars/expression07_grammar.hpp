#ifndef STAN_LANG_GRAMMARS_EXPRESSION07_GRAMMAR_HPP
#define STAN_LANG_GRAMMARS_EXPRESSION07_GRAMMAR_HPP

#include <boost/spirit/include/qi.hpp>

#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/expression_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <stan/lang/grammars/term_grammar.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>

#include <string>
#include <sstream>
#include <vector>

namespace stan {
  namespace lang {

    template <typename Iterator>
    struct term_grammar;

    template <typename Iterator>
    struct expression_grammar;

    template <typename Iterator>
    struct expression07_grammar
      : public boost::spirit::qi::grammar<Iterator,
                                          expression(scope),
                                          whitespace_grammar<Iterator> > {
      expression07_grammar(variable_map& var_map,
                           std::stringstream& error_msgs,
                           expression_grammar<Iterator>& eg);

      // global parser information
      variable_map& var_map_;
      std::stringstream& error_msgs_;

      // nested grammars
      term_grammar<Iterator> term_g;

      boost::spirit::qi::rule<Iterator,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      expression07_r;
    };

  }
}
#endif
