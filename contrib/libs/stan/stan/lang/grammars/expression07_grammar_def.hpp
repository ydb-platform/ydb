#ifndef STAN_LANG_GRAMMARS_EXPRESSION07_GRAMMAR_DEF_HPP
#define STAN_LANG_GRAMMARS_EXPRESSION07_GRAMMAR_DEF_HPP

// probably don't need to turn off warnings now, but if so, uncomment
// #include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/term_grammar.hpp>
#include <stan/lang/grammars/expression_grammar.hpp>
#include <stan/lang/grammars/expression07_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <sstream>
#include <string>

namespace stan {

  namespace lang {

    template <typename Iterator>
    expression07_grammar<Iterator>::expression07_grammar(variable_map& var_map,
                                             std::stringstream& error_msgs,
                                             expression_grammar<Iterator>& eg)
      : expression07_grammar::base_type(expression07_r),
        var_map_(var_map),
        error_msgs_(error_msgs),
        term_g(var_map, error_msgs, eg) {
      using boost::spirit::qi::_1;
      using boost::spirit::qi::eps;
      using boost::spirit::qi::lit;
      using boost::spirit::qi::_pass;
      using boost::spirit::qi::_val;
      using boost::spirit::qi::labels::_r1;

      expression07_r.name("expression");
      expression07_r
        %=  term_g(_r1)
            [assign_lhs_f(_val, _1)]
        > *((lit('+')
             > term_g(_r1)
               [addition3_f(_val, _1, boost::phoenix::ref(error_msgs))])
            |
            (lit('-')
             > term_g(_r1)
               [subtraction3_f(_val, _1, boost::phoenix::ref(error_msgs))]))
        > eps[validate_expr_type3_f(_val, _pass,
                                    boost::phoenix::ref(error_msgs_))];
    }

  }
}
#endif
