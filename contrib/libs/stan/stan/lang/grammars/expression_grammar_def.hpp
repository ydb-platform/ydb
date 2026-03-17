#ifndef STAN_LANG_GRAMMARS_EXPRESSION_GRAMMAR_DEF_HPP
#define STAN_LANG_GRAMMARS_EXPRESSION_GRAMMAR_DEF_HPP

#include <stan/lang/grammars/expression_grammar.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <sstream>
#include <string>
#include <vector>

BOOST_FUSION_ADAPT_STRUCT(stan::lang::conditional_op,
                          (stan::lang::expression, cond_)
                          (stan::lang::expression, true_val_)
                          (stan::lang::expression, false_val_) )


namespace stan {

  namespace lang {

    template <typename Iterator>
    expression_grammar<Iterator>::expression_grammar(variable_map& var_map,
                                             std::stringstream& error_msgs)
      : expression_grammar::base_type(expression_r),
        var_map_(var_map),
        error_msgs_(error_msgs),
        expression07_g(var_map, error_msgs, *this) {
      using boost::spirit::qi::char_;
      using boost::spirit::qi::eps;
      using boost::spirit::qi::lit;
      using boost::spirit::qi::no_skip;
      using boost::spirit::qi::_1;
      using boost::spirit::qi::_pass;
      using boost::spirit::qi::_val;
      using boost::spirit::qi::labels::_r1;

      expression_r.name("expression");
      expression_r
        %= (expression15_r(_r1) >> no_skip[!char_('?')] > eps)
        | conditional_op_r(_r1);

      conditional_op_r.name("conditional op expression, cond ? t_val : f_val ");
      conditional_op_r
        %= expression15_r(_r1)
        >> lit("?")
        >> expression_r(_r1)
        >> lit(":")
        >> expression_r(_r1)[validate_conditional_op_f(_val, _r1, _pass,
                                      boost::phoenix::ref(var_map_),
                                      boost::phoenix::ref(error_msgs))];

      expression15_r.name("expression");
      expression15_r
        = expression14_r(_r1)[assign_lhs_f(_val, _1)]
        > *(lit("||")
            > expression14_r(_r1)
              [binary_op_f(_val, _1, "||", "logical_or",
                           boost::phoenix::ref(error_msgs))]);

      expression14_r.name("expression");
      expression14_r
        = expression10_r(_r1)[assign_lhs_f(_val, _1)]
        > *(lit("&&")
            > expression10_r(_r1)
              [binary_op_f(_val, _1, "&&", "logical_and",
                           boost::phoenix::ref(error_msgs))]);

      expression10_r.name("expression");
      expression10_r
        = expression09_r(_r1)[assign_lhs_f(_val, _1)]
        > *((lit("==")
             > expression09_r(_r1)
               [binary_op_f(_val, _1, "==", "logical_eq",
                            boost::phoenix::ref(error_msgs))])
              |
              (lit("!=")
               > expression09_r(_r1)
                 [binary_op_f(_val, _1, "!=", "logical_neq",
                              boost::phoenix::ref(error_msgs))]));

      expression09_r.name("expression");
      expression09_r
        = expression07_g(_r1)[assign_lhs_f(_val, _1)]
        > *((lit("<=")
             > expression07_g(_r1)
               [binary_op_f(_val, _1, "<", "logical_lte",
                            boost::phoenix::ref(error_msgs))])
            | (lit("<")
               > expression07_g(_r1)
                 [binary_op_f(_val, _1, "<=", "logical_lt",
                              boost::phoenix::ref(error_msgs))])
            | (lit(">=")
               > expression07_g(_r1)
                 [binary_op_f(_val, _1, ">", "logical_gte",
                              boost::phoenix::ref(error_msgs))])
            | (lit(">")
               > expression07_g(_r1)
                 [binary_op_f(_val, _1, ">=", "logical_gt",
                              boost::phoenix::ref(error_msgs))]));
    }
  }
}
#endif
