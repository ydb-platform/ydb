#ifndef STAN_LANG_GRAMMARS_INDEXES_GRAMMAR_DEF_HPP
#define STAN_LANG_GRAMMARS_INDEXES_GRAMMAR_DEF_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/indexes_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>

BOOST_FUSION_ADAPT_STRUCT(stan::lang::uni_idx,
                          (stan::lang::expression, idx_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::multi_idx,
                          (stan::lang::expression, idxs_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::lb_idx,
                          (stan::lang::expression, lb_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::ub_idx,
                          (stan::lang::expression, ub_) )

BOOST_FUSION_ADAPT_STRUCT(stan::lang::lub_idx,
                          (stan::lang::expression, lb_)
                          (stan::lang::expression, ub_) )

namespace stan {

  namespace lang {

    template <typename Iterator>
    indexes_grammar<Iterator>::indexes_grammar(variable_map& var_map,
                                               std::stringstream& error_msgs,
                                               expression_grammar<Iterator>& eg)
      : indexes_grammar::base_type(indexes_r),
        var_map_(var_map),
        error_msgs_(error_msgs),
        expression_g(eg) {
      using boost::spirit::qi::eps;
      using boost::spirit::qi::lit;
      using boost::spirit::qi::_val;
      using boost::spirit::qi::_r1;
      using boost::spirit::qi::_1;
      using boost::spirit::qi::_pass;

      //   _r1 var scope
      indexes_r.name("indexes (zero or more)");
      indexes_r
        %=  lit("[")
        >> (index_r(_r1) % ',')
        > close_indexes_r;

      close_indexes_r.name("one or more container indexes followed by ']'");
      close_indexes_r %= lit(']');

      //   _r1 var scope
      index_r.name("index expression, one of: "
                   "(int, int[], int:, :int, int:int, :)");
      index_r
        %= lub_index_r(_r1)
        | lb_index_r(_r1)
        | uni_index_r(_r1)
        | multi_index_r(_r1)
        | ub_index_r(_r1)
        | omni_index_r(_r1);

      //   _r1 var scope
      lub_index_r.name("index expression int:int");
      lub_index_r
        %= int_expression_r(_r1)
        >> lit(":")
        >> int_expression_r(_r1);


      //   _r1 var scope
      lb_index_r.name("index expression int:");
      lb_index_r
        %= int_expression_r(_r1)
        >> lit(":");

      //   _r1 var scope
      uni_index_r.name("index expression int");
      uni_index_r
        %= int_expression_r(_r1);

      //   _r1 var scope
      multi_index_r.name("index expression int[]");
      multi_index_r
        %= expression_g(_r1)
           [validate_ints_expression_f(_1, _pass,
                                       boost::phoenix::ref(error_msgs_))];

      //   _r1 var scope
      ub_index_r.name("index expression :int");
      ub_index_r
        %= lit(":")
        >> int_expression_r(_r1);

      //   _r1 var scope
      omni_index_r.name("index expression :");
      omni_index_r
        = lit(":")[set_omni_idx_f(_val)]
        |  eps[set_omni_idx_f(_val)];

      //   _r1 var scope
      int_expression_r.name("integer expression");
      int_expression_r
        %= expression_g(_r1)[validate_int_expr_silent_f(_1, _pass)];
    }

  }
}
#endif
