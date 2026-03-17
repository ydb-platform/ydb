#ifndef STAN_LANG_GRAMMARS_BARE_TYPE_GRAMMAR_DEF_HPP
#define STAN_LANG_GRAMMARS_BARE_TYPE_GRAMMAR_DEF_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/bare_type_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/version.hpp>

namespace stan {
  namespace lang {

    template <typename Iterator>
    bare_type_grammar<Iterator>::bare_type_grammar(
                                 std::stringstream& error_msgs)
      : bare_type_grammar::base_type(bare_type_r),
        error_msgs_(error_msgs) {
      using boost::spirit::qi::eps;
      using boost::spirit::qi::lit;
      using boost::spirit::qi::_pass;
      using boost::spirit::qi::_val;
      using boost::spirit::qi::_1;
      using boost::spirit::qi::_2;

      bare_type_r.name("bare type definition\n"
               "   (no dimensions or constraints, just commas,\n"
               "   e.g. real[,] for a 2D array or int for a single integer,\n"
               "   and constrained types such as cov_matrix not allowed)");
      bare_type_r = (type_identifier_r
                     >> bare_dims_r)
                    [validate_bare_type_f(_val, _1, _2, _pass,
                                          boost::phoenix::ref(error_msgs_))];

      type_identifier_r.name("bare type identifier\n"
                "  legal values: void, int, real, vector, row_vector, matrix");
      type_identifier_r
        %= lit("void")[assign_lhs_f(_val, bare_expr_type(void_type()))]
        | lit("int")[assign_lhs_f(_val, bare_expr_type(int_type()))]
        | lit("real")[assign_lhs_f(_val, bare_expr_type(double_type()))]
        | lit("vector")[assign_lhs_f(_val, bare_expr_type(vector_type()))]
        | lit("row_vector")[assign_lhs_f(_val,
                                         bare_expr_type(row_vector_type()))]
        | lit("matrix")[assign_lhs_f(_val, bare_expr_type(matrix_type()))];

      bare_dims_r.name("array dimensions,\n"
             "    e.g., empty (not an array) [] (1D array) or [,] (2D array)");
      bare_dims_r
        %= eps[assign_lhs_f(_val, static_cast<size_t>(0))]
        >> -(lit('[')[assign_lhs_f(_val, static_cast<size_t>(1))]
             > *(lit(',')[increment_size_t_f(_val)])
             > end_bare_types_r);

      end_bare_types_r.name("comma to indicate more dimensions"
                            " or ] to end type declaration");
      end_bare_types_r %= lit(']');
    }

  }
}
#endif
