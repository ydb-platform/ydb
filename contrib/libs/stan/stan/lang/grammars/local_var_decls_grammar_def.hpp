#ifndef STAN_LANG_GRAMMARS_LOCAL_VAR_DECLS_GRAMMAR_DEF_HPP
#define STAN_LANG_GRAMMARS_LOCAL_VAR_DECLS_GRAMMAR_DEF_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/local_var_decls_grammar.hpp>
#include <stan/lang/grammars/common_adaptors_def.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/phoenix/phoenix.hpp>
#include <boost/version.hpp>
#include <string>
#include <vector>

BOOST_FUSION_ADAPT_STRUCT(stan::lang::local_var_decl,
                           (stan::lang::local_var_type, type_)
                           (std::string, name_)
                           (stan::lang::expression, def_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::matrix_local_type,
                          (stan::lang::expression, M_)
                          (stan::lang::expression, N_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::row_vector_local_type,
                          (stan::lang::expression, N_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::vector_local_type,
                          (stan::lang::expression, N_))

namespace stan {

  namespace lang {

    template <typename Iterator>
    local_var_decls_grammar<Iterator>::local_var_decls_grammar(
                                       variable_map& var_map,
                                       std::stringstream& error_msgs)
      : local_var_decls_grammar::base_type(local_var_decls_r),
        var_map_(var_map),
        error_msgs_(error_msgs),
        expression_g(var_map, error_msgs),
        expression07_g(var_map, error_msgs, expression_g) {
      using boost::spirit::qi::_1;
      using boost::spirit::qi::_2;
      using boost::spirit::qi::_3;
      using boost::spirit::qi::_4;
      using boost::spirit::qi::char_;
      using boost::spirit::qi::eps;
      using boost::spirit::qi::lexeme;
      using boost::spirit::qi::lit;
      using boost::spirit::qi::no_skip;
      using boost::spirit::qi::_pass;
      using boost::spirit::qi::_val;
      using boost::spirit::qi::raw;

      using boost::spirit::qi::labels::_r1;

      using boost::phoenix::begin;
      using boost::phoenix::end;

      local_var_decls_r.name("variable declarations");
      local_var_decls_r
        %=  *(local_var_decl_r(_r1));

      local_var_decl_r.name("variable declaration");
      local_var_decl_r
        = ( raw[array_local_var_decl_r(_r1)[assign_lhs_f(_val, _1)]]
               [add_line_number_f(_val, begin(_1), end(_1))]
            | raw[single_local_var_decl_r(_r1)[assign_lhs_f(_val, _1)]]
               [add_line_number_f(_val, begin(_1), end(_1))]
            )
        > eps
        [add_to_var_map_f(_val, boost::phoenix::ref(var_map_), _pass, _r1,
                          boost::phoenix::ref(error_msgs_)),
         validate_definition_f(_r1, _val, _pass,
                               boost::phoenix::ref(error_msgs_))]
        > lit(';');

      array_local_var_decl_r.name("array local var declaration");
      array_local_var_decl_r
        = (local_element_type_r(_r1)
           >> local_identifier_r
           >> local_dims_r(_r1)
           >> local_opt_def_r(_r1))
        [validate_array_local_var_decl_f(_val, _1, _2, _3, _4, _pass,
                                         boost::phoenix::ref(error_msgs_))];

      single_local_var_decl_r.name("single-element local var declaration");
      single_local_var_decl_r
        %= local_element_type_r(_r1)
           > local_identifier_r
           > local_opt_def_r(_r1)
           > eps
           [validate_single_local_var_decl_f(_val, _pass,
                                             boost::phoenix::ref(error_msgs_))];

      local_element_type_r.name("local var element type declaration");
      local_element_type_r
        %= local_int_type_r(_r1)
           | local_double_type_r(_r1)
           | local_vector_type_r(_r1)
           | local_row_vector_type_r(_r1)
           | local_matrix_type_r(_r1);

      local_int_type_r.name("integer type");
      local_int_type_r
        %= lit("int")
            >> no_skip[!char_("a-zA-Z0-9_")];

      local_double_type_r.name("real type");
      local_double_type_r
        %= lit("real")
            >> no_skip[!char_("a-zA-Z0-9_")];

      local_vector_type_r.name("vector type");
      local_vector_type_r
        %= (lit("vector")
            >> no_skip[!char_("a-zA-Z0-9_")])
        > local_dim1_r(_r1);

      local_row_vector_type_r.name("row vector type");
      local_row_vector_type_r
        %= (lit("row_vector")
            >> no_skip[!char_("a-zA-Z0-9_")])
        > local_dim1_r(_r1);

      local_matrix_type_r.name("matrix type");
      local_matrix_type_r
        %= (lit("matrix")
            >> no_skip[!char_("a-zA-Z0-9_")])
        > lit('[')
        > local_int_expr_r(_r1)
        > lit(',')
        > local_int_expr_r(_r1)
        > lit(']');

      local_dims_r.name("array dimensions");
      local_dims_r %= lit('[') > (local_int_expr_r(_r1) % ',') > lit(']');

      local_opt_def_r.name("variable definition (optional)");
      local_opt_def_r %= -local_def_r(_r1);

      local_def_r.name("variable definition");
      local_def_r %= lit('=') > expression_g(_r1);

      local_dim1_r.name("vector length declaration:"
                  " integer expression in square brackets");
      local_dim1_r %= lit('[') > local_int_expr_r(_r1) > lit(']');

      local_int_expr_r.name("integer expression");
      local_int_expr_r
        %= expression_g(_r1)
           [validate_int_expr_f(_1, _pass,
                                boost::phoenix::ref(error_msgs_))];

      local_identifier_r.name("identifier");
      local_identifier_r
        %= local_identifier_name_r
           [validate_identifier_f(_val, _pass,
                                  boost::phoenix::ref(error_msgs_))];

      local_identifier_name_r.name("identifier subrule");
      local_identifier_name_r
        %= lexeme[char_("a-zA-Z")
                  >> *char_("a-zA-Z0-9_.")];
    }
  }


}
#endif
