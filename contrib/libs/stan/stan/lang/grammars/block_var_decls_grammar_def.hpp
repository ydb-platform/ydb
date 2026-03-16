#ifndef STAN_LANG_GRAMMARS_BLOCK_VAR_DECLS_GRAMMAR_DEF_HPP
#define STAN_LANG_GRAMMARS_BLOCK_VAR_DECLS_GRAMMAR_DEF_HPP

#include <boost/phoenix/phoenix.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/version.hpp>
#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/block_var_decls_grammar.hpp>
#include <stan/lang/grammars/common_adaptors_def.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <set>
#include <sstream>
#include <string>
#include <vector>

BOOST_FUSION_ADAPT_STRUCT(stan::lang::block_var_decl,
                          (stan::lang::block_var_type, type_)
                          (std::string, name_)
                          (stan::lang::expression, def_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::double_block_type,
                          (stan::lang::range, bounds_)
                          (stan::lang::offset_multiplier, ls_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::int_block_type,
                          (stan::lang::range, bounds_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::matrix_block_type,
                          (stan::lang::range, bounds_)
                          (stan::lang::offset_multiplier, ls_)
                          (stan::lang::expression, M_)
                          (stan::lang::expression, N_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::row_vector_block_type,
                          (stan::lang::range, bounds_)
                          (stan::lang::offset_multiplier, ls_)
                          (stan::lang::expression, N_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::vector_block_type,
                          (stan::lang::range, bounds_)
                          (stan::lang::offset_multiplier, ls_)
                          (stan::lang::expression, N_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::cholesky_factor_corr_block_type,
                          (stan::lang::expression, K_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::cholesky_factor_cov_block_type,
                          (stan::lang::expression, M_)
                          (stan::lang::expression, N_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::corr_matrix_block_type,
                          (stan::lang::expression, K_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::cov_matrix_block_type,
                          (stan::lang::expression, K_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::ordered_block_type,
                          (stan::lang::expression, K_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::positive_ordered_block_type,
                          (stan::lang::expression, K_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::simplex_block_type,
                          (stan::lang::expression, K_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::unit_vector_block_type,
                          (stan::lang::expression, K_))

BOOST_FUSION_ADAPT_STRUCT(stan::lang::offset_multiplier,
                          (stan::lang::expression, offset_)
                          (stan::lang::expression, multiplier_))

namespace stan {

namespace lang {

template <typename Iterator>
block_var_decls_grammar<Iterator>::block_var_decls_grammar(
    variable_map &var_map, std::stringstream &error_msgs)
    : block_var_decls_grammar::base_type(var_decls_r), var_map_(var_map),
      error_msgs_(error_msgs), expression_g(var_map, error_msgs),
      expression07_g(var_map, error_msgs, expression_g) {
  using boost::spirit::qi::_1;
  using boost::spirit::qi::_2;
  using boost::spirit::qi::_3;
  using boost::spirit::qi::_4;
  using boost::spirit::qi::_pass;
  using boost::spirit::qi::_val;
  using boost::spirit::qi::char_;
  using boost::spirit::qi::eps;
  using boost::spirit::qi::lexeme;
  using boost::spirit::qi::lit;
  using boost::spirit::qi::no_skip;
  using boost::spirit::qi::raw;

  using boost::spirit::qi::labels::_r1;

  using boost::phoenix::begin;
  using boost::phoenix::end;

  var_decls_r.name("variable declarations");
  var_decls_r %= *(var_decl_r(_r1));

  // _r1 var scope
  var_decl_r.name("variable declaration");
  var_decl_r = (raw[array_var_decl_r(_r1)[assign_lhs_f(_val, _1)]]
                   [add_line_number_f(_val, begin(_1), end(_1))] |
                raw[single_var_decl_r(_r1)[assign_lhs_f(_val, _1)]]
                   [add_line_number_f(_val, begin(_1), end(_1))]) >
               eps[add_to_var_map_f(_val, boost::phoenix::ref(var_map_), _pass,
                                    _r1, boost::phoenix::ref(error_msgs_)),
                   validate_definition_f(_r1, _val, _pass,
                                         boost::phoenix::ref(error_msgs_))] >
               lit(';');

  array_var_decl_r.name("array block var declaration");
  array_var_decl_r = (element_type_r(_r1) >> identifier_r >> dims_r(_r1) >>
                      opt_def_r(_r1))[validate_array_block_var_decl_f(
      _val, _1, _2, _3, _4, _pass, boost::phoenix::ref(error_msgs_))];

  single_var_decl_r.name("single-element block var declaration");
  single_var_decl_r %= element_type_r(_r1) > identifier_r > opt_def_r(_r1) >
                       eps[validate_single_block_var_decl_f(
                           _val, _pass, boost::phoenix::ref(error_msgs_))];

  element_type_r.name("block var element type declaration");
  element_type_r %=
      (int_type_r(_r1) | double_range_type_r(_r1) |
       double_offset_multiplier_type_r(_r1) | vector_range_type_r(_r1) |
       vector_offset_multiplier_type_r(_r1) | row_vector_range_type_r(_r1) |
       row_vector_offset_multiplier_type_r(_r1) | matrix_range_type_r(_r1) |
       matrix_offset_multiplier_type_r(_r1) | ordered_type_r(_r1) |
       positive_ordered_type_r(_r1) | simplex_type_r(_r1) |
       unit_vector_type_r(_r1) | corr_matrix_type_r(_r1) |
       cov_matrix_type_r(_r1) | cholesky_factor_corr_type_r(_r1) |
       cholesky_factor_cov_type_r(_r1));

  int_type_r.name("integer type");
  int_type_r %= (lit("int") >> no_skip[!char_("a-zA-Z0-9_")]) >
                -range_brackets_int_r(_r1);

  double_range_type_r.name("real range type");
  double_range_type_r %= (lit("real") >> no_skip[!char_("a-zA-Z0-9_")]) >>
                         range_brackets_double_r(_r1) >
                         empty_offset_multiplier_r(_r1);

  double_offset_multiplier_type_r.name("real offset_multiplier type");
  double_offset_multiplier_type_r %=
      (lit("real") >> no_skip[!char_("a-zA-Z0-9_")]) > empty_range_r(_r1) >
      -offset_multiplier_brackets_double_r(_r1);

  vector_range_type_r.name("vector range type");
  vector_range_type_r %= (lit("vector") >> no_skip[!char_("a-zA-Z0-9_")]) >>
                         range_brackets_double_r(_r1) >
                         empty_offset_multiplier_r(_r1) > dim1_r(_r1);

  vector_offset_multiplier_type_r.name("vector offset_multiplier type");
  vector_offset_multiplier_type_r %=
      (lit("vector") >> no_skip[!char_("a-zA-Z0-9_")]) > empty_range_r(_r1) >
      -offset_multiplier_brackets_double_r(_r1) > dim1_r(_r1);

  row_vector_range_type_r.name("row vector range type");
  row_vector_range_type_r %=
      (lit("row_vector") >> no_skip[!char_("a-zA-Z0-9_")]) >>
      range_brackets_double_r(_r1) > empty_offset_multiplier_r(_r1) >
      dim1_r(_r1);

  row_vector_offset_multiplier_type_r.name("row vector offset_multiplier type");
  row_vector_offset_multiplier_type_r %=
      (lit("row_vector") >> no_skip[!char_("a-zA-Z0-9_")]) >
      empty_range_r(_r1) > -offset_multiplier_brackets_double_r(_r1) >
      dim1_r(_r1);

  matrix_range_type_r.name("matrix range type");
  matrix_range_type_r %=
      (lit("matrix") >> no_skip[!char_("a-zA-Z0-9_")]) >>
      range_brackets_double_r(_r1) > empty_offset_multiplier_r(_r1) > lit('[') >
      int_data_expr_r(_r1) > lit(',') > int_data_expr_r(_r1) > lit(']');

  matrix_offset_multiplier_type_r.name("matrix offset_multiplier type");
  matrix_offset_multiplier_type_r %=
      (lit("matrix") >> no_skip[!char_("a-zA-Z0-9_")]) > empty_range_r(_r1) >
      -offset_multiplier_brackets_double_r(_r1) > lit('[') >
      int_data_expr_r(_r1) > lit(',') > int_data_expr_r(_r1) > lit(']');

  ordered_type_r.name("ordered type");
  ordered_type_r %=
      (lit("ordered") >> no_skip[!char_("a-zA-Z0-9_")]) > dim1_r(_r1);

  positive_ordered_type_r.name("positive ordered type");
  positive_ordered_type_r %=
      (lit("positive_ordered") >> no_skip[!char_("a-zA-Z0-9_")]) > dim1_r(_r1);

  simplex_type_r.name("simplex type");
  simplex_type_r %=
      (lit("simplex") >> no_skip[!char_("a-zA-Z0-9_")]) > dim1_r(_r1);

  unit_vector_type_r.name("unit_vector type");
  unit_vector_type_r %=
      (lit("unit_vector") >> no_skip[!char_("a-zA-Z0-9_")]) > dim1_r(_r1);

  corr_matrix_type_r.name("correlation matrix type");
  corr_matrix_type_r %=
      (lit("corr_matrix") >> no_skip[!char_("a-zA-Z0-9_")]) > dim1_r(_r1);

  cov_matrix_type_r.name("covarience matrix type");
  cov_matrix_type_r %=
      (lit("cov_matrix") >> no_skip[!char_("a-zA-Z0-9_")]) > dim1_r(_r1);

  cholesky_factor_corr_type_r.name("cholesky factor of"
                                   " a correlation matrix type");
  cholesky_factor_corr_type_r %= (lit("cholesky_factor_corr") >>
                                  no_skip[!char_("a-zA-Z0-9_")]) > dim1_r(_r1);

  cholesky_factor_cov_type_r.name("cholesky factor of"
                                  " a covariance matrix type");
  cholesky_factor_cov_type_r %=
      (lit("cholesky_factor_cov") >> no_skip[!char_("a-zA-Z0-9_")]) > lit('[') >
      int_data_expr_r(_r1) > -(lit(',') > int_data_expr_r(_r1)) > lit(']') >
      eps[copy_square_cholesky_dimension_if_necessary_f(_val)];

  // _r1 var scope
  dims_r.name("array dimensions");
  dims_r %= lit('[') > (int_data_expr_r(_r1) % ',') > lit(']');

  // _r1 var scope
  opt_def_r.name("variable definition (optional)");
  opt_def_r %= -def_r(_r1);

  // _r1 var scope
  def_r.name("variable definition");
  def_r %= lit('=') > expression_g(_r1);

  // _r1 var scope
  range_brackets_int_r.name("integer range expression pair, brackets");
  range_brackets_int_r =
      lit('<')[empty_range_f(_val, boost::phoenix::ref(error_msgs_))] >
      (((lit("lower") > lit('=') >
         expression07_g(_r1)[set_int_range_lower_f(
             _val, _1, _pass, boost::phoenix::ref(error_msgs_))]) >
        -(lit(',') > lit("upper") > lit('=') >
          expression07_g(_r1)[set_int_range_upper_f(
              _val, _1, _pass, boost::phoenix::ref(error_msgs_))])) |
       (lit("upper") > lit('=') >
        expression07_g(_r1)[set_int_range_upper_f(
            _val, _1, _pass, boost::phoenix::ref(error_msgs_))])) > lit('>');

  // _r1 var scope
  range_brackets_double_r.name("real range expression pair, brackets");
  range_brackets_double_r =
      lit('<')[empty_range_f(_val, boost::phoenix::ref(error_msgs_))] >>
      (((lit("lower") > lit('=') >
         expression07_g(_r1)[set_double_range_lower_f(
             _val, _1, _pass, boost::phoenix::ref(error_msgs_))]) >
        -(lit(',') > lit("upper") > lit('=') >
          expression07_g(_r1)[set_double_range_upper_f(
              _val, _1, _pass, boost::phoenix::ref(error_msgs_))])) |
       (lit("upper") > lit('=') >
        expression07_g(_r1)[set_double_range_upper_f(
            _val, _1, _pass, boost::phoenix::ref(error_msgs_))])) > lit('>');

  // _r1 var scope
  empty_range_r.name("empty range expression pair");
  empty_range_r = eps[empty_range_f(_val, boost::phoenix::ref(error_msgs_))];

  // _r1 var scope
  offset_multiplier_brackets_double_r.name(
      "real offset-multiplier expression pair, brackets");
  offset_multiplier_brackets_double_r =
      lit('<')[empty_offset_multiplier_f(_val,
                                         boost::phoenix::ref(error_msgs_))] >
      (((lit("offset") > lit('=') >
         expression07_g(_r1)[set_double_offset_multiplier_offset_f(
             _val, _1, _pass, boost::phoenix::ref(error_msgs_))]) >
        -(lit(',') > lit("multiplier") > lit('=') >
          expression07_g(_r1)[set_double_offset_multiplier_multiplier_f(
              _val, _1, _pass, boost::phoenix::ref(error_msgs_))])) |
       (lit("multiplier") > lit('=') >
        expression07_g(_r1)[set_double_offset_multiplier_multiplier_f(
            _val, _1, _pass, boost::phoenix::ref(error_msgs_))])) > lit('>');

  // _r1 var scope
  empty_offset_multiplier_r.name("empty offset-multiplier expression pair");
  empty_offset_multiplier_r =
      eps[empty_offset_multiplier_f(_val, boost::phoenix::ref(error_msgs_))];

  // _r1 var scope
  dim1_r.name("vector length declaration:"
              " data-only integer expression in square brackets");
  dim1_r %= lit('[') > int_data_expr_r(_r1) > lit(']');

  // _r1 var scope
  int_data_expr_r.name("data-only integer expression");
  int_data_expr_r %= expression_g(_r1)[validate_int_data_only_expr_f(
      _1, _pass, boost::phoenix::ref(var_map_),
      boost::phoenix::ref(error_msgs_))];

  identifier_r.name("identifier");
  identifier_r %= identifier_name_r[validate_identifier_f(
      _val, _pass, boost::phoenix::ref(error_msgs_))];

  identifier_name_r.name("identifier subrule");
  identifier_name_r %= lexeme[char_("a-zA-Z") >> *char_("a-zA-Z0-9_.")];
}
}  // namespace lang

}  // namespace stan
#endif
