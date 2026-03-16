#ifndef STAN_LANG_GRAMMARS_BLOCK_VAR_DECLS_GRAMMAR_HPP
#define STAN_LANG_GRAMMARS_BLOCK_VAR_DECLS_GRAMMAR_HPP

#include <boost/spirit/include/qi.hpp>
#include <stan/io/program_reader.hpp>
#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/expression07_grammar.hpp>
#include <stan/lang/grammars/expression_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace stan {
namespace lang {

template <typename Iterator>
struct block_var_decls_grammar
    : boost::spirit::qi::grammar<Iterator, std::vector<block_var_decl>(scope),
                                 whitespace_grammar<Iterator>> {
  block_var_decls_grammar(variable_map &var_map, std::stringstream &error_msgs);
  variable_map &var_map_;
  std::stringstream &error_msgs_;
  expression_grammar<Iterator> expression_g;
  expression07_grammar<Iterator> expression07_g;  // disallows comparisons

  boost::spirit::qi::rule<Iterator, std::vector<block_var_decl>(scope),
                          whitespace_grammar<Iterator>>
      var_decls_r;

  boost::spirit::qi::rule<Iterator, block_var_decl(scope),
                          whitespace_grammar<Iterator>>
      var_decl_r;

  boost::spirit::qi::rule<Iterator, block_var_decl(scope),
                          whitespace_grammar<Iterator>>
      array_var_decl_r;

  boost::spirit::qi::rule<Iterator, block_var_decl(scope),
                          whitespace_grammar<Iterator>>
      single_var_decl_r;

  boost::spirit::qi::rule<Iterator, block_var_type(scope),
                          whitespace_grammar<Iterator>>
      element_type_r;

  boost::spirit::qi::rule<Iterator, double_block_type(scope),
                          whitespace_grammar<Iterator>>
      double_range_type_r;

  boost::spirit::qi::rule<Iterator, double_block_type(scope),
                          whitespace_grammar<Iterator>>
      double_offset_multiplier_type_r;

  boost::spirit::qi::rule<Iterator, int_block_type(scope),
                          whitespace_grammar<Iterator>>
      int_type_r;

  boost::spirit::qi::rule<Iterator, matrix_block_type(scope),
                          whitespace_grammar<Iterator>>
      matrix_range_type_r;

  boost::spirit::qi::rule<Iterator, matrix_block_type(scope),
                          whitespace_grammar<Iterator>>
      matrix_offset_multiplier_type_r;

  boost::spirit::qi::rule<Iterator, row_vector_block_type(scope),
                          whitespace_grammar<Iterator>>
      row_vector_range_type_r;

  boost::spirit::qi::rule<Iterator, row_vector_block_type(scope),
                          whitespace_grammar<Iterator>>
      row_vector_offset_multiplier_type_r;

  boost::spirit::qi::rule<Iterator, vector_block_type(scope),
                          whitespace_grammar<Iterator>>
      vector_range_type_r;

  boost::spirit::qi::rule<Iterator, vector_block_type(scope),
                          whitespace_grammar<Iterator>>
      vector_offset_multiplier_type_r;

  boost::spirit::qi::rule<Iterator, cholesky_factor_corr_block_type(scope),
                          whitespace_grammar<Iterator>>
      cholesky_factor_corr_type_r;

  boost::spirit::qi::rule<Iterator, cholesky_factor_cov_block_type(scope),
                          whitespace_grammar<Iterator>>
      cholesky_factor_cov_type_r;

  boost::spirit::qi::rule<Iterator, corr_matrix_block_type(scope),
                          whitespace_grammar<Iterator>>
      corr_matrix_type_r;

  boost::spirit::qi::rule<Iterator, cov_matrix_block_type(scope),
                          whitespace_grammar<Iterator>>
      cov_matrix_type_r;

  boost::spirit::qi::rule<Iterator, ordered_block_type(scope),
                          whitespace_grammar<Iterator>>
      ordered_type_r;

  boost::spirit::qi::rule<Iterator, positive_ordered_block_type(scope),
                          whitespace_grammar<Iterator>>
      positive_ordered_type_r;

  boost::spirit::qi::rule<Iterator, simplex_block_type(scope),
                          whitespace_grammar<Iterator>>
      simplex_type_r;

  boost::spirit::qi::rule<Iterator, unit_vector_block_type(scope),
                          whitespace_grammar<Iterator>>
      unit_vector_type_r;

  boost::spirit::qi::rule<Iterator, std::string(), whitespace_grammar<Iterator>>
      identifier_r;

  boost::spirit::qi::rule<Iterator, std::string(), whitespace_grammar<Iterator>>
      identifier_name_r;

  boost::spirit::qi::rule<Iterator, expression(scope),
                          whitespace_grammar<Iterator>>
      opt_def_r;

  boost::spirit::qi::rule<Iterator, expression(scope),
                          whitespace_grammar<Iterator>>
      def_r;

  boost::spirit::qi::rule<Iterator, range(scope), whitespace_grammar<Iterator>>
      range_brackets_double_r;

  boost::spirit::qi::rule<Iterator, range(scope), whitespace_grammar<Iterator>>
      empty_range_r;

  boost::spirit::qi::rule<Iterator, offset_multiplier(scope),
                          whitespace_grammar<Iterator>>
      offset_multiplier_brackets_double_r;

  boost::spirit::qi::rule<Iterator, offset_multiplier(scope),
                          whitespace_grammar<Iterator>>
      empty_offset_multiplier_r;

  boost::spirit::qi::rule<Iterator, range(scope), whitespace_grammar<Iterator>>
      range_brackets_int_r;

  boost::spirit::qi::rule<Iterator, expression(scope),
                          whitespace_grammar<Iterator>>
      dim1_r;

  boost::spirit::qi::rule<Iterator, expression(scope),
                          whitespace_grammar<Iterator>>
      int_data_expr_r;

  boost::spirit::qi::rule<Iterator, std::vector<expression>(scope),
                          whitespace_grammar<Iterator>>
      dims_r;
};

}  // namespace lang
}  // namespace stan
#endif
