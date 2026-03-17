#ifndef STAN_LANG_GRAMMARS_LOCAL_VAR_DECLS_GRAMMAR_HPP
#define STAN_LANG_GRAMMARS_LOCAL_VAR_DECLS_GRAMMAR_HPP

#include <stan/io/program_reader.hpp>
#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <stan/lang/grammars/expression_grammar.hpp>
#include <stan/lang/grammars/expression07_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <boost/spirit/include/qi.hpp>
#include <string>
#include <sstream>
#include <vector>

namespace stan {
  namespace lang {

    template <typename Iterator>
    struct local_var_decls_grammar
      : boost::spirit::qi::grammar<Iterator,
                                   std::vector<local_var_decl>(scope),
                                   whitespace_grammar<Iterator> > {
      local_var_decls_grammar(variable_map& var_map,
                              std::stringstream& error_msgs);
      variable_map& var_map_;
      std::stringstream& error_msgs_;
      expression_grammar<Iterator> expression_g;
      expression07_grammar<Iterator> expression07_g;  // disallows comparisons

      boost::spirit::qi::rule<Iterator,
                              std::vector<local_var_decl>(scope),
                              whitespace_grammar<Iterator> >
      local_var_decls_r;

      boost::spirit::qi::rule<Iterator,
                              local_var_decl(scope),
                              whitespace_grammar<Iterator> >
      local_var_decl_r;

      boost::spirit::qi::rule<Iterator,
                              local_var_decl(scope),
                              whitespace_grammar<Iterator> >
      array_local_var_decl_r;

      boost::spirit::qi::rule<Iterator,
                              local_var_decl(scope),
                              whitespace_grammar<Iterator> >
      single_local_var_decl_r;

      boost::spirit::qi::rule<Iterator,
                              local_var_type(scope),
                              whitespace_grammar<Iterator> >
      local_element_type_r;

      boost::spirit::qi::rule<Iterator,
                              double_type(scope),
                              whitespace_grammar<Iterator> >
      local_double_type_r;

      boost::spirit::qi::rule<Iterator,
                              int_type(scope),
                              whitespace_grammar<Iterator> >
      local_int_type_r;

      boost::spirit::qi::rule<Iterator,
                              matrix_local_type(scope),
                              whitespace_grammar<Iterator> >
      local_matrix_type_r;

      boost::spirit::qi::rule<Iterator,
                              row_vector_local_type(scope),
                              whitespace_grammar<Iterator> >
      local_row_vector_type_r;

      boost::spirit::qi::rule<Iterator,
                              vector_local_type(scope),
                              whitespace_grammar<Iterator> >
      local_vector_type_r;

      boost::spirit::qi::rule<Iterator,
                              std::string(),
                              whitespace_grammar<Iterator> >
      local_identifier_r;

      boost::spirit::qi::rule<Iterator,
                              std::string(),
                              whitespace_grammar<Iterator> >
      local_identifier_name_r;

      boost::spirit::qi::rule<Iterator,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      local_opt_def_r;

      boost::spirit::qi::rule<Iterator,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      local_def_r;
      boost::spirit::qi::rule<Iterator,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      local_dim1_r;

      boost::spirit::qi::rule<Iterator,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      local_int_expr_r;

      boost::spirit::qi::rule<Iterator,
                              std::vector<expression>(scope),
                              whitespace_grammar<Iterator> >
      local_dims_r;
    };

  }
}
#endif
