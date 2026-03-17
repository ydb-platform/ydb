#ifndef STAN_LANG_GRAMMARS_PROGRAM_GRAMMAR_HPP
#define STAN_LANG_GRAMMARS_PROGRAM_GRAMMAR_HPP

#include <stan/io/program_reader.hpp>
#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <stan/lang/grammars/expression_grammar.hpp>
#include <stan/lang/grammars/block_var_decls_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <stan/lang/grammars/statement_grammar.hpp>
#include <stan/lang/grammars/functions_grammar.hpp>
#include <boost/spirit/include/qi.hpp>
#include <string>
#include <sstream>
#include <vector>
#include <utility>

namespace stan {

  namespace lang {

    template <typename Iterator>
    struct program_grammar
      : boost::spirit::qi::grammar<Iterator,
                                   program(),
                                   whitespace_grammar<Iterator> > {
      std::string model_name_;
      const io::program_reader& reader_;
      variable_map var_map_;
      std::stringstream error_msgs_;
      expression_grammar<Iterator> expression_g;
      block_var_decls_grammar<Iterator> block_var_decls_g;
      statement_grammar<Iterator> statement_g;
      functions_grammar<Iterator> functions_g;

      program_grammar(const std::string& model_name,
                      const io::program_reader& reader,
                      bool allow_undefined = false);

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<scope>,
                              std::vector<block_var_decl>(),
                              whitespace_grammar<Iterator> >
      data_var_decls_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<scope>,
                              std::pair<std::vector<block_var_decl>,
                                        std::vector<statement> >(),
                              whitespace_grammar<Iterator> >
      derived_data_var_decls_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<scope>,
                              std::pair<std::vector<block_var_decl>,
                                        std::vector<statement> >(),
                              whitespace_grammar<Iterator> >
      derived_var_decls_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<scope>,
                              std::pair<std::vector<block_var_decl>,
                                        std::vector<statement> >(),
                              whitespace_grammar<Iterator> >
      generated_var_decls_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<scope>,
                              statement(),
                              whitespace_grammar<Iterator> >
      model_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<scope>,
                              std::vector<block_var_decl>(),
                              whitespace_grammar<Iterator> >
      param_var_decls_r;


      boost::spirit::qi::rule<Iterator,
                              program(),
                              whitespace_grammar<Iterator> >
      program_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::unused_type,
                              whitespace_grammar<Iterator> >
      end_var_decls_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::unused_type,
                              whitespace_grammar<Iterator> >
      end_var_decls_statements_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::unused_type,
                              whitespace_grammar<Iterator> >
      end_var_definitions_r;
    };

  }
}
#endif
