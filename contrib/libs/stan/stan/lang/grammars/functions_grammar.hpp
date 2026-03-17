#ifndef STAN_LANG_GRAMMARS_FUNCTIONS_GRAMMAR_HPP
#define STAN_LANG_GRAMMARS_FUNCTIONS_GRAMMAR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/bare_type_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <stan/lang/grammars/statement_grammar.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <boost/spirit/include/qi.hpp>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace stan {

  namespace lang {

    template <typename Iterator>
    struct functions_grammar
      : boost::spirit::qi::grammar<Iterator,
                                   std::vector<function_decl_def>(),
                                   whitespace_grammar<Iterator> > {
      variable_map& var_map_;
      std::set<std::pair<std::string,
                         function_signature_t> > functions_declared_;
      std::set<std::pair<std::string,
                         function_signature_t> > functions_defined_;
      std::stringstream& error_msgs_;
      statement_grammar<Iterator> statement_g;
      bare_type_grammar<Iterator> bare_type_g;

      functions_grammar(variable_map& var_map,
                        std::stringstream& error_msgs,
                        bool allow_undefined = false);

      boost::spirit::qi::rule<Iterator,
                              std::vector<function_decl_def>(),
                              whitespace_grammar<Iterator> >
      functions_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<scope>,
                              function_decl_def(),
                              whitespace_grammar<Iterator> >
      function_r;

      boost::spirit::qi::rule<Iterator,
                              std::vector<var_decl>(),
                              whitespace_grammar<Iterator> >
      arg_decls_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::locals<scope>,
                              var_decl(),
                              whitespace_grammar<Iterator> >
      arg_decl_r;

      boost::spirit::qi::rule<Iterator,
                              std::string(),
                              whitespace_grammar<Iterator> >
      identifier_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::unused_type,
                              whitespace_grammar<Iterator> >
      close_arg_decls_r;
    };

  }
}
#endif
