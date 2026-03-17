#ifndef STAN_LANG_GRAMMARS_BARE_TYPE_GRAMMAR_HPP
#define STAN_LANG_GRAMMARS_BARE_TYPE_GRAMMAR_HPP

#include <boost/spirit/include/qi.hpp>
#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>

#include <string>
#include <sstream>
#include <vector>

namespace stan {

  namespace lang {

    template <typename Iterator>
    struct bare_type_grammar
      : boost::spirit::qi::grammar<Iterator,
                                   bare_expr_type(),
                                   whitespace_grammar<Iterator> > {
      std::stringstream& error_msgs_;

      bare_type_grammar(std::stringstream& error_msgs);

      boost::spirit::qi::rule<Iterator,
                              bare_expr_type(),
                              whitespace_grammar<Iterator> >
      bare_type_r;

      boost::spirit::qi::rule<Iterator,
                              bare_expr_type(),
                              whitespace_grammar<Iterator> >
      array_bare_type_r;

      boost::spirit::qi::rule<Iterator,
                              bare_expr_type(),
                              whitespace_grammar<Iterator> >
      single_bare_type_r;

      boost::spirit::qi::rule<Iterator,
                              bare_expr_type(),
                              whitespace_grammar<Iterator> >
      type_identifier_r;

      boost::spirit::qi::rule<Iterator,
                              size_t(),
                              whitespace_grammar<Iterator> >
      bare_dims_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::unused_type,
                              whitespace_grammar<Iterator> >
      end_bare_types_r;
    };

  }
}
#endif
