#ifndef STAN_LANG_GRAMMARS_WHITESPACE_GRAMMAR_DEF_HPP
#define STAN_LANG_GRAMMARS_WHITESPACE_GRAMMAR_DEF_HPP

#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/version.hpp>
#include <sstream>

namespace stan {

  namespace lang {

    template <typename Iterator>
    whitespace_grammar<Iterator>::whitespace_grammar(std::stringstream& ss)
      : whitespace_grammar::base_type(whitespace), error_msgs_(ss) {
      using boost::spirit::qi::omit;
      using boost::spirit::qi::char_;
      using boost::spirit::qi::eol;
      whitespace
        = ((omit["/*"] >> *(char_ - "*/")) > omit["*/"])
        | (omit["//"] >> *(char_ - eol))
        | (omit["#"] >> *(char_ - eol))
          [deprecate_pound_comment_f(boost::phoenix::ref(error_msgs_))]
        | boost::spirit::ascii::space_type();
    }

  }
}
#endif
