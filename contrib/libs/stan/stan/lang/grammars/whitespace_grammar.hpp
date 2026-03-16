#ifndef STAN_LANG_GRAMMARS_WHITESPACE_GRAMMAR_HPP
#define STAN_LANG_GRAMMARS_WHITESPACE_GRAMMAR_HPP

#include <boost/spirit/include/qi.hpp>
#include <sstream>

namespace stan {

  namespace lang {

    template <typename Iterator>
    struct whitespace_grammar : public boost::spirit::qi::grammar<Iterator> {
      explicit whitespace_grammar(std::stringstream& ss);

      std::stringstream& error_msgs_;
      boost::spirit::qi::rule<Iterator> whitespace;
    };


  }

}



#endif
