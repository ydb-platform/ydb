#ifndef STAN_LANG_GRAMMARS_ITERATOR_TYPEDEFS_HPP
#define STAN_LANG_GRAMMARS_ITERATOR_TYPEDEFS_HPP

#include <boost/spirit/include/version.hpp>
#include <boost/spirit/home/support/iterators/line_pos_iterator.hpp>
#include <string>

namespace stan {
  namespace lang {
    typedef std::string::const_iterator input_iterator_t;
    typedef boost::spirit::line_pos_iterator<input_iterator_t> pos_iterator_t;
  }
}
#endif
