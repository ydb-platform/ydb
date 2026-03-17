#ifndef STAN_LANG_GRAMMARS_INDEXES_GRAMMAR_HPP
#define STAN_LANG_GRAMMARS_INDEXES_GRAMMAR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/expression_grammar.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <boost/spirit/include/qi.hpp>
#include <string>
#include <sstream>
#include <vector>

namespace stan {

  namespace lang {

    // needed to break circularity of expression grammar including indexes
    template <typename Iterator>
    struct expression_grammar;

    template <typename Iterator>
    struct indexes_grammar
      : boost::spirit::qi::grammar<Iterator,
                                   std::vector<idx>(scope),
                                   whitespace_grammar<Iterator> > {
      variable_map& var_map_;
      std::stringstream& error_msgs_;
      expression_grammar<Iterator>& expression_g;

      indexes_grammar(variable_map& var_map,
                      std::stringstream& error_msgs,
                      expression_grammar<Iterator>& eg);

      boost::spirit::qi::rule<Iterator,
                              std::vector<idx>(scope),
                              whitespace_grammar<Iterator> >
      indexes_r;

      boost::spirit::qi::rule<Iterator,
                              idx(scope),
                              whitespace_grammar<Iterator> >
      index_r;

      boost::spirit::qi::rule<Iterator,
                              boost::spirit::qi::unused_type,
                              whitespace_grammar<Iterator> >
      close_indexes_r;


      boost::spirit::qi::rule<Iterator,
                              uni_idx(scope),
                              whitespace_grammar<Iterator> >
      uni_index_r;

      boost::spirit::qi::rule<Iterator,
                              multi_idx(scope),
                              whitespace_grammar<Iterator> >
      multi_index_r;

      boost::spirit::qi::rule<Iterator,
                              omni_idx(scope),
                              whitespace_grammar<Iterator> >
      omni_index_r;

      boost::spirit::qi::rule<Iterator,
                              lb_idx(scope),
                              whitespace_grammar<Iterator> >
      lb_index_r;

      boost::spirit::qi::rule<Iterator,
                              ub_idx(scope),
                              whitespace_grammar<Iterator> >
      ub_index_r;


      boost::spirit::qi::rule<Iterator,
                              lub_idx(scope),
                              whitespace_grammar<Iterator> >
      lub_index_r;

      boost::spirit::qi::rule<Iterator,
                              expression(scope),
                              whitespace_grammar<Iterator> >
      int_expression_r;
    };

  }
}
#endif
