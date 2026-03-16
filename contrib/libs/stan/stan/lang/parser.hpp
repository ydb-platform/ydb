#ifndef STAN_LANG_PARSER_HPP
#define STAN_LANG_PARSER_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/program_grammar.hpp>
#include <stan/lang/grammars/whitespace_grammar.hpp>
#include <boost/spirit/home/support/iterators/line_pos_iterator.hpp>
#include <boost/spirit/include/qi.hpp>
#include <istream>
#include <ostream>
#include <sstream>
#include <string>
#include <stdexcept>

namespace stan {
  namespace lang {

    /**
     * Parse the program from the specified input stream, writing
     * warning messages to the specified output stream, with the
     * specified model, setting the specified program to the result,
     * with a flag indicating whether undefined function declarations
     * are allowed.
     *
     * @param out stream to which error messages and warnings are
     *   written
     * @param in stream from which the program is read
     * @param name name of program
     * @param reader program reader with include structure
     * @param prog program into which result is written
     * @param allow_undefined true if functions may be declared but
     *   not defined in the functions block
     * @return true if parse succeeds
     */
     bool parse(std::ostream* out, std::istream& in, const std::string& name,
                const io::program_reader& reader, program& prog,
                const bool allow_undefined = false) {
      using boost::spirit::qi::expectation_failure;
      using boost::spirit::qi::phrase_parse;

      stan::lang::function_signatures::reset_sigs();

      std::ostringstream buf;
      buf << in.rdbuf();
      std::string stan_string = buf.str() + "\n";
      if (!is_nonempty(stan_string))
        *out << std::endl << "WARNING: empty program" << std::endl;

      typedef std::string::const_iterator input_iterator;
      typedef boost::spirit::line_pos_iterator<input_iterator> lp_iterator;

      lp_iterator fwd_begin = lp_iterator(stan_string.begin());
      lp_iterator fwd_end = lp_iterator(stan_string.end());

      program_grammar<lp_iterator> prog_grammar(name, reader, allow_undefined);
      whitespace_grammar<lp_iterator> whitesp_grammar(prog_grammar.error_msgs_);

      bool parse_succeeded = false;
      try {
        parse_succeeded = phrase_parse(fwd_begin, fwd_end, prog_grammar,
                                       whitesp_grammar, prog);
        std::string diagnostics = prog_grammar.error_msgs_.str();
        if (out && is_nonempty(diagnostics))
          *out << "DIAGNOSTIC(S) FROM PARSER:" << std::endl
               << diagnostics << std::endl;
      } catch (const expectation_failure<lp_iterator>& e) {
        std::stringstream msg;
        std::string diagnostics = prog_grammar.error_msgs_.str();
        if (out && is_nonempty(diagnostics))
          msg << "SYNTAX ERROR, MESSAGE(S) FROM PARSER:"
              << std::endl
              << diagnostics;
        if (out) {
          std::stringstream ss;
          ss << e.what_;
          std::string e_what = ss.str();
          std::string angle_eps("<eps>");
          if (e_what != angle_eps)
            msg << "PARSER EXPECTED: "
                << e.what_
                << std::endl;
        }
        throw std::invalid_argument(msg.str());
      } catch (const std::exception& e) {
        std::stringstream msg;
        msg << "PROGRAM ERROR, MESSAGE(S) FROM PARSER:"
            << std::endl
            << prog_grammar.error_msgs_.str()
            << std::endl;

        throw std::invalid_argument(msg.str());
      }
      bool consumed_all_input = (fwd_begin == fwd_end);
      bool success = parse_succeeded && consumed_all_input;
      if (!success) {
        std::stringstream msg;
        if (!parse_succeeded)
          msg << "PARSE FAILED." << std::endl;
        if (!consumed_all_input) {
          std::basic_stringstream<char> unparsed_non_ws;
          unparsed_non_ws << boost::make_iterator_range(fwd_begin, fwd_end);
          msg << "PARSER FAILED TO PARSE INPUT COMPLETELY"
              << std::endl
              << "STOPPED AT LINE "
              << get_line(fwd_begin)
              << ": "
              << std::endl
              << unparsed_non_ws.str()
              << std::endl;
        }
        msg << std::endl << prog_grammar.error_msgs_.str() << std::endl;
        throw std::invalid_argument(msg.str());
      }
      return true;
    }

  }
}
#endif
