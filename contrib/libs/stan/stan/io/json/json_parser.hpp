#ifndef STAN_IO_JSON_JSON_PARSER_HPP
#define STAN_IO_JSON_JSON_PARSER_HPP

#include <boost/lexical_cast.hpp>

#include <stan/io/validate_zero_buf.hpp>
#include <stan/io/json/json_error.hpp>

#include <stdexcept>
#include <iostream>
#include <istream>
#include <limits>
#include <sstream>
#include <string>

namespace stan {

  namespace json {

    const unsigned int MIN_HIGH_SURROGATE = 0xD800;
    const unsigned int MAX_HIGH_SURROGATE = 0xDBFF;
    const unsigned int MIN_LOW_SURROGATE = 0xDC00;
    const unsigned int MAX_LOW_SURROGATE = 0xDFFF;
    const unsigned int MIN_SUPPLEMENTARY_CODE_POINT = 0x010000;

    inline bool is_high_surrogate(unsigned int cp) {
      return (cp >= MIN_HIGH_SURROGATE && cp <= MAX_HIGH_SURROGATE);
    }

    inline bool is_low_surrogate(unsigned int cp) {
      return (cp >= MIN_LOW_SURROGATE && cp <= MAX_LOW_SURROGATE);
    }

    inline bool is_whitespace(char c) {
      return c == ' ' || c == '\n' || c == '\t' || c == '\r';
    }

    /**
     * A <code>json_parser</code> is a SAX-style streaming parser
     * that enforces JSON syntax and parses JSON elements
     * from an input stream, sending callbacks to a user-supplied
     * <code>json_handler</code>.
     */
    template <typename Handler, bool Validate_UTF_8>
    class parser {
    public:
      parser(Handler& h,
             std::istream& in)
        : h_(h),
          in_(in),
          next_char_(0),
          line_(0),
          column_(0)
      {  }

      ~parser() {
      }

      void parse() {
        h_.start_text();
        parse_text();
        h_.end_text();
      }

    private:
      json_error json_exception(const std::string& msg) const {
        std::stringstream ss;
        ss << "Error in JSON parsing at"
           << " line=" << line_ << " column=" << column_
           << std::endl
           << msg
           << std::endl;
        return json_error(ss.str());
      }

      // JSON-text = object / array
      void parse_text() {
        char c = get_non_ws_char();
        if (c == '{') {              // begin-object
          h_.start_object();
          parse_object_members_end_object();
          h_.end_object();
        } else if (c == '[') {      // begin-array
          // array
          h_.start_array();
          parse_array_values_end_array();
          h_.end_array();
        } else {
          throw json_exception("expecting start of object ({) or array ([)");
        }
      }

      // value =  false / null / true / object / array / number / string
      void parse_value() {
        // value
        char c = get_non_ws_char();
        if (c == 'f') {
          // false
          parse_false_literal();
        } else if (c == 'n') {
          // null
          parse_null_literal();
        } else if (c == 't') {
          // true
          parse_true_literal();
        } else if (c == '"') {
          // string
          h_.string(parse_string_chars_quotation_mark());
        } else if (c == '{' || c == '[') {
          // object / array
          unget_char();
          parse_text();
        } else if (c == '-' ||
                   (c >= '0' && c <= '9') ||
                   c == 'I' || c == 'N') {
          unget_char();
          parse_number();
        } else {
          throw json_exception("illegal value, expecting object, array, "
                               "number, string, or literal true/false/null");
        }
      }

      void parse_number() {
        bool is_positive = true;

        std::stringstream ss;
        char c = get_non_ws_char();
        // minus
        if (c == '-') {
          is_positive = false;
          ss << c;
          c = get_char();
        }

        // Infinity
        if ((is_positive && ss.str() == "I") ||
            c == 'I') {
            parse_infinity_literal(is_positive);
            return;
        }
        // Nan
        if (c == 'N') {
          parse_nan_literal();
          return;
        }

        // int
        //   zero / digit1-9
        if (c < '0' || c > '9')
          throw json_exception("expecting int part of number");
        ss << c;

        //   *DIGIT
        bool leading_zero = (c == '0');
        c = get_char();
        if (leading_zero && (c == '0'))
          throw json_exception("zero padded numbers not allowed");
        while (c >= '0' && c <= '9') {
          ss << c;
          c = get_char();
        }

        // frac
        bool is_integer = true;
        if (c == '.') {
          is_integer = false;
          ss << '.';
          c = get_char();
          if (c < '0' || c > '9')
            throw json_exception("expected digit after decimal");
          ss << c;
          c = get_char();
          while (c >= '0' && c <= '9') {
            ss << c;
            c = get_char();
          }
        }

        // exp
        if (c == 'e' || c == 'E') {
          is_integer = false;
          ss << c;
          c = get_char();
          // minus / plus
          if (c == '+' || c == '-') {
            ss << c;
            c = get_char();
          }
          // 1*DIGIT
          if (c < '0' || c > '9')
            throw json_exception("expected digit after e/E");
          while (c >= '0' && c <= '9') {
            ss << c;
            c = get_char();
          }
        }
        unget_char();

        if (is_integer) {
          if (is_positive) {
            unsigned long n;  // NOLINT(runtime/int)
            try {
              // NOLINTNEXTLINE(runtime/int)
              n = boost::lexical_cast<unsigned long>(ss.str());
            } catch (const boost::bad_lexical_cast & ) {
              throw json_exception("number exceeds integer range");
            }
            ss >> n;
            h_.number_unsigned_long(n);
          } else {
            long n;  // NOLINT(runtime/int)
            try {
              // NOLINTNEXTLINE(runtime/int)
              n = boost::lexical_cast<unsigned long>(ss.str());
            } catch (const boost::bad_lexical_cast & ) {
              throw json_exception("number exceeds integer range");
            }
            ss >> n;
            h_.number_long(n);
          }
        } else {
          double x;
          try {
            std::string ss_str = ss.str();
            x = boost::lexical_cast<double>(ss_str);
            if (x == 0)
              io::validate_zero_buf(ss_str);
          } catch (const boost::bad_lexical_cast & ) {
            throw json_exception("number exceeds double range");
          }
          ss >> x;
          h_.number_double(x);
        }
      }

      std::string parse_string_chars_quotation_mark() {
        std::stringstream s;
        while (true) {
          char c = get_char();
          if (c == '"') {
            return s.str();
          } else if (c == '\\') {
            c = get_char();
            if (c == '\\'  || c == '/' || c == '"') {
              s << c;
            } else if (c == 'b') {
              s << '\b';
            } else if (c == 'f') {
              s << '\f';
            } else if (c == 'n') {
              s << '\n';
            } else if (c == 'r') {
              s << '\r';
            } else if (c == 't') {
              s << '\t';
            } else if (c == 'u') {
              get_escaped_unicode(s);
            } else {
              throw json_exception("expecting legal escape");
            }
            continue;
          } else if (c > 0 && c < 0x20) {  // ASCII control characters
            throw json_exception("found control character, char values less "
                                 "than U+0020 must be \\u escaped");
          }
          s << c;
        }
      }

      void parse_true_literal() {
        get_chars("rue");
        h_.boolean(true);
      }

      void parse_false_literal() {
        get_chars("alse");
        h_.boolean(false);
      }

      void parse_null_literal() {
        get_chars("ull");
        h_.null();
      }

      void parse_infinity_literal(bool is_positive) {
        get_chars("nfinity");
        if (is_positive)
          h_.number_double(std::numeric_limits<double>::infinity());
        else
          h_.number_double(-std::numeric_limits<double>::infinity());
      }

      void parse_nan_literal() {
        get_chars("aN");
        h_.number_double(std::numeric_limits<double>::quiet_NaN());
      }

      void get_escaped_unicode(std::stringstream& s) {
        unsigned int codepoint = get_int_as_hex_chars();
        if (!(is_high_surrogate(codepoint) || is_low_surrogate(codepoint))) {
          putCodepoint(s, codepoint);
        } else if (!is_high_surrogate(codepoint)) {
          throw json_exception("illegal unicode values, found "
                               "low-surrogate, missing high-surrogate");
        } else {
          char c = get_char();
          if (!(c == '\\'))
            throw json_exception("illegal unicode values, found "
                                 "high-surrogate, expecting low-surrogate");
          c = get_char();
          if (!(c == 'u'))
            throw json_exception("illegal unicode values, found "
                                 "high-surrogate, expecting low-surrogate");
          unsigned int codepoint2 = get_int_as_hex_chars();
          unsigned int supplemental
            = ((codepoint - MIN_HIGH_SURROGATE) << 10)
            + (codepoint2 - MIN_LOW_SURROGATE)
            + MIN_SUPPLEMENTARY_CODE_POINT;
          putCodepoint(s, supplemental);
        }
      }

      unsigned int get_int_as_hex_chars() {
        std::stringstream s;
        s << std::hex;
        for (int i = 0; i < 4; i++) {
          char c = get_char();
          if (!((c >= 'a' && c<= 'f')
                || (c >= 'A' && c<= 'F')
                || (c >= '0' && c<= '9')))
            throw json_exception("illegal unicode code point");
          s << c;
        }
        unsigned int hex;
        s >> hex;
        return hex;
      }

      void putCodepoint(std::stringstream& s, unsigned int codepoint) {
        if (codepoint <= 0x7f) {
          s.put(codepoint);
        } else if (codepoint <= 0x7ff) {
          s.put(0xc0 | ((codepoint >> 6) & 0x1f));
          s.put(0x80 | (codepoint & 0x3f));
        } else if (codepoint <= 0xffff) {
          s.put(0xe0 | ((codepoint >> 12) & 0x0f));
          s.put(0x80 | ((codepoint >> 6) & 0x3f));
          s.put(0x80 | (codepoint & 0x3f));
        } else {
          s.put(0xf0 | ((codepoint >> 18) & 0x07));
          s.put(0x80 | ((codepoint >> 12) & 0x3f));
          s.put(0x80 | ((codepoint >> 6) & 0x3f));
          s.put(0x80 | (codepoint & 0x3f));
        }
      }

      void get_chars(const std::string& s) {
        for (size_t i = 0; i < s.size(); ++i) {
          char c = get_char();
          if (c != s[i])
            throw json_exception("expecting rest of literal: "
                                 + s.substr(i));
        }
      }

      void parse_array_values_end_array() {
        char c = get_non_ws_char();
        if (c == ']') return;
        unget_char();
        while (true) {
          parse_value();
          char c = get_non_ws_char();
          if (c == ']') return;
          if (c != ',') {
            throw json_exception("in array, expecting ] or ,");
          }
          c = get_non_ws_char();
          if (c == ']')
            throw json_exception("in array, expecting value");
          unget_char();
        }
      }

      void parse_object_members_end_object() {
        char c = get_non_ws_char();
        if (c == '}') return;
        while (true) {
          // string (key)
          if (c != '"')
            throw json_exception("expecting member key"
                                 " or end of object marker (})");
          std::string key = parse_string_chars_quotation_mark();
          h_.key(key);
          // name-separator separator
          c = get_non_ws_char();
          if (c != ':')
            throw json_exception("expecting key-value separator :");
          // value
          parse_value();

          // continuation
          c = get_non_ws_char();
          if (c == '}')
            return;
          if (c != ',')
            throw json_exception("expecting end of object } or separator ,");
          c = get_non_ws_char();
        }
      }

      char get_char() {
        char c = in_.get();
        if (!in_.good())
          throw json_exception("unexpected end of stream");
        if (c == '\n') {
          ++line_;
          column_ = 1;
        } else {
          ++column_;
        }
        return c;
      }

      char get_non_ws_char() {
        while (true) {
          char c = get_char();
          if (is_whitespace(c)) continue;
          return c;
        }
      }

      void unget_char() {
        in_.unget();
        --column_;
      }

      Handler& h_;
      std::istream& in_;
      char next_char_;
      size_t line_;
      size_t column_;
    };


    /**
     * Parse the JSON text represented by the specified input stream,
     * sending events to the specified handler, and optionally
     * validating the UTF-8 encoding.
     *
     * @tparam Validate_UTF_8
     * @tparam Handler
     * @param in Input stream from which to parse
     * @param handler Handler for events from parser
     */
    template <bool Validate_UTF_8, typename Handler>
    void parse(std::istream& in,
               Handler& handler) {
      parser<Handler, Validate_UTF_8>(handler, in).parse();
    }

    /**
     * Parse the JSON text represented by the specified input stream,
     * sending events to the specified handler, and optionally
     * validating the UTF-8 encoding.
     *
     * @tparam Handler
     * @param in Input stream from which to parse
     * @param handler Handler for events from parser
     */
    template <typename Handler>
    void parse(std::istream& in,
               Handler& handler) {
      parse<false>(in, handler);
    }

  }
}
#endif
