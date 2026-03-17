#ifndef STAN_IO_DUMP_HPP
#define STAN_IO_DUMP_HPP

#include <stan/io/validate_zero_buf.hpp>
#include <stan/io/var_context.hpp>
#include <stan/math/prim/mat.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/throw_exception.hpp>
#include <boost/type_traits/is_floating_point.hpp>
#include <boost/type_traits/is_integral.hpp>
#include <boost/type_traits/is_arithmetic.hpp>
#include <boost/utility/enable_if.hpp>
#include <iostream>
#include <limits>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <cctype>

namespace stan {
  namespace io {

    using Eigen::Dynamic;
    /**
     * Reads data from S-plus dump format.
     *
     * A <code>dump_reader</code> parses data from the S-plus dump
     * format, a human-readable ASCII representation of arbitrarily
     * dimensioned arrays of integers and arrays of floating point
     * values.
     *
     * <p>Stan's dump reader is limited to reading
     * integers, scalars and arrays of arbitrary dimensionality of
     * integers and scalars.  It is able to read from a file
     * consisting of a sequence of dumped variables.
     *
     * <p>There cannot be any <code>NA</code>
     * (i.e., undefined) values, because these cannot be
     * represented as <code>double</code> values.
     *
     * <p>The dump reader class follows a standard scanner pattern.
     * The method <code>next()</code> is called to scan the next
     * input.  The type, dimensions, and values of the input is then
     * available through method calls.  Here, the type is either
     * double or integer, and the values are the name of the variable
     * and its array of values.  If there is a single value, the
     * dimension array is empty.  For a list, the dimension
     * array contains a single entry for the number of values.
     * For an array, the dimensions are the dimensions of the array.
     *
     * <p>Reads are performed in an "S-compatible" mode whereby
     * a string such as "1" or "-127" denotes and integer, whereas
     * a string such as "1." or "0.9e-5" represents a floating
     * point value.
     *
     * <p>The dump reader treats "integer(x)" as an array of zeros
     * (type being integer and length x), where x any non-negative
     * integers and x can be omitted to indicate zero-length.
     * So the following are all legitimate: * "x <- integer()",
     * "x <- integer(0) ", and "x <- integer(3)". For array of zeros
     * of type double, we can replace the above "integer" with "double".
     * This is mainly for the purpose of supporting zero-size arrays
     * such as "x <- structure(integer(0), .Dim = c(2, 0))".
     *
     * <p>For dumping, arrays are indexed in last-index major fashion,
     * which corresponds to column-major order for matrices
     * represented as two-dimensional arrays.  As a result, the first
     * indices change fastest.  For instance, if there is an
     * three-dimensional array <code>x</code> with dimensions
     * <code>[2,2,2]</code>, then there are 8 values, provided in the
     * order
     *
     * <p><code>[0,0,0]</code>,
     * <code>[1,0,0]</code>,
     * <code>[0,1,0]</code>,
     * <code>[1,1,0]</code>,
     * <code>[0,0,1]</code>,
     * <code>[1,0,1]</code>,
     * <code>[0,1,1]</code>,
     * <code>[1,1,1]</code>.
     *
     * definitions ::= definition+
     *
     * definition ::= name ("<-" | '=') value optional_semicolon
     *
     * name ::= char*
     *        | ''' char* '''
     *        | '"' char* '"'
     *
     * value ::= value<int> | value<double>
     *
     * value<T> ::= T
     *            | seq<T>
     *            | zero_array<T>
     *            | "structure" '(' seq<T> ',' ".Dim" '=' seq<int> ')'
     *            | "structure" '(' zero_array<T> ',' ".Dim" '=' seq<int> ')'
     *
     * seq<int> ::= int ':' int
     *            | cseq<int>
     *
     * seq<double> ::= cseq<double>
     *
     * cseq<T> ::= 'c' '(' vseq<T> ')'
     *
     * vseq<T> ::= T
     *           | T ',' vseq<T>
     *
     * zero_array<integer> ::= "integer"<non negative int?>
     *
     * zero_array<double> ::= "double"<non negative int?>
     *
     */
    class dump_reader {
    private:
      std::string buf_;
      std::string name_;
      std::vector<int> stack_i_;
      std::vector<double> stack_r_;
      std::vector<size_t> dims_;
      std::istream& in_;

      bool scan_single_char(char c_expected) {
        int c = in_.peek();
        if (in_.fail()) return false;
        if (c != c_expected)
          return false;
        char c_skip;
        in_.get(c_skip);
        return true;
      }

      bool scan_optional_long() {
        if (scan_single_char('l'))
          return true;
        else if (scan_single_char('L'))
          return true;
        else
          return false;
      }

      bool scan_char(char c_expected) {
        char c;
        in_ >> c;
        if (in_.fail()) return false;
        if (c != c_expected) {
          in_.putback(c);
          return false;
        }
        return true;
      }

      bool scan_name_unquoted() {
        char c;
        in_ >> c;
        if (in_.fail()) return false;
        if (!std::isalpha(c)) return false;
        name_.push_back(c);
        while (in_.get(c)) {  // get turns off auto space skip
          if (std::isalpha(c) || std::isdigit(c) || c == '_' || c == '.') {
            name_.push_back(c);
          } else {
            in_.putback(c);
            return true;
          }
        }
        return true;  // but hit eos
      }

      bool scan_name() {
        if (scan_char('"')) {
          if (!scan_name_unquoted()) return false;
          if (!scan_char('"')) return false;
        } else if (scan_char('\'')) {
          if (!scan_name_unquoted()) return false;
          if (!scan_char('\'')) return false;
        } else {
          if (!scan_name_unquoted()) return false;
        }
        return true;
      }


      bool scan_chars(const char *s, bool case_sensitive = true) {
        for (size_t i = 0; s[i]; ++i) {
          char c;
          if (!(in_ >> c)) {
            for (size_t j = 1; j < i; ++j)
              in_.putback(s[i-j]);
            return false;
          }
          // all ASCII, so toupper is OK
          if ((case_sensitive && c != s[i])
              || (!case_sensitive && ::toupper(c) != ::toupper(s[i]))) {
            in_.putback(c);
            for (size_t j = 1; j < i; ++j)
              in_.putback(s[i-j]);
            return false;
          }
        }
        return true;
      }

      bool scan_chars(std::string s, bool case_sensitive = true) {
        for (size_t i = 0; i < s.size(); ++i) {
          char c;
          if (!(in_ >> c)) {
            for (size_t j = 1; j < i; ++j)
              in_.putback(s[i-j]);
            return false;
          }
          // all ASCII, so toupper is OK
          if ((case_sensitive && c != s[i])
              || (!case_sensitive && ::toupper(c) != ::toupper(s[i]))) {
            in_.putback(c);
            for (size_t j = 1; j < i; ++j)
              in_.putback(s[i-j]);
            return false;
          }
        }
        return true;
      }

      size_t scan_dim() {
        char c;
        buf_.clear();
        while (in_.get(c)) {
          if (std::isspace(c)) continue;
          if (std::isdigit(c)) {
            buf_.push_back(c);
          } else {
            in_.putback(c);
            break;
          }
        }
        scan_optional_long();
        size_t d = 0;
        try {
          d = boost::lexical_cast<size_t>(buf_);
        }
        catch ( const boost::bad_lexical_cast &exc ) {
          std::string msg = "value " + buf_ + " beyond array dimension range";
          BOOST_THROW_EXCEPTION(std::invalid_argument(msg));
        }
        return d;
      }

      int scan_int() {
        char c;
        buf_.clear();
        while (in_.get(c)) {
          if (std::isspace(c)) continue;
          if (std::isdigit(c)) {
            buf_.push_back(c);
          } else {
            in_.putback(c);
            break;
          }
        }
        return(get_int());
      }

      int get_int() {
        int n = 0;
        try {
          n = boost::lexical_cast<int>(buf_);
        }
        catch ( const boost::bad_lexical_cast &exc ) {
          std::string msg = "value " + buf_ + " beyond int range";
          BOOST_THROW_EXCEPTION(std::invalid_argument(msg));
        }
        return n;
      }

      double scan_double() {
        double x = 0;
        try {
          x = boost::lexical_cast<double>(buf_);
          if (x == 0)
            validate_zero_buf(buf_);
        }
        catch ( const boost::bad_lexical_cast &exc ) {
          std::string msg = "value " + buf_ + " beyond numeric range";
          BOOST_THROW_EXCEPTION(std::invalid_argument(msg));
        }
        return x;
      }



      // scan number stores number or throws bad lexical cast exception
      void scan_number(bool negate_val) {
        // must take longest first!
        if (scan_chars("Inf")) {
          scan_chars("inity");  // read past if there
          stack_r_.push_back(negate_val
                             ? -std::numeric_limits<double>::infinity()
                             : std::numeric_limits<double>::infinity());
          return;
        }
        if (scan_chars("NaN", false)) {
          stack_r_.push_back(std::numeric_limits<double>::quiet_NaN());
          return;
        }

        char c;
        bool is_double = false;
        buf_.clear();
        while (in_.get(c)) {
          if (std::isdigit(c)) {  // before pre-scan || c == '-' || c == '+') {
            buf_.push_back(c);
          } else if (c == '.'
                     || c == 'e'
                     || c == 'E'
                     || c == '-'
                     || c == '+') {
            is_double = true;
            buf_.push_back(c);
          } else {
            in_.putback(c);
            break;
          }
        }
        if (!is_double && stack_r_.size() == 0) {
          int n = get_int();
          stack_i_.push_back(negate_val ? -n : n);
          scan_optional_long();
        } else {
          for (size_t j = 0; j < stack_i_.size(); ++j)
            stack_r_.push_back(static_cast<double>(stack_i_[j]));
          stack_i_.clear();
          double x = scan_double();
          stack_r_.push_back(negate_val ? -x : x);
        }
      }

      void scan_number() {
        char c;
        while (in_.get(c)) {
          if (std::isspace(c)) continue;
          in_.putback(c);
          break;
        }
        bool negate_val = scan_char('-');
        if (!negate_val) scan_char('+');  // flush leading +
        return scan_number(negate_val);
      }

      bool scan_zero_integers() {
        if (!scan_char('(')) return false;
        if (scan_char(')')) {
          dims_.push_back(0U);
          return true;
        }
        int s = scan_int();
        if (s < 0) return false;
        for (int i = 0; i < s; ++i) {
          stack_i_.push_back(0);
        }
        if (!scan_char(')')) return false;
        dims_.push_back(s);
        return true;
      }

      bool scan_zero_doubles() {
        if (!scan_char('(')) return false;
        if (scan_char(')')) {
          dims_.push_back(0U);
          return true;
        }
        int s = scan_int();
        if (s < 0) return false;
        for (int i = 0; i < s; ++i) {
          stack_r_.push_back(0);
        }
        if (!scan_char(')')) return false;
        dims_.push_back(s);
        return true;
      }


      bool scan_seq_value() {
        if (!scan_char('(')) return false;
        if (scan_char(')')) {
          dims_.push_back(0U);
          return true;
        }
        scan_number();  // first entry
        while (scan_char(',')) {
          scan_number();
        }
        dims_.push_back(stack_r_.size() + stack_i_.size());
        return scan_char(')');
      }

      bool scan_struct_value() {
        if (!scan_char('(')) return false;
        if (scan_chars("integer")) {
          scan_zero_integers();
        } else if (scan_chars("double")) {
          scan_zero_doubles();
        } else if (scan_char('c')) {
          scan_seq_value();
        } else {
          int start = scan_int();
          if (!scan_char(':'))
            return false;
          int end = scan_int();
          if (start <= end) {
            for (int i = start; i <= end; ++i)
              stack_i_.push_back(i);
          } else {
            for (int i = start; i >= end; --i)
              stack_i_.push_back(i);
          }
        }
        dims_.clear();
        if (!scan_char(',')) return false;
        if (!scan_char('.')) return false;
        if (!scan_chars("Dim")) return false;
        if (!scan_char('=')) return false;
        if (scan_char('c')) {
          if (!scan_char('(')) return false;
          size_t dim = scan_dim();
          dims_.push_back(dim);
          while (scan_char(',')) {
            dim = scan_dim();
            dims_.push_back(dim);
          }
          if (!scan_char(')')) return false;
        } else {
          size_t start = scan_dim();
          if (!scan_char(':'))
            return false;
          size_t end = scan_dim();
          if (start < end) {
            for (size_t i = start; i <= end; ++i)
              dims_.push_back(i);
          } else {
            for (size_t i = start; i >= end; --i)
              dims_.push_back(i);
          }
        }
        if (!scan_char(')')) return false;
        return true;
      }

      bool scan_value() {
        if (scan_char('c'))
          return scan_seq_value();
        if (scan_chars("integer"))
          return scan_zero_integers();
        if (scan_chars("double"))
          return scan_zero_doubles();
        if (scan_chars("structure"))
          return scan_struct_value();
        scan_number();
        if (!scan_char(':'))
          return true;
        if (stack_i_.size() != 1)
          return false;
        scan_number();
        if (stack_i_.size() != 2)
          return false;
        int start = stack_i_[0];
        int end = stack_i_[1];
        stack_i_.clear();
        if (start <= end) {
          for (int i = start; i <= end; ++i)
            stack_i_.push_back(i);
        } else {
          for (int i = start; i >= end; --i)
            stack_i_.push_back(i);
        }
        dims_.push_back(stack_i_.size());
        return true;
      }


    public:
      /**
       * Construct a reader for the specified input stream.
       *
       * @param in Input stream reference from which to read.
       */
      explicit dump_reader(std::istream& in) : in_(in) { }

      /**
       * Destroy this reader.
       */
      ~dump_reader() { }


      /**
       * Return the name of the most recently read variable.
       *
       * @return Name of most recently read variable.
       */
      std::string name() {
        return name_;
      }

      /**
       * Return the dimensions of the most recently
       * read variable.
       *
       * @return Last dimensions.
       */
      std::vector<size_t> dims() {
        return dims_;
      }

      /**
       * Checks if the last item read is integer.
       *
       * Return <code>true</code> if the value(s) in the most recently
       * read item are integer values and <code>false</code> if
       * they are floating point.
       */
      bool is_int() {
        // return stack_i_.size() > 0;
        return stack_r_.size() == 0;
      }

      /**
       * Returns the integer values from the last item if the
       * last item read was an integer and the empty vector otherwise.
       *
       * @return Integer values of last item.
       */
      std::vector<int> int_values() {
        return stack_i_;
      }

      /**
       * Returns the floating point values from the last item if the
       * last item read contained floating point values and the empty
       * vector otherwise.
       *
       * @return Floating point values of last item.
       */
      std::vector<double> double_values() {
        return stack_r_;
      }

      /**
       * Read the next value from the input stream, returning
       * <code>true</code> if successful and <code>false</code> if no
       * further input may be read.
       *
       * @return Return <code>true</code> if a fresh variable was read.
       * @throws bad_cast if bad number values encountered.
       */
      bool next() {
        stack_r_.clear();
        stack_i_.clear();
        dims_.clear();
        name_.erase();
        if (!scan_name())  // set name
          return false;
        if (!scan_char('<'))  // set <-
          return false;
        if (!scan_char('-'))
          return false;
        try {
          bool okSyntax = scan_value();  // set stack_r_, stack_i_, dims_
          if (!okSyntax) {
            std::string msg = "syntax error";
            BOOST_THROW_EXCEPTION(std::invalid_argument(msg));
          }
        }
        catch (const std::invalid_argument &e) {
          std::string msg = "data " + name_ + " " + e.what();
          BOOST_THROW_EXCEPTION(std::invalid_argument(msg));
        }
        return true;
      }
    };



    /**
     * Represents named arrays with dimensions.
     *
     * A dump object represents a dump of named arrays with dimensions.
     * The arrays may have any dimensionality.  The values for an array
     * are typed to double or int.
     *
     * <p>See <code>dump_reader</code> for more information on the format.
     *
     * <p>Dump objects are created from reading dump files from an
     * input stream.
     *
     * <p>The dimensions and values of variables
     * may be accessed by name.
     */
    class dump : public stan::io::var_context {
    private:
      std::map<std::string,
               std::pair<std::vector<double>,
                         std::vector<size_t> > > vars_r_;
      std::map<std::string,
               std::pair<std::vector<int>,
                         std::vector<size_t> > > vars_i_;
      std::vector<double> const empty_vec_r_;
      std::vector<int> const empty_vec_i_;
      std::vector<size_t> const empty_vec_ui_;
      /**
       * Return <code>true</code> if this dump contains the specified
       * variable name is defined in the real values. This method
       * returns <code>false</code> if the values are all integers.
       *
       * @param name Variable name to test.
       * @return <code>true</code> if the variable exists in the
       * real values of the dump.
       */
      bool contains_r_only(const std::string& name) const {
        return vars_r_.find(name) != vars_r_.end();
      }

    public:
      /**
       * Construct a dump object from the specified input stream.
       *
       * <b>Warning:</b> This method does not close the input stream.
       *
       * @param in Input stream from which to read.
       */
      explicit dump(std::istream& in) {
        dump_reader reader(in);
        while (reader.next()) {
          if (reader.is_int()) {
            vars_i_[reader.name()]
              = std::pair<std::vector<int>,
                          std::vector<size_t> >(reader.int_values(),
                                                reader.dims());

          } else {
            vars_r_[reader.name()]
              = std::pair<std::vector<double>,
                          std::vector<size_t> >(reader.double_values(),
                                                reader.dims());
          }
        }
      }

      /**
       * Return <code>true</code> if this dump contains the specified
       * variable name is defined. This method returns <code>true</code>
       * even if the values are all integers.
       *
       * @param name Variable name to test.
       * @return <code>true</code> if the variable exists.
       */
      bool contains_r(const std::string& name) const {
        return contains_r_only(name) || contains_i(name);
      }

      /**
       * Return <code>true</code> if this dump contains an integer
       * valued array with the specified name.
       *
       * @param name Variable name to test.
       * @return <code>true</code> if the variable name has an integer
       * array value.
       */
      bool contains_i(const std::string& name) const {
        return vars_i_.find(name) != vars_i_.end();
      }

      /**
       * Return the double values for the variable with the specified
       * name or null.
       *
       * @param name Name of variable.
       * @return Values of variable.
       */
      std::vector<double> vals_r(const std::string& name) const {
        if (contains_r_only(name)) {
          return (vars_r_.find(name)->second).first;
        } else if (contains_i(name)) {
          std::vector<int> vec_int = (vars_i_.find(name)->second).first;
          std::vector<double> vec_r(vec_int.size());
          for (size_t ii = 0; ii < vec_int.size(); ii++) {
            vec_r[ii] = vec_int[ii];
          }
          return vec_r;
        }
        return empty_vec_r_;
      }

      /**
       * Return the dimensions for the double variable with the specified
       * name.
       *
       * @param name Name of variable.
       * @return Dimensions of variable.
       */
      std::vector<size_t> dims_r(const std::string& name) const {
        if (contains_r_only(name)) {
          return (vars_r_.find(name)->second).second;
        } else if (contains_i(name)) {
          return (vars_i_.find(name)->second).second;
        }
        return empty_vec_ui_;
      }

      /**
       * Return the integer values for the variable with the specified
       * name.
       *
       * @param name Name of variable.
       * @return Values.
       */
      std::vector<int> vals_i(const std::string& name) const {
        if (contains_i(name)) {
          return (vars_i_.find(name)->second).first;
        }
        return empty_vec_i_;
      }

      /**
       * Return the dimensions for the integer variable with the specified
       * name.
       *
       * @param name Name of variable.
       * @return Dimensions of variable.
       */
      std::vector<size_t> dims_i(const std::string& name) const {
        if (contains_i(name)) {
          return (vars_i_.find(name)->second).second;
        }
        return empty_vec_ui_;
      }

      /**
       * Return a list of the names of the floating point variables in
       * the dump.
       *
       * @param names Vector to store the list of names in.
       */
      virtual void names_r(std::vector<std::string>& names) const {
        names.resize(0);
        for (std::map<std::string,
                      std::pair<std::vector<double>,
                                std::vector<size_t> > >
                 ::const_iterator it = vars_r_.begin();
             it != vars_r_.end(); ++it)
          names.push_back((*it).first);
      }

      /**
       * Return a list of the names of the integer variables in
       * the dump.
       *
       * @param names Vector to store the list of names in.
       */
      virtual void names_i(std::vector<std::string>& names) const {
        names.resize(0);
        for (std::map<std::string,
                      std::pair<std::vector<int>,
                                std::vector<size_t> > >
                 ::const_iterator it = vars_i_.begin();
             it != vars_i_.end(); ++it)
          names.push_back((*it).first);
      }

      /**
       * Remove variable from the object.
       *
       * @param name Name of the variable to remove.
       * @return If variable is removed returns <code>true</code>, else
       *   returns <code>false</code>.
       */
      bool remove(const std::string& name) {
        return (vars_i_.erase(name) > 0)
          || (vars_r_.erase(name) > 0);
      }
    };

  }

}
#endif
