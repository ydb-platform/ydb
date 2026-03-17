#ifndef STAN_IO_CMD_LINE_HPP
#define STAN_IO_CMD_LINE_HPP

#include <map>
#include <ostream>
#include <set>
#include <string>
#include <sstream>
#include <vector>

namespace stan {

  namespace io {

    /**
     * Print help option with padding.
     *
     * Print 2 spaces, the specified help option, then pad
     * to the specified width with spaces.  If there is not
     * room for at least 2 padding spaces, start a new line
     * and pad out to width.
     *
     * @param option Option to print (default to empty string).
     * @param width Width of option (defaults to 20).
     * @param o Output stream ptr, default to null
     */
    inline void pad_help_option(std::ostream* o,
                                const std::string& option = "",
                                unsigned int width = 20) {
      if (!o) return;
      *o << "  " << option;
      int padding = width - option.size();
      if (padding < 2) {
        *o << std::endl;
        padding = width + 2;  // 2 is
      }
      for (int i = 0; i < padding; ++i)
        *o << ' ';
    }

    /**
     * Prints single print option to output ptr if non-null.
     *
     * @param o
     * @param key_val
     * @param msg
     * @param note
     */
    inline void print_help_helper(std::ostream* o,
                                  const std::string& key_val,
                                  const std::string& msg,
                                  const std::string& note = "") {
      if (!o) return;
      pad_help_option(o, key_val);
      *o << msg
         << std::endl;
      if (note.size() > 0) {
        pad_help_option(o, "");
        *o << "    (" << note << ")"
           << std::endl;
      }
      *o << std::endl;
    }

    /**
     * Prints single print option to output ptr if non-null.
     *
     * @param o
     * @param key
     * @param value_type
     * @param msg
     * @param note
     */
    inline void print_help_option(std::ostream* o,
                                  const std::string& key,
                                  const std::string& value_type,
                                  const std::string& msg,
                                  const std::string& note = "") {
      std::stringstream ss;
      ss << "--" << key;
      if (value_type.size() > 0)
        ss << "=<" << value_type << ">";
      print_help_helper(o, ss.str(), msg, note);
    }

    /**
     * Parses and stores command-line arguments.
     *
     * <p>Command-line arguments are organized into four types.
     *
     * <p><b>Command</b>: The first argument (at index 0) is just the
     * command itself.  There method <code>command()</code> retrieves
     * the command.
     *
     * <p><b>Key/Value</b>: The second type of argument is a key-value pair,
     * which must be in the form <code>--key=val</code>.  Two hyphens
     * are used to separate arguments from negated numbers.  The method
     * <code>has_key(const std::string&)</code> indicates if there is a key
     * and <code>val(const std::string&,T&)</code> writes its value into
     * a reference (whose type is templated; any type understand by the
     * output operator <code>&gt;&gt;</code> is acceptable.
     *
     * <p><b>Flag</b>: Flags are specified as <code>--flag</code>.  The
     * method <code>has_flag(const std::string&)</code> tests if a flag
     * is present.
     *
     * <p><b>Bare Argument</b>: Bare arguments are any arguments that
     * are not prefixed with two hyphens (<code>--</code>).  The
     * method <code>bare_size()</code> returns the number of bare
     * arguments and they are retrieved with the generic method
     * <code>bare(const std::string&,T&)</code>.
     */
    class cmd_line {
    private:
      std::string cmd_;
      std::map<std::string, std::string> key_val_;
      std::set<std::string> flag_;
      std::vector<std::string> bare_;
      void parse_arg(const std::string& s) {
        if (s.size() < 2
            || s[0] != '-'
            || s[1] != '-') {
          bare_.push_back(s);
          return;
        }
        for (size_t i = 2; i < s.size(); ++i) {
          if (s[i] == '=') {
            key_val_[s.substr(2, i - 2)] = s.substr(i + 1, s.size() - i - 1);
            return;
          }
        }
        flag_.insert(s.substr(2, s.size()));
      }

    public:
      /**
       * Construct a command-line argument object from the specified
       * command-line arguments.
       *
       * @param argc Number of arguments.
       * @param argv Argument strings.
       */
      cmd_line(int argc, const char* argv[])
        : cmd_(argv[0]) {
        for (int i = 1; i < argc; ++i)
          parse_arg(argv[i]);
      }

      /**
       * Returns the name of the command itself.  The
       * command is always supplied as the first argument
       * (at index 0).
       *
       * @return Name of command.
       */
      std::string command() {
        return cmd_;
      }

      /**
       * Return <code>true</code> if the specified key is defined.
       *
       * @param key Key to test.
       * @return <code>true</code> if it has a value.
       */
      bool has_key(const std::string& key) const {
        return key_val_.find(key) != key_val_.end();
      }

      /**
       * Returns the value for the key provided.
       *
       * If the specified key is defined, write the value of the key
       * into the specified reference and return <code>true</code>,
       * otherwise do not modify the reference and return
       * <code>false</code>.
       *
       * <p>The conversions defined by <code>std::ostream</code>
       * are used to convert the base string value to the specified
       * type.  Thus this method will work as long as <code>operator>>()</code>
       * is defined for the specified type.
       *
       * @param[in] key Key whose value is returned.
       * @param[out] x Reference to value.
       * @return False if the key is not found, and true if
       * it is found.
       * @tparam Type of value.
       */
      template <typename T>
      inline bool val(const std::string& key, T& x) const {
        if (!has_key(key))
          return false;
        std::stringstream s(key_val_.find(key)->second);
        s >> x;
        return true;
      }

      /**
       * Return <code>true</code> if the specified flag is defined.
       *
       * @param flag Flag to test.
       * @return <code>true</code> if flag is defined.
       */
      bool has_flag(const std::string& flag) const {
        return flag_.find(flag) != flag_.end();
      }

      /**
       * Return the number of bare arguments.
       *
       * @return Number of bare arguments.
       */
      inline size_t bare_size() const {
        return bare_.size();
      }

      /**
       * Returns the bare argument.
       *
       * If the specified index is valid for bare arguments,
       * write the bare argument at the specified index into
       * the specified reference, and otherwise return false
       * without modifying the reference.
       *
       * @param[in] n Bare argument position.
       * @param[out] x Reference to result.
       * @return <code>true</code> if there were enough bare arguments.
       * @tparam T Type of value returned.
       */
      template <typename T>
      inline bool bare(size_t n, T& x) const {
        if (n >= bare_.size())
          return false;
        std::stringstream s(bare_[n]);
        s >> x;
        return true;
      }


      /**
       * Print a human readable parsed form of the command-line
       * arguments to the specified output stream.
       *
       * @param[out] out Output stream.
       */
      void print(std::ostream& out) const {
        out << "COMMAND=" << cmd_ << '\n';
        size_t flag_count = 0;
        for (std::set<std::string>::const_iterator it = flag_.begin();
             it != flag_.end();
             ++it) {
          out << "FLAG " << flag_count << "=" << (*it) << '\n';
          ++flag_count;
        }
        size_t key_val_count = 0;
        for (std::map<std::string, std::string>::const_iterator it
               = key_val_.begin();
             it != key_val_.end();
             ++it) {
          out << "KEY " << key_val_count << "=" << (*it).first;
          out << " VAL " << key_val_count << "=" << (*it).second << '\n';
          ++key_val_count;
        }
        size_t bare_count = 0;
        for (size_t i = 0; i < bare_.size(); ++i) {
          out << "BARE ARG " << bare_count << "=" << bare_[i] << '\n';
          ++bare_count;
        }
      }
    };

    // explicit instantation for std::string to allow for spaces
    // in bare_[n]
    template <>
    inline bool cmd_line::bare<std::string>(size_t n, std::string& x) const {
      if (n >= bare_.size())
        return false;
      x = bare_[n];
      return true;
    }

    // explicit instantation for std::string to allow for spaces
    // in key_val_
    template <>
    inline bool cmd_line::val<std::string>(const std::string& key,
                                           std::string& x) const {
      if (!has_key(key))
        return false;
      x = key_val_.find(key)->second;
      return true;
    }
  }
}

#endif
