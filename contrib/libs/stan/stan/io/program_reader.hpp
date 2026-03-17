#ifndef STAN_IO_PROGRAM_READER_HPP
#define STAN_IO_PROGRAM_READER_HPP

#include <stan/io/ends_with.hpp>
#include <stan/io/read_line.hpp>
#include <stan/io/starts_with.hpp>
#include <stan/io/trim_spaces.hpp>
#include <cstdio>
#include <istream>
#include <iostream>
#include <fstream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace stan {
namespace io {

/**
 * Structure to hold preprocessing events, which consist of (a)
 * line number in concatenated program after includes, (b) line
 * number in the stream from which the text is read, (c) a
 * string-based action, and (d) a path to the current file.
 */
struct preproc_event {
  int concat_line_num_;
  int line_num_;
  std::string action_;
  std::string path_;

  preproc_event(int concat_line_num, int line_num,
                const std::string& action, const std::string& path)
      : concat_line_num_(concat_line_num), line_num_(line_num),
        action_(action), path_(path) { }

  void print(std::ostream& out) {
    out << "(" << concat_line_num_ << ", " << line_num_
        << ", " << action_ << ", " << path_ << ")";
  }
};

/**
 * A <code>program_reader</code> reads a Stan program and unpacks
 * the include statements relative to a search path in such a way
 * that error messages can reproduce the include path.
 */
class program_reader {
 public:
  /**
   * A pair for holding a path and a line number.
   */
  typedef std::pair<std::string, int> path_line_t;

  /**
   * Ordered sequence of path and line number pairs.
   */
  typedef std::vector<path_line_t> trace_t;

  /**
   * Construct a program reader from the specified stream derived
   * from the specified name or path, and a sequence of paths to
   * search for include files.  The paths should be directories.
   *
   * <p>Calling this method does not close the specified input stream.
   *
   * @param[in] in stream from which to start reading
   * @param[in] name name path or name attached to stream
   * @param[in] search_path ordered sequence of directory names to
   * search for included files
   */
  program_reader(std::istream& in, const std::string& name,
                 const std::vector<std::string>& search_path) {
    int concat_line_num = 0;
    read(in, name, search_path, concat_line_num);
  }

  static std::string trim_comment(const std::string& line) {
    for (std::size_t i = 0; i < line.size(); ++i)
      if (starts_with("//", line.substr(i)))
        return line.substr(0, i);
    return line;  // no comments
  }

  /**
   * Construct a copy of the specified reader.  Both the
   * underlying program string and history will be copied.
   *
   * @param r reader to copy
   */
  program_reader(const program_reader& r)
      : program_(r.program_.str()), history_(r.history_) { }

  /**
   * Construct a program reader with an empty program and
   * history.
   */
  program_reader() : program_(""), history_() { }

  /**
   * Return a string representing the concatenated program.  This
   * string may be wrapped in a <code>std::stringstream</code> for
   * reading.
   *
   * @return stream for program
   */
  std::string program() const {
    return program_.str();
  }

  /**
   * Return the include trace of the path and line numbers leading
   * to the specified line of text in the concatenated program.
   * The top of the stack is the most recently read path.
   *
   * @param[in] target line number in concatenated program file
   * @return sequence of files and positions for includes
   */
  trace_t trace(int target) const {
    if (target < 1)
      throw std::runtime_error("trace() argument target must be"
                               " greater than 1");
    trace_t result;
    std::string file = "ERROR: UNINITIALIZED";
    int file_start = -1;
    int concat_start = -1;
    for (size_t i = 0; i < history_.size(); ++i) {
      if (target <= history_[i].concat_line_num_) {
        int line = file_start + target - concat_start;
        result.push_back(path_line_t(file, line));
        return result;
      } else if (history_[i].action_ == "start"
                 || history_[i].action_ == "restart" ) {
        file = history_[i].path_;
        file_start = history_[i].line_num_;
        concat_start = history_[i].concat_line_num_;
      } else if (history_[i].action_ == "end") {
        if (result.size() == 0) break;
        result.pop_back();
      } else if (history_[i].action_ == "include") {
        result.push_back(path_line_t(file, history_[i].line_num_ + 1));
      }
    }
    throw std::runtime_error("ran beyond end of program in trace()");
  }

  /**
   * Return the record of the files and includes used to build up
   * this program.
   *
   * @return I/O history of the program
   */
  const std::vector<preproc_event>& history() const {
    return history_;
  }

  /**
   * Adds preprocessing event with specified components to the
   * back of the history sequence.
   *
   * @param[in] concat_line_num position in concatenated program
   * @param[in] line_num position in current file
   * @param[in] action purpose of preprocessing event
   * @param[in] path location of current file
   */
  void add_event(int concat_line_num, int line_num,
                 const std::string& action, const std::string& path) {
    preproc_event e(concat_line_num, line_num, action, path);
    history_.push_back(e);
  }

 private:
  std::stringstream program_;
  std::vector<preproc_event> history_;

  /**
   * Returns the include path from a line that begins with
   * <code>#include</code> after whitespace.  A path may be a single
   * token or it must be quoted with double quote characters.  Line or
   * block comments are allowed after the include.
   *
   * Does not yet support included file names with double quotes in
   * the name.
   *
   * @param line line of text beginning with <code>#include</code>
   * @return text after <code>#include</code> with whitespace
   * trimmed
   */
  static std::string include_path(const std::string& line) {
    // trim out the initial spaces, #include, and spaces after and advance
    std::string trimmed_line = trim_comment(trim_spaces(line));
    std::size_t start = std::string("#include").size();
    while (is_whitespace(line[start]) && start < trimmed_line.size())
      ++start;
    std::string rest = trimmed_line.substr(start);

    // deal with case where there is nothing left
    if (rest.size() == 0) {
      throw std::runtime_error("***nothing after #include***");
    }

    // extract include path and line position after path
    std::string path;
    std::size_t pos = 0;
    if (rest[pos] == '"') {
      // quoted case
      ++pos;
      while (pos < rest.size() && rest[pos] != '"')
        ++pos;
      return rest.substr(1, pos - 1);
    }
    while (pos < rest.size() && !is_whitespace(rest[pos]))
      ++pos;
    return rest.substr(0, pos);  // pos past last char
  }

  void read(std::istream& in, const std::string& path,
            const std::vector<std::string>& search_path,
            int& concat_line_num, bool is_nested,
            std::set<std::string>& visited_paths) {
    if (visited_paths.find(path) != visited_paths.end())
      return;  // avoids recursive visitation
    visited_paths.insert(path);
    history_.push_back(preproc_event(concat_line_num, 0, "start", path));
    for (int line_num = 1; ; ++line_num) {
      std::string line = read_line(in);
      if (line.empty()) {
        // ends initial out of loop start event
        if (!is_nested) {
          // pad end concat_line_num of outermost file in order to properly
          // report end-of-file parse error - else trace throws exception
          history_.push_back(preproc_event(concat_line_num + 2,
                                           line_num - 1, "end", path));
        } else {
          history_.push_back(preproc_event(concat_line_num,
                                           line_num - 1, "end", path));
        }
        break;
      } else if (starts_with("#include ", trim_spaces(line))) {
        std::string incl_path = include_path(line);
        history_.push_back(preproc_event(concat_line_num, line_num - 1,
                                         "include", incl_path));
        bool found_path = false;
        for (size_t i = 0; i < search_path.size(); ++i) {
          std::string f
              = (search_path[i].size() != 0
                 && !ends_with("/", search_path[i])
                 && !ends_with("\\", search_path[i]))
              ? search_path[i] + "/" + incl_path  // / will work under Windows
              : search_path[i] + incl_path;

          std::ifstream include_in(f.c_str());
          try {
            if (!include_in.good()) {
              include_in.close();
              continue;
            }
            read(include_in, incl_path, search_path, concat_line_num, true,
                 visited_paths);
          } catch (...) {
            include_in.close();
            throw;
          }
          include_in.close();
          history_.push_back(preproc_event(concat_line_num, line_num,
                                           "restart", path));
          found_path = true;
          break;
        }
        if (!found_path) {
          std::ostringstream include_err_msg;

          include_err_msg << "could not find include file " << incl_path
                          << " in the following directories:\n";

          for (size_t i = 0; i < search_path.size(); ++i) {
            include_err_msg << "    " << search_path[i] << "\n";
          }

          throw std::runtime_error(include_err_msg.str());
        }
      } else {
        ++concat_line_num;
        program_ << line;
      }
    }
    visited_paths.erase(path);  // allow multiple, just not nested
  }


  /**
   * Read the rest of a program from the specified input stream in
   * the specified path, with the specified search path for
   * include files, and incrementing the specified concatenated
   * line number.  This method is called recursively for included
   * files.  If a file is included recursively, the second include
   * is ignored.
   *
   * @param[in] in stream from which to read
   * @param[in] path name of stream
   * @param[in] search_path sequence of path names to search for
   * include files
   * @param[in,out] concat_line_num position in concatenated file
   * to be updated
   * @throw std::runtime_error if an included file cannot be found
   */
  void read(std::istream& in, const std::string& path,
            const std::vector<std::string>& search_path,
            int& concat_line_num) {
    std::set<std::string> visited_paths;
    read(in, path, search_path, concat_line_num, false, visited_paths);
  }
};

}
}
#endif
