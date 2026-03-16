#ifndef STAN_LANG_GENERATOR_GENERATE_PROGRAM_READER_FUN_HPP
#define STAN_LANG_GENERATOR_GENERATE_PROGRAM_READER_FUN_HPP

#include <stan/io/program_reader.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate a top-level function that returns the program reader
     * for the specified history.
     *
     * <p>Implementation note: Because this is only called when there
     * is an error to report, reconstructing on each call has
     * acceptable performance.
     *
     * @param[in] history record of I/O path for lines in compound program
     * @param[in, out] o stream to which generated code is written
     */
    void
    generate_program_reader_fun(const std::vector<io::preproc_event>& history,
                                std::ostream& o) {
      o << "stan::io::program_reader prog_reader__() {" << std::endl;
      o << INDENT << "stan::io::program_reader reader;" << std::endl;
      for (size_t i = 0; i < history.size(); ++i)
        o << INDENT << "reader.add_event("
          << history[i].concat_line_num_
          << ", " << history[i]. line_num_
          << ", \"" << history[i].action_ << "\""
          << ", \"" << history[i].path_ << "\");" << std::endl;
      o << INDENT << "return reader;" << std::endl;
      o << "}" << std::endl << std::endl;
    }

  }
}
#endif
