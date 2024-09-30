/*******************************************************************************
 * tlx/cmdline_parser.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_CMDLINE_PARSER_HEADER
#define TLX_CMDLINE_PARSER_HEADER

#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

namespace tlx {

/*!

Command line parser which automatically fills variables and prints nice usage
messages.

This is a straightforward command line parser in C++, which will recognize short
options -s, long options --long and parameters, both required and optional. It
will automatically parse integers and <b>byte sizes</b> with SI/IEC suffixes
(e.g. 1 GiB). It also works with lists of strings, e.g. multiple filenames.

\snippet tests/cmdline_parser_example.cpp example

When running the program above without arguments, it will print:
\verbatim
$ ./tlx_cmdline_parser_example
Missing required argument for parameter 'filename'

Usage: ./tlx_cmdline_parser_example [options] <filename>

This may some day be a useful program, which solves many serious problems of
the real world and achives global peace.

Author: Timo Bingmann <tb@panthema.net>

Parameters:
  filename  A filename to process
Options:
  -r, --rounds N  Run N rounds of the experiment.
  -s, --size      Number of bytes to process.
\endverbatim

Nice output, notice the line wrapping of the description and formatting of
parameters and arguments. These too are wrapped if the description is too long.

We now try to give the program some arguments:
\verbatim
$ ./tlx_cmdline_parser_example -s 2GiB -r 42 /dev/null
Option -s, --size set to 2147483648.
Option -r, --rounds N set to 42.
Parameter filename set to "/dev/null".
Command line parsed okay.
Parameters:
  filename        (string)            "/dev/null"
Options:
  -r, --rounds N  (unsigned integer)  42
  -s, --size      (bytes)             2147483648
\endverbatim

The output shows pretty much what happens. The command line parser is by default
in a verbose mode outputting all arguments and values parsed. The debug summary
shows to have values the corresponding variables were set.

One feature worth naming is that the parser also supports lists of strings,
i.e. \c std::vector<std::string> via \ref CmdlineParser::add_param_stringlist()
and similar.
*/

class CmdlineParser
{
private:
    // forward declaration of Argument classes
    class Argument;
    class ArgumentBool;
    class ArgumentInt;
    class ArgumentUnsigned;
    class ArgumentSizeT;
    class ArgumentFloat;
    class ArgumentDouble;
    class ArgumentBytes32;
    class ArgumentBytes64;
    class ArgumentString;
    class ArgumentStringlist;

private:
    //! option and parameter list type
    using ArgumentList = std::vector<Argument*>;

    //! list of options available
    ArgumentList option_list_;
    //! list of parameters, both required and optional
    ArgumentList param_list_;

    //! formatting width for options, '-s, --switch <#>'
    size_t option_max_width_ = 8;
    //! formatting width for parameters, 'param <#>'
    size_t param_max_width_ = 8;

    //! argv[0] for usage.
    const char* program_name_ = nullptr;

    //! verbose processing of arguments
    bool verbose_process_ = false;

    //! user set description of program, will be wrapped
    std::string description_;
    //! user set author of program, will be wrapped
    std::string author_;

    //! set line wrap length
    unsigned int line_wrap_ = 80;

    //! maximum length of a type_name() result
    static constexpr int max_type_name_ = 16;

private:
    //! update maximum formatting width for new option
    void calc_option_max(const Argument* arg);

    //! update maximum formatting width for new parameter
    void calc_param_max(const Argument* arg);

public:
    //! Wrap a long string at spaces into lines. Prefix is added
    //! unconditionally to each line. Lines are wrapped after wraplen
    //! characters if possible.
    static void output_wrap(
        std::ostream& os, const std::string& text,
        size_t wraplen, size_t indent_first = 0, size_t indent_rest = 0,
        size_t current = 0, size_t indent_newline = 0);

public:
    //! Constructor
    CmdlineParser();

    //! Delete all added arguments
    ~CmdlineParser();

    //! Set description of program, text will be wrapped
    void set_description(const std::string& description);

    //! Set author of program, will be wrapped.
    void set_author(const std::string& author);

    //! Set verbose processing of command line arguments
    void set_verbose_process(bool verbose_process);

    /**************************************************************************/

    //! \name Add Option with short -k, --longkey, and description.
    //! \{

    //! add boolean option flag -key, --longkey with description and store to
    //! dest
    void add_bool(char key, const std::string& longkey, bool& dest, // NOLINT
                  const std::string& desc);

    //! add boolean option flag -key, --longkey with description and store to
    //! dest. identical to add_bool()
    void add_flag(char key, const std::string& longkey, bool& dest, // NOLINT
                  const std::string& desc);

    //! add signed integer option -key, --longkey with description and store to
    //! dest
    void add_int(char key, const std::string& longkey, int& dest, // NOLINT
                 const std::string& desc);

    //! add unsigned integer option -key, --longkey with description and store
    //! to dest
    void add_unsigned(char key, const std::string& longkey,
                      unsigned int& dest, const std::string& desc); // NOLINT

    //! add unsigned integer option -key, --longkey with description and store
    //! to dest. identical to add_unsigned()
    void add_uint(char key, const std::string& longkey,
                  unsigned int& dest, const std::string& desc); // NOLINT

    //! add size_t option -key, --longkey with description and store to dest
    void add_size_t(char key, const std::string& longkey,
                    size_t& dest, const std::string& desc); // NOLINT

    //! add float option -key, --longkey with description and store to dest
    void add_float(char key, const std::string& longkey,
                   float& dest, const std::string& desc); // NOLINT

    //! add double option -key, --longkey with description and store to dest
    void add_double(char key, const std::string& longkey,
                    double& dest, const std::string& desc); // NOLINT

    //! add SI/IEC suffixes byte size option -key, --longkey and store to 32-bit
    //! dest
    void add_bytes(char key, const std::string& longkey,
                   std::uint32_t& dest, const std::string& desc); // NOLINT

    //! add SI/IEC suffixes byte size option -key, --longkey and store to 64-bit
    //! dest
    void add_bytes(char key, const std::string& longkey,
                   std::uint64_t& dest, const std::string& desc); // NOLINT

    //! add string option -key, --longkey and store to dest
    void add_string(char key, const std::string& longkey,
                    std::string& dest, const std::string& desc); // NOLINT

    //! add string list option -key, --longkey and store to dest
    void add_stringlist(
        char key, const std::string& longkey,
        std::vector<std::string>& dest, const std::string& desc); // NOLINT

    //! \}

    /**************************************************************************/

    //! \name Add Option with --longkey and description.
    //! \{

    //! add boolean option flag --longkey with description and store to dest
    void add_bool(const std::string& longkey, bool& dest, // NOLINT
                  const std::string& desc);

    //! add boolean option flag --longkey with description and store to
    //! dest. identical to add_bool()
    void add_flag(const std::string& longkey, bool& dest, // NOLINT
                  const std::string& desc);

    //! add signed integer option --longkey with description and store to dest
    void add_int(const std::string& longkey, int& dest, // NOLINT
                 const std::string& desc);

    //! add unsigned integer option --longkey with description and store to dest
    void add_unsigned(const std::string& longkey,
                      unsigned int& dest, const std::string& desc); // NOLINT

    //! add unsigned integer option --longkey with description and store to
    //! dest. identical to add_unsigned()
    void add_uint(const std::string& longkey,
                  unsigned int& dest, const std::string& desc); // NOLINT

    //! add size_t option --longkey with description and store to dest
    void add_size_t(const std::string& longkey,
                    size_t& dest, const std::string& desc); // NOLINT

    //! add float option --longkey with description and store to dest
    void add_float(const std::string& longkey,
                   float& dest, const std::string& desc); // NOLINT

    //! add double option --longkey with description and store to dest
    void add_double(const std::string& longkey,
                    double& dest, const std::string& desc); // NOLINT

    //! add SI/IEC suffixes byte size option --longkey and store to 32-bit dest
    void add_bytes(const std::string& longkey,
                   std::uint32_t& dest, const std::string& desc); // NOLINT

    //! add SI/IEC suffixes byte size option --longkey and store to 64-bit dest
    void add_bytes(const std::string& longkey,
                   std::uint64_t& dest, const std::string& desc); // NOLINT

    //! add string option --longkey and store to dest
    void add_string(const std::string& longkey,
                    std::string& dest, const std::string& desc); // NOLINT

    //! add string list option --longkey and store to dest
    void add_stringlist(
        const std::string& longkey,
        std::vector<std::string>& dest, const std::string& desc); // NOLINT

    //! \}

    /**************************************************************************/

    //! \name Add Option with short -k, --longkey, [keytype], and description.
    //! \{

    //! add boolean option flag -key, --longkey [keytype] with description and
    //! store to dest
    void add_bool(
        char key, const std::string& longkey,
        const std::string& keytype, bool& dest, // NOLINT
        const std::string& desc);

    //! add boolean option flag -key, --longkey [keytype] with description and
    //! store to dest. identical to add_bool()
    void add_flag(
        char key, const std::string& longkey,
        const std::string& keytype, bool& dest, // NOLINT
        const std::string& desc);

    //! add signed integer option -key, --longkey [keytype] with description
    //! and store to dest
    void add_int(
        char key, const std::string& longkey,
        const std::string& keytype, int& dest, // NOLINT
        const std::string& desc);

    //! add unsigned integer option -key, --longkey [keytype] with description
    //! and store to dest
    void add_unsigned(
        char key, const std::string& longkey,
        const std::string& keytype, unsigned int& dest, // NOLINT
        const std::string& desc);

    //! add unsigned integer option -key, --longkey [keytype] with description
    //! and store to dest. identical to add_unsigned()
    void add_uint(
        char key, const std::string& longkey,
        const std::string& keytype, unsigned int& dest, // NOLINT
        const std::string& desc);

    //! add size_t option -key, --longkey [keytype] with description and store
    //! to dest
    void add_size_t(
        char key, const std::string& longkey,
        const std::string& keytype, size_t& dest, // NOLINT
        const std::string& desc);

    //! add float option -key, --longkey [keytype] with description and store
    //! to dest
    void add_float(
        char key, const std::string& longkey,
        const std::string& keytype, float& dest, // NOLINT
        const std::string& desc);

    //! add double option -key, --longkey [keytype] with description and store
    //! to dest
    void add_double(
        char key, const std::string& longkey,
        const std::string& keytype, double& dest, // NOLINT
        const std::string& desc);

    //! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
    //! store to 64-bit dest
    void add_bytes(
        char key, const std::string& longkey,
        const std::string& keytype, std::uint32_t& dest, // NOLINT
        const std::string& desc);

    //! add SI/IEC suffixes byte size option -key, --longkey [keytype] and
    //! store to 64-bit dest
    void add_bytes(
        char key, const std::string& longkey,
        const std::string& keytype, std::uint64_t& dest, // NOLINT
        const std::string& desc);

    //! add string option -key, --longkey [keytype] and store to dest
    void add_string(
        char key, const std::string& longkey,
        const std::string& keytype, std::string& dest, // NOLINT
        const std::string& desc);

    //! add string list option -key, --longkey [keytype] and store to dest
    void add_stringlist(
        char key, const std::string& longkey,
        const std::string& keytype, std::vector<std::string>& dest, // NOLINT
        const std::string& desc);

    //! \}

    // ************************************************************************

    //! \name Add Required Parameter [name] with description.
    //! \{

    //! add signed integer parameter [name] with description and store to dest
    void add_param_int(
        const std::string& name, int& dest, const std::string& desc); // NOLINT

    //! add unsigned integer parameter [name] with description and store to dest
    void add_param_unsigned(
        const std::string& name, unsigned int& dest, // NOLINT
        const std::string& desc);

    //! add unsigned integer parameter [name] with description and store to
    //! dest. identical to add_unsigned()
    void add_param_uint(
        const std::string& name, unsigned int& dest, // NOLINT
        const std::string& desc);

    //! add size_t parameter [name] with description and store to dest
    void add_param_size_t(
        const std::string& name, size_t& dest, // NOLINT
        const std::string& desc);

    //! add float parameter [name] with description and store to dest
    void add_param_float(
        const std::string& name, float& dest, // NOLINT
        const std::string& desc);

    //! add double parameter [name] with description and store to dest
    void add_param_double(
        const std::string& name, double& dest, // NOLINT
        const std::string& desc);

    //! add SI/IEC suffixes byte size parameter [name] with description and
    //! store to dest
    void add_param_bytes(
        const std::string& name, std::uint32_t& dest, // NOLINT
        const std::string& desc);

    //! add SI/IEC suffixes byte size parameter [name] with description and
    //! store to dest
    void add_param_bytes(
        const std::string& name, std::uint64_t& dest, // NOLINT
        const std::string& desc);

    //! add string parameter [name] with description and store to dest
    void add_param_string(
        const std::string& name, std::string& dest, // NOLINT
        const std::string& desc);

    //! add string list parameter [name] with description and store to dest.
    //! \warning this parameter must be last, as it will gobble all non-option
    //! arguments!
    void add_param_stringlist(
        const std::string& name, std::vector<std::string>& dest, // NOLINT
        const std::string& desc);

    //! \}

    /**************************************************************************/

    //! \name Add Optional Parameter [name] with description.
    //! \{

    //! add optional signed integer parameter [name] with description and store
    //! to dest
    void add_opt_param_int(
        const std::string& name, int& dest, const std::string& desc); // NOLINT

    //! add optional unsigned integer parameter [name] with description and
    //! store to dest
    void add_opt_param_unsigned(
        const std::string& name, unsigned int& dest, // NOLINT
        const std::string& desc);

    //! add optional unsigned integer parameter [name] with description and
    //! store to dest. identical to add_unsigned()
    void add_opt_param_uint(
        const std::string& name, unsigned int& dest, // NOLINT
        const std::string& desc);

    //! add optional size_t parameter [name] with description and store to dest
    void add_opt_param_size_t(
        const std::string& name, size_t& dest, // NOLINT
        const std::string& desc);

    //! add optional float parameter [name] with description and store to dest
    void add_opt_param_float(
        const std::string& name, float& dest, // NOLINT
        const std::string& desc);

    //! add optional double parameter [name] with description and store to dest
    void add_opt_param_double(
        const std::string& name, double& dest, // NOLINT
        const std::string& desc);

    //! add optional SI/IEC suffixes byte size parameter [name] with
    //! description and store to dest
    void add_opt_param_bytes(
        const std::string& name, std::uint32_t& dest, // NOLINT
        const std::string& desc);

    //! add optional SI/IEC suffixes byte size parameter [name] with
    //! description and store to dest
    void add_opt_param_bytes(
        const std::string& name, std::uint64_t& dest, // NOLINT
        const std::string& desc);

    //! add optional string parameter [name] with description and store to dest
    void add_opt_param_string(
        const std::string& name, std::string& dest, // NOLINT
        const std::string& desc);

    //! add optional string parameter [name] with description and store to dest
    //! \warning this parameter must be last, as it will gobble all non-option
    //! arguments!
    void add_opt_param_stringlist(
        const std::string& name, std::vector<std::string>& dest, // NOLINT
        const std::string& desc);

    //! \}

    /**************************************************************************/

    //! output nicely formatted usage information including description of all
    //! parameters and options.
    void print_usage(std::ostream& os);

    //! output to std::cout nicely formatted usage information including
    //! description of all parameters and options.
    void print_usage();

private:
    //! print error about option.
    void print_option_error(int argc, const char* const* argv,
                            const Argument* arg, std::ostream& os);

    //! print error about parameter.
    void print_param_error(int argc, const char* const* argv,
                           const Argument* arg, std::ostream& os);

public:
    //! sort options by key (but not the positional parameters)
    CmdlineParser& sort();

    //! parse command line options as specified by the options and parameters
    //! added.
    //! \return true if command line is okay and all required parameters are
    //! present.
    bool process(
        int argc, const char* const* argv, std::ostream& os);

    //! parse command line options as specified by the options and parameters
    //! added.
    //! \return true if command line is okay and all required parameters are
    //! present.
    bool process(int argc, const char* const* argv);

    //! print nicely formatted result of processing
    void print_result(std::ostream& os);

    //! print nicely formatted result of processing to std::cout
    void print_result();
};

} // namespace tlx

#endif // !TLX_CMDLINE_PARSER_HEADER

/******************************************************************************/
