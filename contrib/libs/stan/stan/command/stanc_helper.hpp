#ifndef STAN_COMMAND_STANC_HELPER_HPP
#define STAN_COMMAND_STANC_HELPER_HPP

#include <boost/tokenizer.hpp>
#include <stan/version.hpp>
#include <stan/lang/compiler.hpp>
#include <stan/lang/compile_functions.hpp>
#include <stan/io/cmd_line.hpp>
#include <exception>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

/**
 * Print the version of stanc with major, minor and patch.
 *
 * @param[in,out] out_stream stream to which version is written.
 */
inline void print_version(std::ostream* out_stream) {
  if (!out_stream) return;
  *out_stream << "stanc version "
              << stan::MAJOR_VERSION
              << "."
              << stan::MINOR_VERSION
              << "."
              << stan::PATCH_VERSION
              << std::endl;
}

/**
 * Prints the Stan compiler (stanc) help.
 *
 * @param[in,out] out_stream stream to which help is written
 */
inline void print_stanc_help(std::ostream* out_stream) {
  using stan::io::print_help_option;

  if (!out_stream) return;

  *out_stream << std::endl;
  print_version(out_stream);
  *out_stream << std::endl;

  *out_stream << "USAGE:  " << "stanc [options] <model_file>" << std::endl;
  *out_stream << std::endl;

  *out_stream << "OPTIONS:" << std::endl;
  *out_stream << std::endl;

  print_help_option(out_stream, "help", "", "Display this information");

  print_help_option(out_stream, "version", "", "Display stanc version number");

  print_help_option(out_stream, "name", "string",
                    "Model name",
                    "default = \"$model_filename_model\"");

  print_help_option(out_stream, "o", "file",
                    "Output file for generated C++ code",
                    "default = \"$name.cpp\"");

  print_help_option(out_stream, "allow_undefined", "",
                    "Do not fail if a function is declared but not defined");

  print_help_option(out_stream, "include_paths", "comma-separated list",
                    "Comma-separated list of directories that may contain a "
                    "file in an #include directive");
  // TODO(martincerny) help for standalone function compilation
}

/**
 * Delte the file at the specified path, writing messages to error
 * stream if not possible.  Do nothing on zero size file name input.
 * Only write to error stream if it is non-null.
 *
 * @param[in,out] err_stream stream to which error messages are
 * written
 * @param[in] file_name path of file
 */
inline void delete_file(std::ostream* err_stream,
                        const std::string& file_name) {
  if (file_name.size() == 0)
    return;
  int return_code = std::remove(file_name.c_str());
  if (return_code != 0)
    if (err_stream)
      *err_stream << "Could not remove output file=" << file_name
                  << std::endl;
}

/**
 * Transform a provided input file name into a valid C++ identifier
 * @param[in] in_file_name the name of the input file
 * @return a valid C++ identifier based on the file name.
 */
inline std::string identifier_from_file_name(const std::string& in_file_name) {
  size_t slashInd = in_file_name.rfind('/');
  size_t ptInd = in_file_name.rfind('.');
  if (ptInd == std::string::npos)
    ptInd = in_file_name.length();
  if (slashInd == std::string::npos) {
    slashInd = in_file_name.rfind('\\');
  }
  if (slashInd == std::string::npos) {
    slashInd = 0;
  } else {
    slashInd++;
  }
  std::string result =
    in_file_name.substr(slashInd, ptInd - slashInd);
  for (std::string::iterator strIt = result.begin();
       strIt != result.end(); strIt++) {
    if (!isalnum(*strIt) && *strIt != '_') {
      *strIt = '_';
    }
  }

  return result;
}


/**
 * Check whether a given file has the specified extension.
 *
 * @param[in] file_name The name of the file
 * @param[in] extension The extension (WITHOUT dot)- e.g. "stan".
 * @return true if the file has the extension
 */
inline bool has_extension(const std::string& file_name,
                          const std::string& extension) {
  if (file_name.length() >= extension.length() + 1) {  // +1 for the dot
    if (0 == file_name.compare (file_name.length() - extension.length(),
                                extension.length(), extension)
        && file_name[file_name.length() - extension.length() - 1] == '.')
      return true;
    else
      return false;
  } else {
    return false;
  }
}

/**
 * Test whether a given string is a valid C++ identifier and throw
 * an exception when it is not.
 * @param[in] identifier the identifier to be checked
 * @param[in] identifier_type the type of the identifier to be reported
 * in error messages
 */
inline void check_identifier(const std::string& identifier,
                             const std::string& identifier_type) {
  if (!isalpha(identifier[0]) && identifier[0] != '_') {
    std::string msg(identifier_type + " must not start with a "
                    "number or symbol other than _");
    throw std::invalid_argument(msg);
  }
  for (std::string::const_iterator strIt = identifier.begin();
       strIt != identifier.end(); strIt++) {
    if (!isalnum(*strIt) && *strIt != '_') {
      std::string msg(identifier_type
                      + " must contain only letters, numbers and _");
      throw std::invalid_argument(msg);
    }
  }
}

/**
 * Invoke the stanc command on the specified argument list, writing
 * output and error messages to the specified streams, return a return
 * code.
 *
 * <p>The return codes are: 0 for success, -1 for an exception,
 * -2 is parsing failed, and -3 if there are invalid arguments.
 *
 * @param[in] argc number of arguments
 * @param[in] argv arguments
 * @param[in,out] out_stream stream to which output is written
 * @param[in,out] err_stream stream to which error messages are
 * written
 * @return return code
 */
inline int stanc_helper(int argc, const char* argv[],
                        std::ostream* out_stream, std::ostream* err_stream) {
  enum CompilationType {
    kModel,
    kStandaloneFunctions
  };
  static const int SUCCESS_RC = 0;
  static const int EXCEPTION_RC = -1;
  static const int PARSE_FAIL_RC = -2;
  static const int INVALID_ARGUMENT_RC = -3;

  std::string out_file_name;  // declare outside of try to delete in catch

  try {
    stan::io::cmd_line cmd(argc, argv);

    if (cmd.has_flag("help")) {
      print_stanc_help(out_stream);
      return SUCCESS_RC;
    }

    if (cmd.has_flag("version")) {
      print_version(out_stream);
      return SUCCESS_RC;
    }

    if (cmd.bare_size() != 1) {
      std::string msg("Require model file as argument. ");
      throw std::invalid_argument(msg);
    }
    std::string in_file_name;
    cmd.bare(0, in_file_name);

    CompilationType compilation_type;
    if (has_extension(in_file_name, "stanfuncs")) {
      compilation_type = kStandaloneFunctions;
    } else {
      compilation_type = kModel;
    }

    std::ifstream in(in_file_name.c_str());
    if (!in.is_open()) {
      std::stringstream msg;
      msg << "Failed to open model file "
          <<  in_file_name.c_str();
      throw std::invalid_argument(msg.str());
    }

    std::vector<std::string> include_paths;
    include_paths.push_back("");

    if (cmd.has_key("include_paths")) {
      std::string extra_path_str;
      cmd.val("include_paths", extra_path_str);

      // extra_path_els is given explicitly so that multiple quote
      // characters (in this case single and double quotes) can be
      // used.
      boost::escaped_list_separator<char> extra_path_els("\\",
                                                         ",",
                                                         "\"'");

      boost::tokenizer<
        boost::escaped_list_separator<char>
        > extra_path_tokenizer(extra_path_str, extra_path_els);

      for (const auto & inc_path : extra_path_tokenizer) {
        if (!inc_path.empty()) {
          include_paths.push_back(inc_path);
        }
      }
    }

    bool allow_undefined = cmd.has_flag("allow_undefined");

    bool valid_input = false;

    switch (compilation_type) {
    case kModel: {
      std::string model_name;
      if (cmd.has_key("name")) {
        cmd.val("name", model_name);
      } else {
        model_name = identifier_from_file_name(in_file_name) + "_model";
      }

      // TODO(martincerny) Check that the -namespace flag is not set

      if (cmd.has_key("o")) {
        cmd.val("o", out_file_name);
      } else {
        out_file_name = model_name;
        // TODO(carpenter): shouldn't this be .hpp without a main()?
        out_file_name += ".cpp";
      }

      check_identifier(model_name, "model_name");

      std::fstream out(out_file_name.c_str(), std::fstream::out);
      if (out_stream) {
        *out_stream << "Model name=" << model_name << std::endl;
        *out_stream << "Input file=" << in_file_name << std::endl;
        *out_stream << "Output file=" << out_file_name << std::endl;
      }
      if (!out.is_open()) {
        std::stringstream msg;
        msg << "Failed to open output file "
            <<  out_file_name.c_str();
        throw std::invalid_argument(msg.str());
      }

      valid_input = stan::lang::compile(err_stream, in, out, model_name,
                                        allow_undefined, in_file_name,
                                        include_paths);
      out.close();
      break;
    }
    case kStandaloneFunctions: {
      if (cmd.has_key("o")) {
        cmd.val("o", out_file_name);
      } else {
        out_file_name = identifier_from_file_name(in_file_name);
        out_file_name += ".hpp";
      }

      // TODO(martincerny) Allow multiple namespaces
      // (split namespace argument by "::")
      std::vector<std::string> namespaces;
      if (cmd.has_key("namespace")) {
        std::string ns;
        cmd.val("namespace", ns);
        namespaces.push_back(ns);
      } else {
        namespaces.push_back(identifier_from_file_name(in_file_name)
                             + "_functions");
      }

      // TODO(martincerny) Check that the -name flag is not set

      for (size_t namespace_i = 0;
           namespace_i < namespaces.size(); ++namespace_i) {
        check_identifier(namespaces[namespace_i], "namespace");
      }

      std::fstream out(out_file_name.c_str(), std::fstream::out);
      if (out_stream) {
        *out_stream << "Parsing a fuctions-only file" << std::endl;

        *out_stream << "Target namespace= ";
        for (size_t namespace_i = 0;
             namespace_i < namespaces.size(); ++namespace_i) {
          *out_stream << "::" << namespaces[namespace_i];
        }
        *out_stream << std::endl;

        *out_stream << "Input file=" << in_file_name << std::endl;
        *out_stream << "Output file=" << out_file_name << std::endl;
      }

      valid_input = stan::lang::compile_functions(err_stream, in, out,
                                                  namespaces, allow_undefined);

      out.close();
      break;
    }
    default: {
      assert(false);
    }
    }

    if (!valid_input) {
      if (err_stream)
        *err_stream << "PARSING FAILED." << std::endl;
      // FIXME: how to remove triple cut-and-paste?
      delete_file(out_stream, out_file_name);
      return PARSE_FAIL_RC;
    }
  } catch (const std::invalid_argument& e) {
    if (err_stream) {
      *err_stream << std::endl
                  << e.what()
                  << std::endl;
      delete_file(out_stream, out_file_name);
    }
    return INVALID_ARGUMENT_RC;
  } catch (const std::exception& e) {
    if (err_stream) {
      *err_stream << std::endl
                  << e.what()
                  << std::endl;
    }
    delete_file(out_stream, out_file_name);
    return EXCEPTION_RC;
  }
  return SUCCESS_RC;
}

#endif
