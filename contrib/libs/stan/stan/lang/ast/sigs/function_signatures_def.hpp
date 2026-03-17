#ifndef STAN_LANG_AST_SIGS_FUNCTION_SIGNATURES_DEF_HPP
#define STAN_LANG_AST_SIGS_FUNCTION_SIGNATURES_DEF_HPP

#include <stan/lang/ast.hpp>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace stan {
  namespace lang {

    void function_signatures::reset_sigs() {
      if (sigs_ == 0) return;
      delete sigs_;
      sigs_ = 0;
    }

    function_signatures& function_signatures::instance() {
      // TODO(carpenter): for threaded autodiff, requires double-check lock
      if (!sigs_)
        sigs_ = new function_signatures;
      return *sigs_;
    }

    void function_signatures::set_user_defined(
      const std::pair<std::string, function_signature_t>& name_sig) {
      user_defined_set_.insert(name_sig);
    }

    bool function_signatures::is_user_defined(
      const std::pair<std::string, function_signature_t>& name_sig) {
      return user_defined_set_.find(name_sig) != user_defined_set_.end();
    }

    bool function_signatures::is_defined(const std::string& name,
                                         const function_signature_t& sig) {
      if (sigs_map_.find(name) == sigs_map_.end())
        return false;
      const std::vector<function_signature_t> sigs = sigs_map_[name];
      // check return type
      for (size_t i = 0; i < sigs.size(); ++i)
        if (sig.first == sigs[i].first && sig.second == sigs[i].second)
          return true;
      return false;
    }

    bool function_signatures::discrete_first_arg(const std::string& fun)
      const {
      using std::map;
      using std::string;
      using std::vector;
      map<string, vector<function_signature_t> >::const_iterator it
        = sigs_map_.find(fun);
      if (it == sigs_map_.end())
        return false;
      const vector<function_signature_t> sigs = it->second;
      for (size_t i = 0; i < sigs.size(); ++i) {
        if (sigs[i].second.size() == 0
            || !sigs[i].second[0].innermost_type().is_int_type())
          return false;
      }
      return true;
    }

    function_signature_t
    function_signatures::get_definition(const std::string& name,
                                        const function_signature_t& sig) {
      const std::vector<function_signature_t> sigs = sigs_map_[name];
      for (size_t i = 0; i < sigs.size(); ++i)
        if (sig.first == sigs[i].first && sig.second == sigs[i].second) {
          return sigs[i];
        }
      bare_expr_type ill_formed;
      std::vector<bare_expr_type> arg_types;
      return function_signature_t(ill_formed, arg_types);
    }

    void function_signatures::add(const std::string& name,
                                  const bare_expr_type& result_type,
                                  const std::vector<bare_expr_type>&
                                  arg_types) {
      function_signature_t sig_def(result_type, arg_types);
      sigs_map_[name].push_back(function_signature_t(result_type, arg_types));
    }

    void function_signatures::add(const std::string& name,
                                  const bare_expr_type& result_type) {
      std::vector<bare_expr_type> arg_types;
      add(name, result_type, arg_types);
    }

    void function_signatures::add(const std::string& name,
                                  const bare_expr_type& result_type,
                                  const bare_expr_type& arg_type) {
      std::vector<bare_expr_type> arg_types;
      arg_types.push_back(arg_type);
      add(name, result_type, arg_types);
    }

    void function_signatures::add(const std::string& name,
                                  const bare_expr_type& result_type,
                                  const bare_expr_type& arg_type1,
                                  const bare_expr_type& arg_type2) {
      std::vector<bare_expr_type> arg_types;
      arg_types.push_back(arg_type1);
      arg_types.push_back(arg_type2);
      add(name, result_type, arg_types);
    }

    void function_signatures::add(const std::string& name,
                                  const bare_expr_type& result_type,
                                  const bare_expr_type& arg_type1,
                                  const bare_expr_type& arg_type2,
                                  const bare_expr_type& arg_type3) {
      std::vector<bare_expr_type> arg_types;
      arg_types.push_back(arg_type1);
      arg_types.push_back(arg_type2);
      arg_types.push_back(arg_type3);
      add(name, result_type, arg_types);
    }

    void function_signatures::add(const std::string& name,
                                  const bare_expr_type& result_type,
                                  const bare_expr_type& arg_type1,
                                  const bare_expr_type& arg_type2,
                                  const bare_expr_type& arg_type3,
                                  const bare_expr_type& arg_type4) {
      std::vector<bare_expr_type> arg_types;
      arg_types.push_back(arg_type1);
      arg_types.push_back(arg_type2);
      arg_types.push_back(arg_type3);
      arg_types.push_back(arg_type4);
      add(name, result_type, arg_types);
    }

    void function_signatures::add(const std::string& name,
                                  const bare_expr_type& result_type,
                                  const bare_expr_type& arg_type1,
                                  const bare_expr_type& arg_type2,
                                  const bare_expr_type& arg_type3,
                                  const bare_expr_type& arg_type4,
                                  const bare_expr_type& arg_type5) {
      std::vector<bare_expr_type> arg_types;
      arg_types.push_back(arg_type1);
      arg_types.push_back(arg_type2);
      arg_types.push_back(arg_type3);
      arg_types.push_back(arg_type4);
      arg_types.push_back(arg_type5);
      add(name, result_type, arg_types);
    }

    void function_signatures::add(const std::string& name,
                                  const bare_expr_type& result_type,
                                  const bare_expr_type& arg_type1,
                                  const bare_expr_type& arg_type2,
                                  const bare_expr_type& arg_type3,
                                  const bare_expr_type& arg_type4,
                                  const bare_expr_type& arg_type5,
                                  const bare_expr_type& arg_type6) {
      std::vector<bare_expr_type> arg_types;
      arg_types.push_back(arg_type1);
      arg_types.push_back(arg_type2);
      arg_types.push_back(arg_type3);
      arg_types.push_back(arg_type4);
      arg_types.push_back(arg_type5);
      arg_types.push_back(arg_type6);
      add(name, result_type, arg_types);
    }

    void function_signatures::add(const std::string& name,
                                  const bare_expr_type& result_type,
                                  const bare_expr_type& arg_type1,
                                  const bare_expr_type& arg_type2,
                                  const bare_expr_type& arg_type3,
                                  const bare_expr_type& arg_type4,
                                  const bare_expr_type& arg_type5,
                                  const bare_expr_type& arg_type6,
                                  const bare_expr_type& arg_type7) {
      std::vector<bare_expr_type> arg_types;
      arg_types.push_back(arg_type1);
      arg_types.push_back(arg_type2);
      arg_types.push_back(arg_type3);
      arg_types.push_back(arg_type4);
      arg_types.push_back(arg_type5);
      arg_types.push_back(arg_type6);
      arg_types.push_back(arg_type7);
      add(name, result_type, arg_types);
    }

    void function_signatures::add_nullary(const::std::string& name) {
      add(name, bare_expr_type(double_type()));
    }

    void function_signatures::add_unary(const::std::string& name) {
      double_type tDouble;
      bare_expr_type a1(tDouble);
      add(name, a1, a1);
    }

    void function_signatures::add_unary_vectorized(const::std::string&
                                                   name) {
      // note:  vectorized functions always return elements of type real;
      //        integer elements are promoted to real elements
      add(name, bare_expr_type(double_type()), bare_expr_type(int_type()));
      add(name, bare_expr_type(double_type()), bare_expr_type(double_type()));
      add(name, bare_expr_type(vector_type()), bare_expr_type(vector_type()));
      add(name, bare_expr_type(row_vector_type()),
          bare_expr_type(row_vector_type()));
      add(name, bare_expr_type(matrix_type()), bare_expr_type(matrix_type()));

      int_type tInt;
      bare_array_type arInt(tInt);
      bare_expr_type arIntType(arInt);
      double_type tDouble;
      bare_array_type arDouble(tDouble);
      bare_expr_type arDoubleType(arDouble);
      matrix_type tMatrix;
      bare_array_type arMatrix(tMatrix);
      bare_expr_type arMatrixType(arMatrix);
      row_vector_type tRowVector;
      bare_array_type arRowVector(tRowVector);
      bare_expr_type arRowVectorType(arRowVector);
      vector_type tVector;
      bare_array_type arVector(tVector);
      bare_expr_type arVectorType(arVector);
      for (size_t i = 0; i < 8; ++i) {
        add(name, arDoubleType, arIntType);
        add(name, arDoubleType, arDoubleType);
        add(name, arMatrixType, arMatrixType);
        add(name, arRowVectorType, arRowVectorType);
        add(name, arVectorType, arVectorType);
        arIntType = bare_expr_type(bare_array_type(arIntType));
        arDoubleType = bare_expr_type(bare_array_type(arDoubleType));
        arMatrixType = bare_expr_type(bare_array_type(arMatrixType));
        arRowVectorType = bare_expr_type(bare_array_type(arRowVectorType));
        arVectorType = bare_expr_type(bare_array_type(arVectorType));
      }
    }

    void function_signatures::add_binary(const::std::string& name) {
      add(name, bare_expr_type(double_type()), bare_expr_type(double_type()),
          bare_expr_type(double_type()));
    }

    void function_signatures::add_ternary(const::std::string& name) {
      add(name, bare_expr_type(double_type()), bare_expr_type(double_type()),
          bare_expr_type(double_type()), bare_expr_type(double_type()));
    }

    void function_signatures::add_quaternary(const::std::string& name) {
      add(name, bare_expr_type(double_type()), bare_expr_type(double_type()),
          bare_expr_type(double_type()), bare_expr_type(double_type()),
          bare_expr_type(double_type()));
    }

    template<typename T>
    bare_expr_type
    function_signatures::rng_return_type(const bare_expr_type& t) {
      T return_type;
      return t.is_primitive() ?
        bare_expr_type(return_type) :
        bare_expr_type(bare_array_type(return_type, 1));
    }

    template<typename T>
    bare_expr_type function_signatures::rng_return_type(
                            const bare_expr_type& t,
                            const bare_expr_type& u) {
      T return_type;
      return t.is_primitive() && u.is_primitive()
        ? bare_expr_type(return_type)
        : bare_expr_type(bare_array_type(return_type, 1));
    }

    template<typename T>
    bare_expr_type function_signatures::rng_return_type(
                            const bare_expr_type& t,
                            const bare_expr_type& u,
                            const bare_expr_type& v) {
      return rng_return_type<T>(rng_return_type<T>(t, u), v);
    }

    int function_signatures::num_promotions(
                             const std::vector<bare_expr_type>& call_args,
                             const std::vector<bare_expr_type>& sig_args) {
      if (call_args.size() != sig_args.size()) {
        return -1;  // failure
      }
      int num_promotions = 0;
      for (size_t i = 0; i < call_args.size(); ++i) {
        if (call_args[i] == sig_args[i]) {
          continue;
        } else if (call_args[i].is_primitive()
                   && sig_args[i].is_double_type()) {
          ++num_promotions;
        } else {
          return -1;    // failed match
        }
      }
      return num_promotions;
    }

    int function_signatures::get_signature_matches(const std::string& name,
                             const std::vector<bare_expr_type>& args,
                             function_signature_t& signature) {
      if (!has_key(name)) return 0;
      std::vector<function_signature_t> signatures = sigs_map_[name];
      size_t min_promotions = std::numeric_limits<size_t>::max();
      size_t num_matches = 0;
      for (size_t i = 0; i < signatures.size(); ++i) {
        signature = signatures[i];
        int promotions = num_promotions(args, signature.second);
        if (promotions < 0) continue;  // no match
        size_t promotions_ui = static_cast<size_t>(promotions);
        if (promotions_ui < min_promotions) {
          min_promotions = promotions_ui;
          num_matches = 1;
        } else if (promotions_ui == min_promotions) {
          ++num_matches;
        }
      }
      return num_matches;
    }

    bool is_binary_operator(const std::string& name) {
      return name == "add"
        || name == "subtract"
        || name == "multiply"
        || name == "divide"
        || name == "modulus"
        || name == "mdivide_left"
        || name == "mdivide_right"
        || name == "elt_multiply"
        || name == "elt_divide";
    }

    bool is_unary_operator(const std::string& name) {
      return name == "minus"
        || name == "logical_negation";
    }

    bool is_unary_postfix_operator(const std::string& name) {
      return name == "transpose";
    }

    bool is_operator(const std::string& name) {
      return is_binary_operator(name)
        || is_unary_operator(name)
        || is_unary_postfix_operator(name);
    }


    std::string fun_name_to_operator(const std::string& name) {
      // binary infix (pow handled by parser)
      if (name == "add") return "+";
      if (name == "subtract") return "-";
      if (name == "multiply") return "*";
      if (name == "divide") return "/";
      if (name == "modulus") return "%";
      if (name == "mdivide_left") return "\\";
      if (name == "mdivide_right") return "/";
      if (name == "elt_multiply") return ".*";
      if (name == "elt_divide") return "./";

      // unary prefix (+ handled by parser)
      if (name == "minus") return "-";
      if (name == "logical_negation") return "!";

      // unary suffix
      if (name == "transpose") return "'";

      // none of the above
      return "ERROR";
    }

    void print_signature(const std::string& name,
                         const std::vector<bare_expr_type>& arg_types,
                         bool sampling_error_style,
                         std::ostream& msgs) {
      static size_t OP_SIZE = std::string("operator").size();
      msgs << "  ";
      if (name.size() > OP_SIZE && name.substr(0, OP_SIZE) == "operator") {
        std::string operator_name = name.substr(OP_SIZE);
        if (arg_types.size() == 2) {
          msgs << arg_types[0] << " " << operator_name << " "
               << arg_types[1] << std::endl;
          return;
        } else if (arg_types.size() == 1) {
          if (operator_name == "'")  // exception for postfix
            msgs << arg_types[0] << operator_name << std::endl;
          else
            msgs << operator_name << arg_types[0] << std::endl;
          return;
        } else {
          // should not be reachable due to operator grammar
          // continue on purpose to get more info to user if this happens
          msgs << "Operators must have 1 or 2 arguments." << std::endl;
        }
      }
      if (sampling_error_style && arg_types.size() > 0)
        msgs << arg_types[0] << " ~ ";
      msgs << name << "(";
      size_t start = sampling_error_style ? 1 : 0;
      for (size_t j = start; j < arg_types.size(); ++j) {
        if (j > start) msgs << ", ";
        msgs << arg_types[j];
      }
      msgs << ")" << std::endl;
    }

    bare_expr_type function_signatures::get_result_type(const std::string& name,
                                        const std::vector<bare_expr_type>& args,
                                        std::ostream& error_msgs,
                                        bool sampling_error_style) {
      std::vector<function_signature_t> signatures = sigs_map_[name];
      size_t match_index = 0;
      size_t min_promotions = std::numeric_limits<size_t>::max();
      size_t num_matches = 0;

      std::string display_name;
      if (is_operator(name)) {
        display_name = "operator" + fun_name_to_operator(name);
      } else if (sampling_error_style && ends_with("_log", name)) {
        display_name = name.substr(0, name.size() - 4);
      } else if (sampling_error_style
                 && (ends_with("_lpdf", name) || ends_with("_lcdf", name))) {
        display_name = name.substr(0, name.size() - 5);
      } else {
        display_name = name;
      }

      for (size_t i = 0; i < signatures.size(); ++i) {
        int promotions = num_promotions(args, signatures[i].second);
        if (promotions < 0) continue;  // no match
        size_t promotions_ui = static_cast<size_t>(promotions);
        if (promotions_ui < min_promotions) {
          min_promotions = promotions_ui;
          match_index = i;
          num_matches = 1;
        } else if (promotions_ui == min_promotions) {
          ++num_matches;
        }
      }

      if (num_matches == 1)
        return signatures[match_index].first;

      // all returns after here are for ill-typed input
      if (num_matches == 0) {
        error_msgs << "No matches for: "
                   << std::endl << std::endl;
      } else {
        error_msgs << "Ambiguous: "
                   << num_matches << " matches with "
                   << min_promotions << " integer promotions for: "
                   << std::endl;
      }
      print_signature(display_name, args, sampling_error_style, error_msgs);

      if (signatures.size() == 0) {
        error_msgs << std::endl
                   << (sampling_error_style ? "Distribution " : "Function ")
                   << display_name << " not found.";
        if (sampling_error_style)
          error_msgs << " Require function with _lpdf or _lpmf or _log suffix";
        error_msgs << std::endl;
      } else {
        error_msgs << std::endl
                   << "Available argument signatures for "
                   << display_name << ":" << std::endl << std::endl;

        for (size_t i = 0; i < signatures.size(); ++i) {
          print_signature(display_name, signatures[i].second,
                          sampling_error_style, error_msgs);
        }
        error_msgs << std::endl;
      }
      return bare_expr_type();  // ill-formed dummy
    }

    function_signatures::function_signatures() {
#include <stan/lang/function_signatures.h>  // NOLINT
    }

    bool function_signatures::has_user_defined_key(const std::string& key)
      const {
      using std::pair;
      using std::set;
      using std::string;
      for (set<pair<string, function_signature_t> >::const_iterator
             it = user_defined_set_.begin();
           it != user_defined_set_.end();
           ++it) {
        if (it->first == key)
          return true;
      }
      return false;
    }

    std::set<std::string> function_signatures::key_set() const {
      using std::map;
      using std::set;
      using std::string;
      using std::vector;
      set<string> result;
      for (map<string, vector<function_signature_t> >::const_iterator
             it = sigs_map_.begin();
           it != sigs_map_.end();
           ++it)
        result.insert(it->first);
      return result;
    }

    bool function_signatures::has_key(const std::string& key) const {
      return sigs_map_.find(key) != sigs_map_.end();
    }

    /**
     * Global variable holding singleton; initialized to NULL.
     * Retrieve through the static class function
     * <code>function_signatures::instance()</code>.
     */
    function_signatures* function_signatures::sigs_ = 0;
  }
}
#endif
