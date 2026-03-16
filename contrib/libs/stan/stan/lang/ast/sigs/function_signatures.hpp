#ifndef STAN_LANG_AST_SIGS_FUNCTION_SIGNATURES_HPP
#define STAN_LANG_AST_SIGS_FUNCTION_SIGNATURES_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <stan/lang/ast/sigs/function_signature_t.hpp>
#include <map>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>


namespace stan {
  namespace lang {

    /**
     * This class is a singleton used to store the available functions
     * in the Stan object language and their signatures.  Use
     * <code>instance()</code> to retrieve the single instance.
     */
    class function_signatures {
    public:
      /**
       * Return the instance of this singleton.
       *
       * @return singleton function signatures object
       */
      static function_signatures& instance();

      /**
       * Reset the signature singleton to contain no instances.
       */
      static void reset_sigs();

      /**
       * Set the specified name and signature to be a user-defined
       * function.
       *
       * @param name_sig name and signature of user-defined function
       */
      void set_user_defined(const std::pair<std::string, function_signature_t>&
                            name_sig);

      /**
       * Return true if the specified name and signature have been
       * added as user-defined functions.
       *
       * @param name_sig name and signature of function
       */
      bool is_user_defined(const std::pair<std::string, function_signature_t>&
                           name_sig);

      /**
       * Return the function definition given the function name and argument
       * expression types. Used to check argument qualifiers, which are
       * only available from function definition.
       *
       * @param name function name
       * @param sig functionand sig
       */
      function_signature_t get_definition(const std::string& name,
                                          const function_signature_t& sig);

      /**
       * Add a built-in function with the specified name, result, type
       * and arguments.
       *
       * @param name function name
       * @param result_type function return type
       * @param arg_types sequence of argument types
       */
      void add(const std::string& name,
               const bare_expr_type& result_type,
               const std::vector<bare_expr_type>& arg_types);

      /**
       * Add a built-in function with the specifed name and result
       * type, with no arguments.
       *
       * @param name function name
       * @param result_type function return type
       */
      void add(const std::string& name,
               const bare_expr_type& result_type);

      /**
       * Add a built-in function with the specifed name, result type,
       * and argument types.
       *
       * @param name function name
       * @param result_type function return type
       * @param arg_type1 type of first argument
       */
      void add(const std::string& name,
               const bare_expr_type& result_type,
               const bare_expr_type& arg_type1);
      /**
       * Add a built-in function with the specifed name, result type,
       * and argument types.
       *
       * @param name function name
       * @param result_type function return type
       * @param arg_type1 type of first argument
       * @param arg_type2 type of second argument
       */
      void add(const std::string& name,
               const bare_expr_type& result_type,
               const bare_expr_type& arg_type1,
               const bare_expr_type& arg_type2);

      /**
       * Add a built-in function with the specifed name, result type,
       * and argument types.
       *
       * @param name function name
       * @param result_type function return type
       * @param arg_type1 type of first argument
       * @param arg_type2 type of second argument
       * @param arg_type3 type of third argument
       */
      void add(const std::string& name,
               const bare_expr_type& result_type,
               const bare_expr_type& arg_type1,
               const bare_expr_type& arg_type2,
               const bare_expr_type& arg_type3);

      /**
       * Add a built-in function with the specifed name, result type,
       * and argument types.
       *
       * @param name function name
       * @param result_type function return type
       * @param arg_type1 type of first argument
       * @param arg_type2 type of second argument
       * @param arg_type3 type of third argument
       * @param arg_type4 type of fourth argument
       */
      void add(const std::string& name,
               const bare_expr_type& result_type,
               const bare_expr_type& arg_type1,
               const bare_expr_type& arg_type2,
               const bare_expr_type& arg_type3,
               const bare_expr_type& arg_type4);

      /**
       * Add a built-in function with the specifed name, result type,
       * and argument types.
       *
       * @param name function name
       * @param result_type function return type
       * @param arg_type1 type of first argument
       * @param arg_type2 type of second argument
       * @param arg_type3 type of third argument
       * @param arg_type4 type of fourth argument
       * @param arg_type5 type of fifth argument
       */
      void add(const std::string& name,
               const bare_expr_type& result_type,
               const bare_expr_type& arg_type1,
               const bare_expr_type& arg_type2,
               const bare_expr_type& arg_type3,
               const bare_expr_type& arg_type4,
               const bare_expr_type& arg_type5);

      /**
       * Add a built-in function with the specifed name, result type,
       * and argument types.
       *
       * @param name function name
       * @param result_type function return type
       * @param arg_type1 type of first argument
       * @param arg_type2 type of second argument
       * @param arg_type3 type of third argument
       * @param arg_type4 type of fourth argument
       * @param arg_type5 type of fifth argument
       * @param arg_type6 type of sixth argument
       */
      void add(const std::string& name,
               const bare_expr_type& result_type,
               const bare_expr_type& arg_type1,
               const bare_expr_type& arg_type2,
               const bare_expr_type& arg_type3,
               const bare_expr_type& arg_type4,
               const bare_expr_type& arg_type5,
               const bare_expr_type& arg_type6);

      /**
       * Add a built-in function with the specifed name, result type,
       * and argument types.
       *
       * @param name function name
       * @param result_type function return type
       * @param arg_type1 type of first argument
       * @param arg_type2 type of second argument
       * @param arg_type3 type of third argument
       * @param arg_type4 type of fourth argument
       * @param arg_type5 type of fifth argument
       * @param arg_type6 type of sixth argument
       * @param arg_type7 type of seventh argument
       */
      void add(const std::string& name,
               const bare_expr_type& result_type,
               const bare_expr_type& arg_type1,
               const bare_expr_type& arg_type2,
               const bare_expr_type& arg_type3,
               const bare_expr_type& arg_type4,
               const bare_expr_type& arg_type5,
               const bare_expr_type& arg_type6,
               const bare_expr_type& arg_type7);

      /**
       * Add a built-in function with the specified name, a real
       * return type, and no arguments.
       *
       * @param name function name
       */
      void add_nullary(const::std::string& name);

      /**
       * Add a built-in function with the specified name, a real
       * return type, and a single real argument.
       *
       * @param name function name
       */
      void add_unary(const::std::string& name);

      /**
       * Add built-in functions for all the vectorized form of a unary
       * function with the speicifed name and a single real argument.
       *
       * @param name function name
       */
      void add_unary_vectorized(const::std::string& name);

      /**
       * Add a built-in function with the specified name, a real
       * return type, and two real arguments.
       *
       * @param name function name
       */
      void add_binary(const::std::string& name);

      /**
       * Add a built-in function with the specified name, a real
       * return type, and three real arguments.
       *
       * @param name function name
       */
      void add_ternary(const::std::string& name);

      /**
       * Add a built-in function with the specified name, a real
       * return type, and four real arguments.
       *
       * @param name function name
       */
      void add_quaternary(const::std::string& name);

      /**
       * Determine the return type of distributions' RNG function
       * based on the primitiveness of the arguments. If both
       * arguments are scalar, the return type is int or real
       * depending on the distribtuion. Otherwise, the return type is
       * int[] for discrete distributions and real[] for continuous
       * ones.
       *
       * @param t type of first argument
       * @return expression type resulting from primitiveness of
       * arguments and distribution's support
       */
      template<typename T>
      bare_expr_type rng_return_type(const bare_expr_type& t);

      /**
       * Determine the return type of distributions' RNG function
       * based on the primitiveness of the arguments. If both
       * arguments are scalar, the return type is int or real
       * depending on the distribtuion. Otherwise, the return type is
       * int[] for discrete distributions and real[] for continuous
       * ones.
       *
       * @param t type of first argument
       * @param u type of second argument
       * @return expression type resulting from primitiveness of
       * arguments and distribution's support
       */
      template<typename T>
      bare_expr_type rng_return_type(const bare_expr_type& t,
                                     const bare_expr_type& u);

      /**
       * Determine the return type of distributions' RNG function
       * based on the primitiveness of the arguments. If both
       * arguments are scalar, the return type is int or real
       * depending on the distribtuion. Otherwise, the return type is
       * int[] for discrete distributions and real[] for continuous
       * ones.
       *
       * @param t type of first argument
       * @param u type of second argument
       * @param v type of third argument
       * @return expression type resulting from primitiveness of
       * arguments and distribution's support
       */
      template<typename T>
      bare_expr_type rng_return_type(const bare_expr_type& t,
                                const bare_expr_type& u,
                                const bare_expr_type& v);

      /**
       * Return the number of integer to real promotions required to
       * convert the specified call arguments to the specified
       * signature arguments.
       *
       * @param call_args argument types in function call
       * @param sig_args argument types in function signature
       * @return number of promotions required to cast call arguments
       * to the signature arguments
       */
      int num_promotions(const std::vector<bare_expr_type>& call_args,
                         const std::vector<bare_expr_type>& sig_args);

      /**
       * Return the result expression type resulting from applying a
       * function of the speicified name and argument types, with
       * errors going to the specified error message string and a flag
       * to control error output.
       *
       * @param name function name
       * @param args sequence of argument types it is called with
       * @param error_msgs stream to which error messages are written
       * @param sampling_error_style type of error message, with true
       * value indicating that it was called in a sampling statement
       * @return expression type resulting from applying function with
       * specified names to arguments of specified type
       */
      bare_expr_type get_result_type(const std::string& name,
                                const std::vector<bare_expr_type>& args,
                                std::ostream& error_msgs,
                                bool sampling_error_style = false);

      /**
       * Return the number of declared function signatures match for
       * the specified name, argument types, and signature.
       *
       * @param name function name
       * @param args argument types with which function is called
       * @param signature signature to match
       * @return number of matches
       */
      int get_signature_matches(const std::string& name,
                                const std::vector<bare_expr_type>& args,
                                function_signature_t& signature);

      /**
       * Return true if the specified function name is defined for the
       * specified signature.
       *
       * @param name function name
       * @param sig signature
       * @return true if function name is defined for signature
       */
      bool is_defined(const std::string& name,
                      const function_signature_t& sig);

      /**
       * Return true if the specified name is the name of a
       * user-defined function.
       *
       * @param name function name
       * @return true if function name has been declared as a
       * user-defined function
       */
      bool has_user_defined_key(const std::string& name) const;

      /**
       * Return the set of function names defined.
       *
       * @return set of function names
       */
      std::set<std::string> key_set() const;

      /**
       * Return true if specified key is the name of a declared
       * function.
       *
       * @param key function name
       * @return true if specified function name has been declared
       */
      bool has_key(const std::string& key) const;

      /**
       * Return true if all of the function signatures for functions
       * with the specified name have integer base types.
       *
       * @param name function name
       * @return true if all first arguments to function with
       * specified name are integers
       */
      bool discrete_first_arg(const std::string& name) const;

    private:
      /**
       * Construction is private to enforce singleton pattern.
       */
      function_signatures();

      /**
       * Copy constructor also private to enforce singleton pattern.
       *
       * @param fs function signatures
       */
      function_signatures(const function_signatures& fs);

      /**
       * The mapping from function names to their signatures.
       */
      std::map<std::string, std::vector<function_signature_t> > sigs_map_;

      /**
       * The set of user-defined function name and signature pairs.
       */
      std::set<std::pair<std::string, function_signature_t> > user_defined_set_;

      /**
       * Pointer to store singleton instance; initialized out of class.
       */
      static function_signatures* sigs_;  // init below outside of class
    };

  }
}
#endif
