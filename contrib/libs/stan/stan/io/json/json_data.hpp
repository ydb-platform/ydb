#ifndef STAN_IO_JSON_JSON_DATA_HPP
#define STAN_IO_JSON_JSON_DATA_HPP

#include <boost/throw_exception.hpp>
#include <boost/lexical_cast.hpp>
#include <stan/io/var_context.hpp>
#include <stan/io/json/json_error.hpp>
#include <stan/io/json/json_parser.hpp>
#include <stan/io/json/json_data_handler.hpp>
#include <cctype>
#include <iostream>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace stan {

  namespace json {

    /**
     * A <code>json_data</code> is a <code>var_context</code> object
     * that represents a set of named values which are typed to either
     * <code>double</code> or <code>int</code> and can be either scalar
     * value or a non-empty array of values of any dimensionality.
     * Arrays must be retangular and the values of an array are all of
     * the same type, either double or int.
     *
     * <p>The dimensions and values of variables are accessed by variable name.
     * The values of a variable are stored as a vector of values and
     * a vector of array dimensions, where a scalar value consists of
     * a single value and an emtpy vector for the dimensionality.
     * Multidimensional arrays are stored in column-major order,
     * meaning the first index changes the most quickly.
     * If all the values of an array are int values, the array will be
     * stored as a vector of ints, else the array will be stored
     * as a vector of type double.
     *
     * <p><code>json_data</code> objects are created by using the
     * <code>json_parser</code> and a <code>json_data_handler</code>
     * to read a single JSON text from an input stream.
     */
    class json_data : public stan::io::var_context {
    private:
      vars_map_r vars_r_;
      vars_map_i vars_i_;

      std::vector<double> const empty_vec_r_;
      std::vector<int> const empty_vec_i_;
      std::vector<size_t> const empty_vec_ui_;

      /**
       * Return <code>true</code> if this json_data contains the specified
       * variable name defined as a real-valued variable. This method
       * returns <code>false</code> if the values are all integers.
       *
       * @param name Variable name to test.
       * @return <code>true</code> if the variable exists in the
       * real values of the json_data.
       */
      bool contains_r_only(const std::string& name) const {
        return vars_r_.find(name) != vars_r_.end();
      }

    public:
      /**
       * Construct a json_data object from the specified input stream.
       *
       * <b>Warning:</b> This method does not close the input stream.
       *
       * @param in Input stream from which to read.
       * @throws json_exception if data is not well-formed stan data declaration
       */
      explicit json_data(std::istream& in) : vars_r_(), vars_i_() {
        json_data_handler handler(vars_r_, vars_i_);
        stan::json::parse(in, handler);
      }

      /**
       * Return <code>true</code> if this json_data contains the specified
       * variable name. This method returns <code>true</code>
       * even if the values are all integers.
       *
       * @param name Variable name to test.
       * @return <code>true</code> if the variable exists.
       */
      bool contains_r(const std::string& name) const {
        return contains_r_only(name) || contains_i(name);
      }

      /**
       * Return <code>true</code> if this json_data contains an integer
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
       * Return the dimensions for the variable with the specified
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
       * the json_data.
       *
       * @param names Vector to store the list of names in.
       */
      virtual void names_r(std::vector<std::string>& names) const {
        names.resize(0);
        for (vars_map_r::const_iterator it = vars_r_.begin();
             it != vars_r_.end(); ++it)
          names.push_back((*it).first);
      }

      /**
       * Return a list of the names of the integer variables in
       * the json_data.
       *
       * @param names Vector to store the list of names in.
       */
      virtual void names_i(std::vector<std::string>& names) const {
        names.resize(0);
        for (vars_map_i::const_iterator it = vars_i_.begin();
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
