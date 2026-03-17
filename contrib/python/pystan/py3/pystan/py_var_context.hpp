#ifndef PYSTAN__IO__PY_VAR_CONTEXT_HPP
#define PYSTAN__IO__PY_VAR_CONTEXT_HPP

#include <fstream>
#include <sstream>
#include <stdexcept>
#include <map>
#include <vector>
#include <string>

#include <stan/io/var_context.hpp>
#include <stan/io/dump.hpp>
#include <stan/io/ends_with.hpp>
#include <stan/io/json/json_data.hpp>

/* see stan/io/dump.hpp */

namespace pystan {

  namespace io {

    /**
     * Represents named arrays with dimensions.
     *
     * A py_var_context implements var_context directly
     * by passing data using the required C++ containers.
     *
     * The dimension for a single value is an empty vector. A list of
     * values is indicated by an entry with the number of values.
     * The dimensions of an array are indicated as one would expect.
     *
     */
    class py_var_context : public stan::io::var_context {

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
       * Construct a py_var_context object from the specified containers.
       *
       * @param vars_r Mapping between variable names and pair(data, dims)
       * @param vars_i Mapping between variable names and pair(data, dims)
       */
      py_var_context(
        std::map<std::string,
          std::pair<std::vector<double>, std::vector<size_t> > >& vars_r,
        std::map<std::string,
          std::pair<std::vector<int>, std::vector<size_t> > >& vars_i)
      {
          vars_r_ = vars_r;
          vars_i_ = vars_i;
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

    std::shared_ptr<stan::io::var_context> get_var_context(const std::string file) {
    std::fstream stream(file.c_str(), std::fstream::in);
    if (file != "" && (stream.rdstate() & std::ifstream::failbit)) {
      std::stringstream msg;
      msg << "Can't open specified file, \"" << file << "\"" << std::endl;
      throw std::invalid_argument(msg.str());
    }
    if (stan::io::ends_with(".json", file)) {
      stan::json::json_data var_context(stream);
      stream.close();
      std::shared_ptr<stan::io::var_context> result = std::make_shared<stan::json::json_data>(var_context);
      return result;
    }
    stan::io::dump var_context(stream);
    stream.close();
    std::shared_ptr<stan::io::var_context> result = std::make_shared<stan::io::dump>(var_context);
    return result;
    }

  }

}

#endif
