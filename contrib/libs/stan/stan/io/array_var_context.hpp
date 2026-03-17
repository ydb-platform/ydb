#ifndef STAN_IO_ARRAY_VAR_CONTEXT_HPP
#define STAN_IO_ARRAY_VAR_CONTEXT_HPP

#include <stan/io/var_context.hpp>
#include <boost/throw_exception.hpp>
#include <Eigen/Dense>
#include <map>
#include <sstream>
#include <string>
#include <vector>
#include <utility>

namespace stan {

  namespace io {

    template<typename T>
    T product(std::vector<T> dims) {
      T y = 1;
      for (size_t i = 0; i < dims.size(); ++i)
        y *= dims[i];
      return y;
    }

    /**
     * An array_var_context object represents a named arrays
     * with dimensions constructed from an array, a vector
     * of names, and a vector of all dimensions for each element.
     */
    class array_var_context : public var_context {
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

      bool contains_r_only(const std::string& name) const {
        return vars_r_.find(name) != vars_r_.end();
      }

      /**
       * Check (1) if the vector size of dimensions is no smaller
       * than the name vector size; (2) if the size of the input
       * array is large enough for what is needed.
       */
      template <typename T>
      void validate(const std::vector<std::string>& names,
                    const T& array,
                    const std::vector<std::vector<size_t> >& dims) {
        size_t total = 0;
        size_t num_par = names.size();
        if (num_par > dims.size()) {
          std::stringstream msg;
          msg << "size of vector of dimensions (found " << dims.size() << ") "
              << "should be no smaller than number of parameters (found "
              << num_par << ").";
          BOOST_THROW_EXCEPTION(std::invalid_argument(msg.str()));
        }
        for (size_t i = 0; i < num_par; i++)
          total += stan::io::product(dims[i]);
        size_t array_len = array.size();
        if (total > array_len) {
          std::stringstream msg;
          msg << "array is not long enough for all elements: " << array_len
              << " is found, but "
              << total << " is needed.";
          BOOST_THROW_EXCEPTION(std::invalid_argument(msg.str()));
        }
      }

      void add_r(const std::vector<std::string>& names,
                 const std::vector<double>& values,
                 const std::vector<std::vector<size_t> >& dims) {
        validate(names, values, dims);
        size_t start = 0;
        size_t end = 0;
        for (size_t i = 0; i < names.size(); i++) {
          end += product(dims[i]);
          std::vector<double> v(values.begin() + start, values.begin() + end);
          vars_r_[names[i]]
            = std::pair<std::vector<double>,
                        std::vector<size_t> >(v, dims[i]);
          start = end;
        }
      }

      void add_r(const std::vector<std::string>& names,
                 const Eigen::VectorXd& values,
                 const std::vector<std::vector<size_t> >& dims) {
        validate(names, values, dims);
        size_t start = 0;
        size_t end = 0;
        for (size_t i = 0; i < names.size(); i++) {
          end += product(dims[i]);
          size_t v_len = end - start;
          std::vector<double> v(v_len);
          for (size_t i = 0; i < v_len; ++i)
            v[i] = values(start + i);
          vars_r_[names[i]]
            = std::pair<std::vector<double>,
                        std::vector<size_t> >(v, dims[i]);
          start = end;
        }
      }

      void add_i(const std::vector<std::string>& names,
                 const std::vector<int>& values,
                 const std::vector<std::vector<size_t> >& dims) {
        validate(names, values, dims);
        size_t start = 0;
        size_t end = 0;
        for (size_t i = 0; i < names.size(); i++) {
          end += product(dims[i]);
          std::vector<int> v(values.begin() + start, values.begin() + end);
          vars_i_[names[i]]
            = std::pair<std::vector<int>,
                        std::vector<size_t> >(v, dims[i]);
          start = end;
        }
      }

    public:
      /**
       * Construct an array_var_context from only real value arrays.
       *
       * @param names_r  names for each element
       * @param values_r a vector of double values for all elements
       * @param dim_r   a vector of dimensions
       */
      array_var_context(const std::vector<std::string>& names_r,
                        const std::vector<double>& values_r,
                        const std::vector<std::vector<size_t> >& dim_r) {
        add_r(names_r, values_r, dim_r);
      }

      /**
       * Construct an array_var_context from an Eigen::RowVectorXd.
       *
       * @param names_r  names for each element
       * @param values_r an Eigen RowVector double values for all elements
       * @param dim_r   a vector of dimensions
       */
      array_var_context(const std::vector<std::string>& names_r,
                        const Eigen::RowVectorXd& values_r,
                        const std::vector<std::vector<size_t> >& dim_r) {
        add_r(names_r, values_r, dim_r);
      }

      /**
       * Construct an array_var_context from only integer value arrays.
       *
       * @param names_i  names for each element
       * @param values_i a vector of integer values for all elements
       * @param dim_i   a vector of dimensions
       */
      array_var_context(const std::vector<std::string>& names_i,
                        const std::vector<int>& values_i,
                        const std::vector<std::vector<size_t> >& dim_i) {
        add_i(names_i, values_i, dim_i);
      }

      /**
       * Construct an array_var_context from arrays of both double
       * and integer separately
       *
       */
      array_var_context(const std::vector<std::string>& names_r,
                        const std::vector<double>& values_r,
                        const std::vector<std::vector<size_t> >& dim_r,
                        const std::vector<std::string>& names_i,
                        const std::vector<int>& values_i,
                        const std::vector<std::vector<size_t> >& dim_i) {
        add_i(names_i, values_i, dim_i);
        add_r(names_r, values_r, dim_r);
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
