#ifndef STAN_LANG_AST_NODE_MAP_RECT_HPP
#define STAN_LANG_AST_NODE_MAP_RECT_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <string>
#include <utility>
#include <vector>

namespace stan {
namespace lang {

/**
 * Structure to hold the arguments to the map_rect function.
 */
struct map_rect {
  /**
   * Static identifier that gets incremented for each instance of this
   * class.
   */
  static int CALL_ID_;

  /**
   * Return the singleton sequence of map_rect calls.
   *
   * @return the sequence of map_rect calls.
   */
  static std::vector<std::pair<int, std::string> >& registered_calls() {
    static std::vector<std::pair<int, std::string> > REGISTERED_CALLS_;
    return REGISTERED_CALLS_;
  }

  /**
   * Unique index for this specific instance of map_rect.
   */
  int call_id_;

  /**
   * Name of function being mapped.
   */
  std::string fun_name_;

  /**
   * Vector of shared parameters.
   */
  expression shared_params_;

  /**
   * Array of vectors of job-specific parameters.
   */
  expression job_params_;

  /**
   * Two-dimensional real array of job-specific real data.
   */
  expression job_data_r_;

  /**
   * Two-dimensional real array of job-specific integer data.
   */
  expression job_data_i_;

  /**
   * Construct a default instance of this class with an empty function
   * name and ill-formed expressions for all of the parameters.
   */
  map_rect();

  /**
   * Copy constructor using the member variables of the specified
   * object to construct a copy.
   *
   * @param[in] mr rectangular map to copy
   */
  map_rect(const map_rect& mr);

  /**
   * Assign the member variables of the specified rectangular map
   * object to this object's member variables.  This operator and the
   * copy constructor are defined to avoid incrementing the call
   * identifier accidentally.
   *
   * @param[in] mr rectangular map to assign
   * @return a reference to this object
   */
  map_rect& operator=(const map_rect& mr);

  /**
   * Construct an instance with the specified function name, shared
   * parameters, job-specific parameters, and job-specific data, with
   * an automatically generated call ID.  The call IDs are assigned
   * and then incremented as the map_rect calls are encountered in the
   * program, starting from 1.
   *
   * @param[in] fun_name name of function being mapped
   * @param[in] shared_params expression for vector of parameters used in
   * every job
   * @param[in] job_params expression for array of vectors of job-specific
   * parameters
   * @param[in] job_data_r data-only expression for array of arrays of
   * job-specific real data
   * @param[in] job_data_i data-only expression for array of arrays of
   * job-specific integer data
   */
  map_rect(const std::string& fun_name, const expression& shared_params,
           const expression& job_params, const expression& job_data_r,
           const expression& job_data_i);

  /**
   * Add this rectangular map to the sequence of registered
   * instances.  These will have macros for MPI generated for them by
   * the generator.
   */
  void register_id();
};

}
}
#endif
