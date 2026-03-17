#ifndef PYSTAN__STAN_FIT_HPP
#define PYSTAN__STAN_FIT_HPP

#include <cstring>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>

#include <stan/version.hpp>

#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/math/special_functions/fpclassify.hpp>
#include <boost/random/additive_combine.hpp> // L'Ecuyer RNG
#include <boost/random/uniform_real_distribution.hpp>

#include <stan/callbacks/interrupt.hpp>
#include <stan/callbacks/stream_logger.hpp>
#include <stan/callbacks/stream_writer.hpp>
#include <stan/callbacks/writer.hpp>
#include <stan/io/empty_var_context.hpp>
#include <stan/services/diagnose/diagnose.hpp>
#include <stan/services/optimize/bfgs.hpp>
#include <stan/services/optimize/lbfgs.hpp>
#include <stan/services/optimize/newton.hpp>
#include <stan/services/sample/fixed_param.hpp>
#include <stan/services/sample/hmc_nuts_dense_e.hpp>
#include <stan/services/sample/hmc_nuts_dense_e_adapt.hpp>
#include <stan/services/sample/hmc_nuts_diag_e.hpp>
#include <stan/services/sample/hmc_nuts_diag_e_adapt.hpp>
#include <stan/services/sample/hmc_nuts_unit_e.hpp>
#include <stan/services/sample/hmc_nuts_unit_e_adapt.hpp>
#include <stan/services/sample/hmc_static_dense_e.hpp>
#include <stan/services/sample/hmc_static_dense_e_adapt.hpp>
#include <stan/services/sample/hmc_static_diag_e.hpp>
#include <stan/services/sample/hmc_static_diag_e_adapt.hpp>
#include <stan/services/sample/hmc_static_unit_e.hpp>
#include <stan/services/sample/hmc_static_unit_e_adapt.hpp>
#include <stan/services/experimental/advi/fullrank.hpp>
#include <stan/services/experimental/advi/meanfield.hpp>
#include <stan/services/util/create_rng.hpp>

#include "py_var_context.hpp"
#include "Python.h"
#ifndef Py_PYTHON_H
    #error Python headers needed to compile C extensions, please install development version of Python.
#endif
#include "pystan_writer.hpp"


typedef std::map<std::string, std::pair<std::vector<double>, std::vector<size_t> > > vars_r_t;
typedef std::map<std::string, std::pair<std::vector<int>, std::vector<size_t> > > vars_i_t;

namespace pystan {

  // rstan's stan_fit.hpp adds an anonymous namespace here; pystan needs direct access to items below

  enum sampling_algo_t { NUTS = 1, HMC = 2, Metropolis = 3, Fixed_param = 4};
  enum optim_algo_t { Newton = 1, BFGS = 3, LBFGS = 4};
  enum variational_algo_t { MEANFIELD = 1, FULLRANK = 2};
  enum sampling_metric_t { UNIT_E = 1, DIAG_E = 2, DENSE_E = 3};
  enum stan_args_method_t { SAMPLING = 1, OPTIM = 2, TEST_GRADIENT = 3, VARIATIONAL = 4};

  /* functions from RStan's stan_args.hpp */

  void write_comment(std::ostream& o) {
    o << "#" << std::endl;
  }

  template <typename M>
  void write_comment(std::ostream& o, const M& msg) {
    o << "# " << msg << std::endl;
  }

  template <typename K, typename V>
  void write_comment_property(std::ostream& o, const K& key, const V& val) {
    o << "# " << key << "=" << val << std::endl;
  }

  /**
   * Find the index of an element in a vector.
   * @param v the vector in which an element are searched.
   * @param e the element that we are looking for.
   * @return If e is in v, return the index (0 to size - 1);
   *  otherwise, return the size.
   */

  template <class T, class T2>
  size_t find_index(const std::vector<T>& v, const T2& e) {
    return std::distance(v.begin(), std::find(v.begin(), v.end(), T(e)));
  }

  /* Simple class to store arguments provided by Python. Mirrors RStan's stan_args.
   *
   * Apart from the StanArgs class all the functionality found in RStan's
   * stan_args.hpp such as validate_args() and the constructor is handled in
   * Python for easier debugging.
   *
   */
  class StanArgs {
  public:
    unsigned int random_seed;
    unsigned int chain_id;
    std::string init;
    /* init_vars_r and init_vars_i == RStan's init_list */
    vars_r_t init_vars_r;
    vars_i_t init_vars_i;
    double init_radius;
    bool enable_random_init;
    std::string sample_file; // the file for outputting the samples
    bool append_samples;
    bool sample_file_flag; // true: write out to a file; false, do not
    stan_args_method_t method;
    std::string diagnostic_file;
    bool diagnostic_file_flag;
    std::string metric_file; // initial mass matrix
    bool metric_file_flag;
    union {
      struct {
        int iter;   // number of iterations
        int refresh;  //
        sampling_algo_t algorithm;
        int warmup; // number of warmup
        int thin;
        bool save_warmup; // weather to save warmup samples (always true now)
        int iter_save; // number of iterations saved
        int iter_save_wo_warmup; // number of iterations saved wo warmup
        bool adapt_engaged;
        double adapt_gamma;
        double adapt_delta;
        double adapt_kappa;
        unsigned int adapt_init_buffer;
        unsigned int adapt_term_buffer;
        unsigned int adapt_window;
        double adapt_t0;
        sampling_metric_t metric; // UNIT_E, DIAG_E, DENSE_E;
        double stepsize; // defaut to 1;
        double stepsize_jitter;
        int max_treedepth; // for NUTS, default to 10.
        double int_time; // for HMC, default to 2 * pi
      } sampling;
      struct {
        int iter; // default to 2000
        int refresh; // default to 100
        optim_algo_t algorithm; // Newton, (L)BFGS
        bool save_iterations; // default to false
        double init_alpha; // default to 0.0001, for (L)BFGS
        double tol_obj; // default to 1e-12, for (L)BFGS
        double tol_grad; // default to 1e-8, for (L)BFGS
        double tol_param; // default to 1e-8, for (L)BFGS
        double tol_rel_obj; // default to 1e4, for (L)BFGS
        double tol_rel_grad; // default to 1e7, for (L)BFGS
        int history_size; // default to 5, for LBFGS only
      } optim;
      struct {
        int iter; // default to 10000
        variational_algo_t algorithm;  // MEANFIELD or FULLRANK
        int grad_samples; // default to 1
        int elbo_samples; // default to 100
        int eval_elbo;    // default to 100
        int output_samples; // default to 1000
        double eta; // defaults to 1.0
        bool adapt_engaged; // defaults to 1
        int adapt_iter; // defaults to 50
        double tol_rel_obj; // default to 0.01
      } variational;
      struct {
        double epsilon; // default to 1e-6, for test_grad
        double error;  // default to 1e-6, for test_grad
      } test_grad;
    } ctrl;

    inline const std::string& get_sample_file() const {
      return sample_file;
    }
    inline bool get_sample_file_flag() const {
      return sample_file_flag;
    }
    inline bool get_diagnostic_file_flag() const {
      return diagnostic_file_flag;
    }
    inline const std::string& get_diagnostic_file() const {
      return diagnostic_file;
    }
    inline bool get_metric_file_flag() const {
      return metric_file_flag;
    }
    inline const std::string& get_metric_file() const {
      return metric_file;
    }

    void set_random_seed(unsigned int seed) {
      random_seed = seed;
    }

    inline unsigned int get_random_seed() const {
      return random_seed;
    }

    inline int get_ctrl_variational_grad_samples() const {
      return ctrl.variational.grad_samples;
    }
    inline int get_ctrl_variational_elbo_samples() const {
      return ctrl.variational.elbo_samples;
    }
    inline int get_ctrl_variational_output_samples() const {
      return ctrl.variational.output_samples;
    }
    inline int get_ctrl_variational_eval_elbo() const {
      return ctrl.variational.eval_elbo;
    }
    inline double get_ctrl_variational_eta() const {
      return ctrl.variational.eta;
    }
    inline bool get_ctrl_variational_adapt_engaged() const {
      return ctrl.variational.adapt_engaged;
    }
    inline double get_ctrl_variational_tol_rel_obj() const {
      return ctrl.variational.tol_rel_obj;
    }
    inline variational_algo_t get_ctrl_variational_algorithm() const {
      return ctrl.variational.algorithm;
    }
    inline int get_ctrl_variational_adapt_iter() const {
      return ctrl.variational.adapt_iter;
    }
    inline int get_ctrl_sampling_refresh() const {
      return ctrl.sampling.refresh;
    }
    inline sampling_metric_t get_ctrl_sampling_metric() const {
      return ctrl.sampling.metric;
    }
    inline sampling_algo_t get_ctrl_sampling_algorithm() const {
      return ctrl.sampling.algorithm;
    }
    inline int get_ctrl_sampling_warmup() const {
      return ctrl.sampling.warmup;
    }
    void set_ctrl_sampling_warmup(int n) {
      ctrl.sampling.warmup = n;
    }
    inline int get_ctrl_sampling_thin() const {
      return ctrl.sampling.thin;
    }
    inline double get_ctrl_sampling_int_time() const {
      return ctrl.sampling.int_time;
    }
    inline bool get_append_samples() const {
      return append_samples;
    }
    inline stan_args_method_t get_method() const {
      return method;
    }
    inline int get_iter() const {
      switch (method) {
        case SAMPLING: return ctrl.sampling.iter;
        case OPTIM: return ctrl.optim.iter;
        case VARIATIONAL: return ctrl.variational.iter;
        case TEST_GRADIENT: return 0;
      }
      return 0;
    }
    inline bool get_ctrl_sampling_adapt_engaged() const {
      return ctrl.sampling.adapt_engaged;
    }
    inline double get_ctrl_sampling_adapt_gamma() const {
      return ctrl.sampling.adapt_gamma;
    }
    inline double get_ctrl_sampling_adapt_delta() const {
      return ctrl.sampling.adapt_delta;
    }
    inline double get_ctrl_sampling_adapt_kappa() const {
      return ctrl.sampling.adapt_kappa;
    }
    inline double get_ctrl_sampling_adapt_t0() const {
      return ctrl.sampling.adapt_t0;
    }
    inline unsigned int get_ctrl_sampling_adapt_init_buffer() const {
      return ctrl.sampling.adapt_init_buffer;
    }
    inline unsigned int get_ctrl_sampling_adapt_term_buffer() const {
      return ctrl.sampling.adapt_term_buffer;
    }
    inline unsigned int get_ctrl_sampling_adapt_window() const {
      return ctrl.sampling.adapt_window;
    }
    inline double get_ctrl_sampling_stepsize() const {
       return ctrl.sampling.stepsize;
    }
    inline double get_ctrl_sampling_stepsize_jitter() const {
       return ctrl.sampling.stepsize_jitter;
    }
    inline int get_ctrl_sampling_max_treedepth() const {
       return ctrl.sampling.max_treedepth;
    }
    inline int get_ctrl_sampling_iter_save_wo_warmup() const {
       return ctrl.sampling.iter_save_wo_warmup;
    }
    inline int get_ctrl_sampling_iter_save() const {
       return ctrl.sampling.iter_save;
    }
    inline bool get_ctrl_sampling_save_warmup() const {
       return true;
    }
    inline optim_algo_t get_ctrl_optim_algorithm() const {
      return ctrl.optim.algorithm;
    }
    inline int get_ctrl_optim_refresh() const {
      return ctrl.optim.refresh;
    }
    inline bool get_ctrl_optim_save_iterations() const {
      return ctrl.optim.save_iterations;
    }
    inline double get_ctrl_optim_init_alpha() const {
      return ctrl.optim.init_alpha;
    }
    inline double get_ctrl_optim_tol_obj() const {
      return ctrl.optim.tol_obj;
    }
    inline double get_ctrl_optim_tol_grad() const {
      return ctrl.optim.tol_grad;
    }
    inline double get_ctrl_optim_tol_param() const {
      return ctrl.optim.tol_param;
    }
    inline double get_ctrl_optim_tol_rel_obj() const {
      return ctrl.optim.tol_rel_obj;
    }
    inline double get_ctrl_optim_tol_rel_grad() const {
      return ctrl.optim.tol_rel_grad;
    }
    inline int get_ctrl_optim_history_size() const {
      return ctrl.optim.history_size;
    }
    inline double get_ctrl_test_grad_epsilon() const {
      return ctrl.test_grad.epsilon;
    }
    inline double get_ctrl_test_grad_error() const {
      return ctrl.test_grad.error;
    }
    inline unsigned int get_chain_id() const {
      return chain_id;
    }
    inline double get_init_radius() const {
      return init_radius;
    }
    inline bool get_enable_random_init() const {
      return enable_random_init;
    }
    const std::string& get_init() const {
      return init;
    }

    void write_args_as_comment(std::ostream& ostream) const {
      write_comment_property(ostream,"init",init);
      write_comment_property(ostream,"enable_random_init",enable_random_init);
      write_comment_property(ostream,"seed",random_seed);
      write_comment_property(ostream,"chain_id",chain_id);
      write_comment_property(ostream,"iter",get_iter());
      switch (method) {
        case VARIATIONAL:
          write_comment_property(ostream,"grad_samples", ctrl.variational.grad_samples);
          write_comment_property(ostream,"elbo_samples", ctrl.variational.elbo_samples);
          write_comment_property(ostream,"output_samples", ctrl.variational.output_samples);
          write_comment_property(ostream,"eval_elbo", ctrl.variational.eval_elbo);
          write_comment_property(ostream,"eta", ctrl.variational.eta);
          write_comment_property(ostream,"tol_rel_obj", ctrl.variational.tol_rel_obj);
          switch (ctrl.variational.algorithm) {
            case MEANFIELD: write_comment_property(ostream,"algorithm", "meanfield"); break;
            case FULLRANK: write_comment_property(ostream,"algorithm", "fullrank"); break;
          }
          break;
        case SAMPLING:
          write_comment_property(ostream,"warmup",ctrl.sampling.warmup);
          write_comment_property(ostream,"save_warmup",ctrl.sampling.save_warmup);
          write_comment_property(ostream,"thin",ctrl.sampling.thin);
          write_comment_property(ostream,"refresh",ctrl.sampling.refresh);
          write_comment_property(ostream,"stepsize",ctrl.sampling.stepsize);
          write_comment_property(ostream,"stepsize_jitter",ctrl.sampling.stepsize_jitter);
          write_comment_property(ostream,"adapt_engaged",ctrl.sampling.adapt_engaged);
          write_comment_property(ostream,"adapt_gamma",ctrl.sampling.adapt_gamma);
          write_comment_property(ostream,"adapt_delta",ctrl.sampling.adapt_delta);
          write_comment_property(ostream,"adapt_kappa",ctrl.sampling.adapt_kappa);
          write_comment_property(ostream,"adapt_t0",ctrl.sampling.adapt_t0);
          switch (ctrl.sampling.algorithm) {
            case NUTS:
              write_comment_property(ostream,"max_treedepth",ctrl.sampling.max_treedepth);
              switch (ctrl.sampling.metric) {
                case UNIT_E: write_comment_property(ostream,"sampler_t","NUTS(unit_e)"); break;
                case DIAG_E: write_comment_property(ostream,"sampler_t","NUTS(diag_e)"); break;
                case DENSE_E: write_comment_property(ostream,"sampler_t","NUTS(dense_e)"); break;
              }
              break;
            case HMC: write_comment_property(ostream,"sampler_t", "HMC");
                      write_comment_property(ostream,"int_time", ctrl.sampling.int_time);
                      break;
            case Metropolis: write_comment_property(ostream,"sampler_t", "Metropolis"); break;
            case Fixed_param: write_comment_property(ostream, "sampler_t", "Fixed_param"); break;
            default: break;
          }
          break;

        case OPTIM:
          write_comment_property(ostream,"refresh",ctrl.optim.refresh);
          write_comment_property(ostream,"save_iterations",ctrl.optim.save_iterations);
          switch (ctrl.optim.algorithm) {
            case Newton: write_comment_property(ostream,"algorithm", "Newton"); break;
            case BFGS: write_comment_property(ostream,"algorithm", "BFGS");
                       write_comment_property(ostream,"init_alpha", ctrl.optim.init_alpha);
                       write_comment_property(ostream,"tol_obj", ctrl.optim.tol_obj);
                       write_comment_property(ostream,"tol_grad", ctrl.optim.tol_grad);
                       write_comment_property(ostream,"tol_param", ctrl.optim.tol_param);
                       write_comment_property(ostream,"tol_rel_obj", ctrl.optim.tol_rel_obj);
                       write_comment_property(ostream,"tol_rel_grad", ctrl.optim.tol_rel_grad);
                       break;
            case LBFGS: write_comment_property(ostream,"algorithm", "LBFGS");
                       write_comment_property(ostream,"init_alpha", ctrl.optim.init_alpha);
                       write_comment_property(ostream,"tol_obj", ctrl.optim.tol_obj);
                       write_comment_property(ostream,"tol_grad", ctrl.optim.tol_grad);
                       write_comment_property(ostream,"tol_param", ctrl.optim.tol_param);
                       write_comment_property(ostream,"tol_rel_obj", ctrl.optim.tol_rel_obj);
                       write_comment_property(ostream,"tol_rel_grad", ctrl.optim.tol_rel_grad);
                       write_comment_property(ostream,"history_size", ctrl.optim.history_size);
                       break;
          }
        case TEST_GRADIENT: break;
      }
      if (sample_file_flag)
        write_comment_property(ostream,"sample_file",sample_file);
      if (diagnostic_file_flag)
        write_comment_property(ostream,"diagnostic_file",diagnostic_file);
      if (metric_file_flag)
        write_comment_property(ostream,"metric_file",metric_file);
      write_comment_property(ostream,"append_samples",append_samples);
      write_comment(ostream);
    }
  };

  /* simple class to store data for Python */
  class StanHolder {

      public:
          int num_failed;
          bool test_grad;
          std::vector<double> inits;
          std::vector<double> par;
          double value;
          std::vector<std::vector<double> > chains;
          std::vector<std::string> chain_names;
          StanArgs args;
          std::vector<double> mean_pars;
          std::vector<std::string> mean_par_names;
          double mean_lp__;
          std::string adaptation_info;
          std::vector<std::vector<double> > sampler_params;
          std::vector<std::string> sampler_param_names;
  };


  /* the following mirrors RStan's stan_fit.hpp */
  namespace {
    /**
     *@tparam T The type by which we use for dimensions. T could be say size_t
     * or unsigned int. This whole business (not using size_t) is due to that
     * Rcpp::wrap/as does not support size_t on some platforms and R could not
     * deal with 64bits integers.
     *
     */
    template <class T>
    size_t calc_num_params(const std::vector<T>& dim) {
      T num_params = 1;
      for (size_t i = 0;  i < dim.size(); ++i)
        num_params *= dim[i];
      return num_params;
    }

    template <class T>
    void calc_starts(const std::vector<std::vector<T> >& dims,
                     std::vector<T>& starts) {
      starts.resize(0);
      starts.push_back(0);
      for (size_t i = 1; i < dims.size(); ++i)
        starts.push_back(starts[i - 1] + calc_num_params(dims[i - 1]));
    }

    template <class T>
    T calc_total_num_params(const std::vector<std::vector<T> >& dims) {
      T num_params = 0;
      for (size_t i = 0; i < dims.size(); ++i)
        num_params += calc_num_params(dims[i]);
      return num_params;
    }

    /**
     *  Get the parameter indexes for a vector(array) parameter.
     *  For example, we have parameter beta, which has
     *  dimension [2,3]. Then this function gets
     *  the indexes as (if col_major = false)
     *  [0,0], [0,1], [0,2]
     *  [1,0], [1,1], [1,2]
     *  or (if col_major = true)
     *  [0,0], [1,0]
     *  [0,1], [1,1]
     *  [0,2], [121]
     *
     *  @param dim[in] the dimension of parameter
     *  @param idx[out] for keeping all the indexes
     *
     *  <p> when idx is empty (size = 0), idx
     *  would contains an empty vector.
     *
     *
     */

    template <class T>
    void expand_indices(std::vector<T> dim,
                        std::vector<std::vector<T> >& idx,
                        bool col_major = false) {
      size_t len = dim.size();
      idx.resize(0);
      size_t total = calc_num_params(dim);
      if (0 >= total) return;
      std::vector<size_t> loopj;
      for (size_t i = 1; i <= len; ++i)
        loopj.push_back(len - i);

      if (col_major)
        for (size_t i = 0; i < len; ++i)
          loopj[i] = len - 1 - loopj[i];

      idx.push_back(std::vector<T>(len, 0));
      for (size_t i = 1; i < total; i++) {
        std::vector<T>  v(idx.back());
        for (size_t j = 0; j < len; ++j) {
          size_t k = loopj[j];
          if (v[k] < dim[k] - 1) {
            v[k] += 1;
            break;
          }
          v[k] = 0;
        }
        idx.push_back(v);
      }
    }

    /**
     * Get the names for an array of given dimensions
     * in the way of column majored.
     * For example, if we know an array named `a`, with
     * dimensions of [2, 3, 4], the names then are (starting
     * from 0):
     * a[0,0,0]
     * a[1,0,0]
     * a[0,1,0]
     * a[1,1,0]
     * a[0,2,0]
     * a[1,2,0]
     * a[0,0,1]
     * a[1,0,1]
     * a[0,1,1]
     * a[1,1,1]
     * a[0,2,1]
     * a[1,2,1]
     * a[0,0,2]
     * a[1,0,2]
     * a[0,1,2]
     * a[1,1,2]
     * a[0,2,2]
     * a[1,2,2]
     * a[0,0,3]
     * a[1,0,3]
     * a[0,1,3]
     * a[1,1,3]
     * a[0,2,3]
     * a[1,2,3]
     *
     * @param name The name of the array variable
     * @param dim The dimensions of the array
     * @param fnames[out] Where the names would be pushed.
     * @param first_is_one[true] Where to start for the first index: 0 or 1.
     *
     */
    template <class T> void
    get_flatnames(const std::string& name,
                  const std::vector<T>& dim,
                  std::vector<std::string>& fnames,
                  bool col_major = true,
                  bool first_is_one = true) {

      fnames.clear();
      if (0 == dim.size()) {
        fnames.push_back(name);
        return;
      }

      std::vector<std::vector<T> > idx;
      expand_indices(dim, idx, col_major);
      size_t first = first_is_one ? 1 : 0;
      for (typename std::vector<std::vector<T> >::const_iterator it = idx.begin();
           it != idx.end();
           ++it) {
        std::stringstream stri;
        stri << name << "[";

        size_t lenm1 = it -> size() - 1;
        for (size_t i = 0; i < lenm1; i++)
          stri << ((*it)[i] + first) << ",";
        stri << ((*it)[lenm1] + first) << "]";
        fnames.push_back(stri.str());
      }
    }

    // vectorize get_flatnames
    template <class T>
    void get_all_flatnames(const std::vector<std::string>& names,
                           const std::vector<T>& dims,
                           std::vector<std::string>& fnames,
                           bool col_major = true) {
      fnames.clear();
      for (size_t i = 0; i < names.size(); ++i) {
        std::vector<std::string> i_names;
        // NOTE: col_major = true, first_is_one = true for PyStan (true for RStan)
        // first_is_one changed to true from false in PyStan 2.18
	get_flatnames(names[i], dims[i], i_names, col_major, true);
        fnames.insert(fnames.end(), i_names.begin(), i_names.end());
      }
    }

    /* To facilitate transform an array variable ordered by col-major index
     * to row-major index order by providing the transforming indices.
     * For example, we have "x[2,3]", then if ordered by col-major, we have
     *
     * x[1,1], x[2,1], x[1,2], x[2,2], x[1,3], x[3,1]
     *
     * Then the indices for transforming to row-major order are
     * [0, 2, 4, 1, 3, 5] + start.
     *
     * @param dim[in] the dimension of the array variable, empty means a scalar
     * @param midx[out] store the indices for mapping col-major to row-major
     * @param start shifts the indices with a starting point
     *
     */
    template <typename T, typename T2>
    void get_indices_col2row(const std::vector<T>& dim, std::vector<T2>& midx,
                             T start = 0) {
      size_t len = dim.size();
      if (len < 1) {
        midx.push_back(start);
        return;
      }

      std::vector<T> z(len, 1);
      for (size_t i = 1; i < len; i++) {
        z[i] *= z[i - 1] * dim[i - 1];
      }

      T total = calc_num_params(dim);
      midx.resize(total);
      std::fill_n(midx.begin(), total, start);
      std::vector<T> v(len, 0);
      for (T i = 1; i < total; i++) {
        for (size_t j = 0; j < len; ++j) {
          size_t k = len - j - 1;
          if (v[k] < dim[k] - 1) {
            v[k] += 1;
            break;
          }
          v[k] = 0;
        }
        // v is the index of the ith element by row-major, for example v=[0,1,2].
        // obtain the position for v if it is col-major indexed.
        T pos = 0;
        for (size_t j = 0; j < len; j++)
          pos += z[j] * v[j];
        midx[i] += pos;
      }
    }

    template <class T>
    void get_all_indices_col2row(const std::vector<std::vector<T> >& dims,
                                 std::vector<size_t>& midx) {
      midx.clear();
      std::vector<T> starts;
      calc_starts(dims, starts);
      for (size_t i = 0; i < dims.size(); ++i) {
        std::vector<size_t> midxi;
        get_indices_col2row(dims[i], midxi, starts[i]);
        midx.insert(midx.end(), midxi.begin(), midxi.end());
      }
    }

    template <class Model>
    std::vector<std::string> get_param_names(Model& m) {
      std::vector<std::string> names;
      m.get_param_names(names);
      names.push_back("lp__");
      return names;
    }

    template <class T>
    void print_vector(const std::vector<T>& v, std::ostream& o,
                      const std::vector<size_t>& midx,
                      const std::string& sep = ",") {
      if (v.size() > 0)
        o << v[midx.at(0)];
      for (size_t i = 1; i < v.size(); i++)
        o << sep << v[midx.at(i)];
      o << std::endl;
    }

    template <class T>
    void print_vector(const std::vector<T>& v, std::ostream& o,
                      const std::string& sep = ",") {
      if (v.size() > 0)
        o << v[0];
      for (size_t i = 1; i < v.size(); i++)
        o << sep << v[i];
      o << std::endl;
    }

    void write_stan_version_as_comment(std::ostream& output) {
       write_comment_property(output,"stan_version_major",stan::MAJOR_VERSION);
       write_comment_property(output,"stan_version_minor",stan::MINOR_VERSION);
       write_comment_property(output,"stan_version_patch",stan::PATCH_VERSION);
    }

    /**
     * Cast a size_t vector to an unsigned int vector.
     * The reason is that first Rcpp::wrap/as does not
     * support size_t on some platforms; second R
     * could not deal with 64bits integers.
     */

    std::vector<unsigned int>
    sizet_to_uint(std::vector<size_t> v1) {
      std::vector<unsigned int> v2(v1.size());
      for (size_t i = 0; i < v1.size(); ++i)
        v2[i] = static_cast<unsigned int>(v1[i]);
      return v2;
    }

    template <class Model>
    std::vector<std::vector<unsigned int> > get_param_dims(Model& m) {
      std::vector<std::vector<size_t> > dims;
      m.get_dims(dims);

      std::vector<std::vector<unsigned int> > uintdims;
      for (std::vector<std::vector<size_t> >::const_iterator it = dims.begin();
           it != dims.end();
           ++it)
        uintdims.push_back(sizet_to_uint(*it));

      std::vector<unsigned int> scalar_dim; // for lp__
      uintdims.push_back(scalar_dim);
      return uintdims;
    }

    struct PyErr_CheckSignals_Functor : public stan::callbacks::interrupt {
      void operator()() {
        // PyErr_CheckSignals is defined in Python.h
        PyErr_CheckSignals();
      }
    };

    template <class Model>
    std::vector<double> unconstrained_to_constrained(Model& model,
                                                     unsigned int random_seed,
                                                     unsigned int id,
                                                     const std::vector<double>& params) {
      std::vector<int> params_i;
      std::vector<double> constrained_params;
      boost::ecuyer1988 rng = stan::services::util::create_rng(random_seed, id);
      model.write_array(rng, const_cast<std::vector<double>&>(params), params_i,
                        constrained_params);
      return constrained_params;
    }

    /**
     * @tparam Model
     * @tparam RNG
     *
     * @param args: the instance that wraps the arguments passed for sampling.
     * @param model: the model instance.
     * @param holder[out]: the object to hold all the information returned to Python.
     * @param qoi_idx: the indexes for all parameters of interest.
     * @param fnames_oi: the parameter names of interest.
     * @param base_rng: the boost RNG instance.
     */
    template <class Model, class RNG_t>
    int command(StanArgs& args, Model& model, StanHolder& holder,
                const std::vector<size_t>& qoi_idx,
                const std::vector<std::string>& fnames_oi, RNG_t& base_rng) {
      if (args.get_method() == SAMPLING
          && model.num_params_r() == 0
          && args.get_ctrl_sampling_algorithm() != Fixed_param)
        throw std::runtime_error("Must use algorithm=\"Fixed_param\" for "
                                 "model that has no parameters.");
      stan::callbacks::stream_logger logger(std::cout, std::cout, std::cout,
                                            std::cerr, std::cerr);

      PyErr_CheckSignals_Functor interrupt;

      std::fstream sample_stream;
      std::fstream diagnostic_stream;
      std::stringstream comment_stream;
      bool append_samples(args.get_append_samples());
      if (args.get_sample_file_flag()) {
        std::ios_base::openmode samples_append_mode
          = append_samples ? (std::fstream::out | std::fstream::app)
          : std::fstream::out;
        sample_stream.open(args.get_sample_file().c_str(), samples_append_mode);

        if (args.get_method() == TEST_GRADIENT)
          write_comment(sample_stream, "Output generated by Stan (test_grad)");
        else if (args.get_method() == OPTIM)
          write_comment(sample_stream, "Point Estimate Generated by Stan");
        else if (args.get_method() == SAMPLING)
          write_comment(sample_stream, "Sample generated by Stan");
        else if (args.get_method() == VARIATIONAL)
          write_comment(sample_stream, "Sample generated by Stan (Variational Bayes)");
        write_stan_version_as_comment(sample_stream);
        args.write_args_as_comment(sample_stream);
      }
      if (args.get_diagnostic_file_flag()) {
        diagnostic_stream.open(args.get_diagnostic_file().c_str(), std::fstream::out);

        if (args.get_method() == TEST_GRADIENT)
          write_comment(diagnostic_stream, "Output generated by Stan (test_grad)");
        else if (args.get_method() == OPTIM)
          write_comment(diagnostic_stream, "Point Estimate Generated by Stan");
        else if (args.get_method() == SAMPLING)
          write_comment(diagnostic_stream, "Sample generated by Stan");
        else if (args.get_method() == VARIATIONAL)
          write_comment(diagnostic_stream, "Sample generated by Stan (Variational Bayes)");
        write_stan_version_as_comment(diagnostic_stream);
        args.write_args_as_comment(diagnostic_stream);
      }

      stan::callbacks::stream_writer diagnostic_writer(diagnostic_stream, "# ");
      std::auto_ptr<stan::io::var_context> init_context_ptr;
      if (args.get_init() == "user")
        init_context_ptr.reset(new io::py_var_context(args.init_vars_r, args.init_vars_i));
      else
        init_context_ptr.reset(new stan::io::empty_var_context());
      std::vector<std::string> constrained_param_names;
      model.constrained_param_names(constrained_param_names);
      value init_writer;

      int return_code = stan::services::error_codes::CONFIG;

      unsigned int random_seed = args.get_random_seed();
      unsigned int id = args.get_chain_id();
      double init_radius = args.get_init_radius();

      std::string metric_file = args.get_metric_file();
      std::shared_ptr<stan::io::var_context> metric_context_ptr = pystan::io::get_var_context(metric_file);

      if (args.get_method() == TEST_GRADIENT) {
        double epsilon = args.get_ctrl_test_grad_epsilon();
        double error = args.get_ctrl_test_grad_error();

        stan::callbacks::writer sample_writer;
        return_code = stan::services::diagnose::diagnose(model,
                                                         *init_context_ptr,
                                                         random_seed, id,
                                                         init_radius,
                                                         epsilon, error,
                                                         interrupt,
                                                         logger,
                                                         init_writer,
                                                         sample_writer);
        holder.num_failed = return_code;
        holder.test_grad = true;
        holder.inits = unconstrained_to_constrained(model, random_seed, id,
                                                    init_writer.x());
      }
      if (args.get_method() == OPTIM) {
        value sample_writer;
        bool save_iterations = args.get_ctrl_optim_save_iterations();
        int num_iterations = args.get_iter();
        if (args.get_ctrl_optim_algorithm() == Newton) {
          return_code
            = stan::services::optimize::newton(model, *init_context_ptr,
                                               random_seed, id, init_radius,
                                               num_iterations,
                                               save_iterations,
                                               interrupt, logger,
                                               init_writer, sample_writer);
        }
        if (args.get_ctrl_optim_algorithm() == BFGS) {
          double init_alpha = args.get_ctrl_optim_init_alpha();
          double tol_obj= args.get_ctrl_optim_tol_obj();
          double tol_rel_obj = args.get_ctrl_optim_tol_rel_obj();
          double tol_grad = args.get_ctrl_optim_tol_grad();
          double tol_rel_grad = args.get_ctrl_optim_tol_rel_grad();
          double tol_param = args.get_ctrl_optim_tol_param();
          int refresh = args.get_ctrl_optim_refresh();
          return_code
            = stan::services::optimize::bfgs(model, *init_context_ptr,
                                             random_seed, id, init_radius,
                                             init_alpha,
                                             tol_obj,
                                             tol_rel_obj,
                                             tol_grad,
                                             tol_rel_grad,
                                             tol_param,
                                             num_iterations,
                                             save_iterations,
                                             refresh,
                                             interrupt, logger,
                                             init_writer, sample_writer);
        }
        if (args.get_ctrl_optim_algorithm() == LBFGS) {
          int history_size = args.get_ctrl_optim_history_size();
          double init_alpha = args.get_ctrl_optim_init_alpha();
          double tol_obj= args.get_ctrl_optim_tol_obj();
          double tol_rel_obj = args.get_ctrl_optim_tol_rel_obj();
          double tol_grad = args.get_ctrl_optim_tol_grad();
          double tol_rel_grad = args.get_ctrl_optim_tol_rel_grad();
          double tol_param = args.get_ctrl_optim_tol_param();
          int refresh = args.get_ctrl_optim_refresh();
          return_code
            = stan::services::optimize::lbfgs(model, *init_context_ptr,
                                              random_seed, id, init_radius,
                                              history_size,
                                              init_alpha,
                                              tol_obj,
                                              tol_rel_obj,
                                              tol_grad,
                                              tol_rel_grad,
                                              tol_param,
                                              num_iterations,
                                              save_iterations,
                                              refresh,
                                              interrupt, logger,
                                              init_writer, sample_writer);
        }
        std::vector<double> params = sample_writer.x();
        double lp = params.front();
        params.erase(params.begin());
        holder.par = params;
        holder.value = lp;
      }
      if (args.get_method() == SAMPLING) {
        std::vector<std::string> sample_names;
        stan::mcmc::sample::get_sample_param_names(sample_names);
        std::vector<std::string> sampler_names;

        pystan_sample_writer *sample_writer_ptr;
        size_t sample_writer_offset;

        int num_warmup = args.get_ctrl_sampling_warmup();
        int num_samples = args.get_iter() - num_warmup;
        int num_thin = args.get_ctrl_sampling_thin();
        bool save_warmup = args.get_ctrl_sampling_save_warmup();
        int refresh = args.get_ctrl_sampling_refresh();
        int num_iter_save = args.get_ctrl_sampling_iter_save();
        int num_warmup_save = num_iter_save - args.get_ctrl_sampling_iter_save_wo_warmup();

        if (args.get_ctrl_sampling_algorithm() == Fixed_param) {
          sampler_names.resize(0);
          sample_writer_ptr = sample_writer_factory(&sample_stream,
                                                    comment_stream, "# ",
                                                    sample_names.size(),
                                                    sampler_names.size(),
                                                    constrained_param_names.size(),
                                                    num_iter_save,
                                                    num_warmup_save,
                                                    qoi_idx);
          return_code
            = stan::services::sample::fixed_param(model, *init_context_ptr,
                                                  random_seed, id, init_radius,
                                                  num_samples,
                                                  num_thin,
                                                  refresh,
                                                  interrupt,
                                                  logger, init_writer,
                                                  *sample_writer_ptr, diagnostic_writer);
        } else if (args.get_ctrl_sampling_algorithm() == NUTS) {
          sampler_names.resize(5);
          sampler_names[0] = "stepsize__";
          sampler_names[1] = "treedepth__";
          sampler_names[2] = "n_leapfrog__";
          sampler_names[3] = "divergent__";
          sampler_names[4] = "energy__";
          sample_writer_offset = sample_names.size() + sampler_names.size();

          sample_writer_ptr = sample_writer_factory(&sample_stream,
                                                    comment_stream, "# ",
                                                    sample_names.size(),
                                                    sampler_names.size(),
                                                    constrained_param_names.size(),
                                                    num_iter_save,
                                                    num_warmup_save,
                                                    qoi_idx);



          double stepsize = args.get_ctrl_sampling_stepsize();
          double stepsize_jitter = args.get_ctrl_sampling_stepsize_jitter();
          int max_depth = args.get_ctrl_sampling_max_treedepth();

          if (args.get_ctrl_sampling_metric() == DENSE_E) {
            if (!args.get_ctrl_sampling_adapt_engaged()) {
              if (args.get_metric_file_flag()) {
              return_code = stan::services::sample
                ::hmc_nuts_dense_e(model, *init_context_ptr, *metric_context_ptr,
                                   random_seed, id, init_radius,
                                   num_warmup, num_samples,
                                   num_thin, save_warmup, refresh,
                                   stepsize, stepsize_jitter, max_depth,
                                   interrupt, logger, init_writer,
                                   *sample_writer_ptr, diagnostic_writer);
              } else {
              return_code = stan::services::sample
                ::hmc_nuts_dense_e(model, *init_context_ptr,
                                   random_seed, id, init_radius,
                                   num_warmup, num_samples,
                                   num_thin, save_warmup, refresh,
                                   stepsize, stepsize_jitter, max_depth,
                                   interrupt, logger, init_writer,
                                   *sample_writer_ptr, diagnostic_writer);
              }
            } else {
              double delta = args.get_ctrl_sampling_adapt_delta();
              double gamma = args.get_ctrl_sampling_adapt_gamma();
              double kappa = args.get_ctrl_sampling_adapt_kappa();
              double t0 = args.get_ctrl_sampling_adapt_t0();
              unsigned int init_buffer = args.get_ctrl_sampling_adapt_init_buffer();
              unsigned int term_buffer = args.get_ctrl_sampling_adapt_term_buffer();
              unsigned int window = args.get_ctrl_sampling_adapt_window();

              if (args.get_metric_file_flag()) {
              return_code = stan::services::sample
                ::hmc_nuts_dense_e_adapt(model, *init_context_ptr, *metric_context_ptr,
                                         random_seed, id, init_radius,
                                         num_warmup, num_samples,
                                         num_thin, save_warmup, refresh,
                                         stepsize, stepsize_jitter, max_depth,
                                         delta, gamma, kappa,
                                         t0, init_buffer, term_buffer, window,
                                         interrupt, logger, init_writer,
                                         *sample_writer_ptr, diagnostic_writer);
              } else {
                return_code = stan::services::sample
                  ::hmc_nuts_dense_e_adapt(model, *init_context_ptr,
                                           random_seed, id, init_radius,
                                           num_warmup, num_samples,
                                           num_thin, save_warmup, refresh,
                                           stepsize, stepsize_jitter, max_depth,
                                           delta, gamma, kappa,
                                           t0, init_buffer, term_buffer, window,
                                           interrupt, logger, init_writer,
                                           *sample_writer_ptr, diagnostic_writer);
              }
            }
          } else if (args.get_ctrl_sampling_metric() == DIAG_E) {
            if (!args.get_ctrl_sampling_adapt_engaged()) {
              if (args.get_metric_file_flag()) {
              return_code = stan::services::sample
                ::hmc_nuts_diag_e(model, *init_context_ptr, *metric_context_ptr,
                                  random_seed, id, init_radius,
                                  num_warmup, num_samples,
                                  num_thin, save_warmup, refresh,
                                  stepsize, stepsize_jitter, max_depth,
                                  interrupt, logger, init_writer,
                                  *sample_writer_ptr, diagnostic_writer);
              } else {
                return_code = stan::services::sample
                  ::hmc_nuts_diag_e(model, *init_context_ptr,
                                    random_seed, id, init_radius,
                                    num_warmup, num_samples,
                                    num_thin, save_warmup, refresh,
                                    stepsize, stepsize_jitter, max_depth,
                                    interrupt, logger, init_writer,
                                    *sample_writer_ptr, diagnostic_writer);
              }
            } else {
              double delta = args.get_ctrl_sampling_adapt_delta();
              double gamma = args.get_ctrl_sampling_adapt_gamma();
              double kappa = args.get_ctrl_sampling_adapt_kappa();
              double t0 = args.get_ctrl_sampling_adapt_t0();
              unsigned int init_buffer = args.get_ctrl_sampling_adapt_init_buffer();
              unsigned int term_buffer = args.get_ctrl_sampling_adapt_term_buffer();
              unsigned int window = args.get_ctrl_sampling_adapt_window();

              if (args.get_metric_file_flag()) {
              return_code = stan::services::sample
                ::hmc_nuts_diag_e_adapt(model, *init_context_ptr, *metric_context_ptr,
                                        random_seed, id, init_radius,
                                        num_warmup, num_samples,
                                        num_thin, save_warmup, refresh,
                                        stepsize, stepsize_jitter, max_depth,
                                        delta, gamma, kappa,
                                        t0, init_buffer, term_buffer, window,
                                        interrupt, logger, init_writer,
                                        *sample_writer_ptr, diagnostic_writer);
              } else {
                return_code = stan::services::sample
                  ::hmc_nuts_diag_e_adapt(model, *init_context_ptr,
                                          random_seed, id, init_radius,
                                          num_warmup, num_samples,
                                          num_thin, save_warmup, refresh,
                                          stepsize, stepsize_jitter, max_depth,
                                          delta, gamma, kappa,
                                          t0, init_buffer, term_buffer, window,
                                          interrupt, logger, init_writer,
                                          *sample_writer_ptr, diagnostic_writer);
              }
            }
          } else if (args.get_ctrl_sampling_metric() == UNIT_E) {
            if (!args.get_ctrl_sampling_adapt_engaged()) {
              return_code = stan::services::sample
                ::hmc_nuts_unit_e(model, *init_context_ptr,
                                  random_seed, id, init_radius,
                                  num_warmup, num_samples,
                                  num_thin, save_warmup, refresh,
                                  stepsize, stepsize_jitter, max_depth,
                                  interrupt, logger, init_writer,
                                  *sample_writer_ptr, diagnostic_writer);
            } else {
              double delta = args.get_ctrl_sampling_adapt_delta();
              double gamma = args.get_ctrl_sampling_adapt_gamma();
              double kappa = args.get_ctrl_sampling_adapt_kappa();
              double t0 = args.get_ctrl_sampling_adapt_t0();

              return_code = stan::services::sample
                ::hmc_nuts_unit_e_adapt(model, *init_context_ptr,
                                        random_seed, id, init_radius,
                                        num_warmup, num_samples,
                                        num_thin, save_warmup, refresh,
                                        stepsize, stepsize_jitter, max_depth,
                                        delta, gamma, kappa, t0,
                                        interrupt, logger, init_writer,
                                        *sample_writer_ptr, diagnostic_writer);
            }
          }
        } else if (args.get_ctrl_sampling_algorithm() == HMC) {
          sampler_names.resize(3);
          sampler_names[0] = "stepsize__";
          sampler_names[1] = "int_time__";
          sampler_names[2] = "energy__";
          sample_writer_offset = sample_names.size() + sampler_names.size();

          sample_writer_ptr = sample_writer_factory(&sample_stream,
                                                    comment_stream, "# ",
                                                    sample_names.size(),
                                                    sampler_names.size(),
                                                    constrained_param_names.size(),
                                                    num_iter_save,
                                                    num_warmup_save,
                                                    qoi_idx);

          double stepsize = args.get_ctrl_sampling_stepsize();
          double stepsize_jitter = args.get_ctrl_sampling_stepsize_jitter();
          double int_time = args.get_ctrl_sampling_int_time();

          if (args.get_ctrl_sampling_metric() == DENSE_E) {
            if (!args.get_ctrl_sampling_adapt_engaged()) {
              if (args.get_metric_file_flag()) {
              return_code = stan::services::sample
                ::hmc_static_dense_e(model, *init_context_ptr, *metric_context_ptr,
                                     random_seed, id, init_radius,
                                     num_warmup, num_samples,
                                     num_thin, save_warmup, refresh,
                                     stepsize, stepsize_jitter, int_time,
                                     interrupt, logger, init_writer,
                                     *sample_writer_ptr, diagnostic_writer);
              } else {
                return_code = stan::services::sample
                  ::hmc_static_dense_e(model, *init_context_ptr,
                                       random_seed, id, init_radius,
                                       num_warmup, num_samples,
                                       num_thin, save_warmup, refresh,
                                       stepsize, stepsize_jitter, int_time,
                                       interrupt, logger, init_writer,
                                       *sample_writer_ptr, diagnostic_writer);
              }
            } else {
              double delta = args.get_ctrl_sampling_adapt_delta();
              double gamma = args.get_ctrl_sampling_adapt_gamma();
              double kappa = args.get_ctrl_sampling_adapt_kappa();
              double t0 = args.get_ctrl_sampling_adapt_t0();
              unsigned int init_buffer = args.get_ctrl_sampling_adapt_init_buffer();
              unsigned int term_buffer = args.get_ctrl_sampling_adapt_term_buffer();
              unsigned int window = args.get_ctrl_sampling_adapt_window();

              if (args.get_metric_file_flag()) {
              return_code = stan::services::sample
                ::hmc_static_dense_e_adapt(model, *init_context_ptr, *metric_context_ptr,
                                           random_seed, id, init_radius,
                                           num_warmup, num_samples,
                                           num_thin, save_warmup, refresh,
                                           stepsize, stepsize_jitter, int_time,
                                           delta, gamma, kappa, t0,
                                           init_buffer, term_buffer, window,
                                           interrupt, logger, init_writer,
                                           *sample_writer_ptr, diagnostic_writer);
              } else {
                return_code = stan::services::sample
                  ::hmc_static_dense_e_adapt(model, *init_context_ptr,
                                             random_seed, id, init_radius,
                                             num_warmup, num_samples,
                                             num_thin, save_warmup, refresh,
                                             stepsize, stepsize_jitter, int_time,
                                             delta, gamma, kappa, t0,
                                             init_buffer, term_buffer, window,
                                             interrupt, logger, init_writer,
                                             *sample_writer_ptr, diagnostic_writer);
              }

            }
          } else if (args.get_ctrl_sampling_metric() == DIAG_E) {
            if (!args.get_ctrl_sampling_adapt_engaged()) {
              if (args.get_metric_file_flag()) {
              return_code = stan::services::sample
                ::hmc_static_diag_e(model, *init_context_ptr, *metric_context_ptr,
                                    random_seed, id, init_radius,
                                    num_warmup, num_samples,
                                    num_thin, save_warmup, refresh,
                                    stepsize, stepsize_jitter, int_time,
                                    interrupt, logger, init_writer,
                                    *sample_writer_ptr, diagnostic_writer);
              } else {
                return_code = stan::services::sample
                  ::hmc_static_diag_e(model, *init_context_ptr,
                                      random_seed, id, init_radius,
                                      num_warmup, num_samples,
                                      num_thin, save_warmup, refresh,
                                      stepsize, stepsize_jitter, int_time,
                                      interrupt, logger, init_writer,
                                      *sample_writer_ptr, diagnostic_writer);
              }

            } else {
              double delta = args.get_ctrl_sampling_adapt_delta();
              double gamma = args.get_ctrl_sampling_adapt_gamma();
              double kappa = args.get_ctrl_sampling_adapt_kappa();
              double t0 = args.get_ctrl_sampling_adapt_t0();
              unsigned int init_buffer = args.get_ctrl_sampling_adapt_init_buffer();
              unsigned int term_buffer = args.get_ctrl_sampling_adapt_term_buffer();
              unsigned int window = args.get_ctrl_sampling_adapt_window();

              if (args.get_metric_file_flag()) {
              return_code = stan::services::sample
                ::hmc_static_diag_e_adapt(model, *init_context_ptr, *metric_context_ptr,
                                          random_seed, id, init_radius,
                                          num_warmup, num_samples,
                                          num_thin, save_warmup, refresh,
                                          stepsize, stepsize_jitter, int_time,
                                          delta, gamma, kappa, t0,
                                          init_buffer, term_buffer, window,
                                          interrupt, logger, init_writer,
                                          *sample_writer_ptr, diagnostic_writer);
              } else {
                return_code = stan::services::sample
                  ::hmc_static_diag_e_adapt(model, *init_context_ptr,
                                            random_seed, id, init_radius,
                                            num_warmup, num_samples,
                                            num_thin, save_warmup, refresh,
                                            stepsize, stepsize_jitter, int_time,
                                            delta, gamma, kappa, t0,
                                            init_buffer, term_buffer, window,
                                            interrupt, logger, init_writer,
                                            *sample_writer_ptr, diagnostic_writer);
              }
            }
          } else if (args.get_ctrl_sampling_metric() == UNIT_E) {
            if (args.get_ctrl_sampling_adapt_engaged()) {
              return_code = stan::services::sample
                ::hmc_static_unit_e(model, *init_context_ptr,
                                    random_seed, id, init_radius,
                                    num_warmup, num_samples,
                                    num_thin, save_warmup, refresh,
                                    stepsize, stepsize_jitter, int_time,
                                    interrupt, logger, init_writer,
                                    *sample_writer_ptr, diagnostic_writer);

            } else  {
              double delta = args.get_ctrl_sampling_adapt_delta();
              double gamma = args.get_ctrl_sampling_adapt_gamma();
              double kappa = args.get_ctrl_sampling_adapt_kappa();
              double t0 = args.get_ctrl_sampling_adapt_t0();

              return_code = stan::services::sample
                ::hmc_static_unit_e_adapt(model, *init_context_ptr,
                            random_seed, id, init_radius,
                            num_warmup, num_samples,
                            num_thin, save_warmup, refresh,
                            stepsize, stepsize_jitter, int_time,
                            delta, gamma, kappa, t0,
                            interrupt, logger, init_writer,
                            *sample_writer_ptr, diagnostic_writer);
            }
          }
        }
        double mean_lp(0);
        std::vector<double> mean_pars;
        mean_pars.resize(constrained_param_names.size(), 0);

        sample_writer_offset = sample_names.size() + sampler_names.size();
        if (args.get_ctrl_sampling_iter_save_wo_warmup() > 0) {
          double inverse_saved = 1.0 / args.get_ctrl_sampling_iter_save_wo_warmup();
          mean_lp = sample_writer_ptr->sum_.sum()[0] * inverse_saved;
          for (size_t n = 0; n < mean_pars.size(); n++) {
            mean_pars[n] = sample_writer_ptr->sum_.sum()[sample_writer_offset + n] * inverse_saved;
          }
        }

        holder.chains = sample_writer_ptr->values_.x();
        holder.test_grad = false;
        holder.args = args;
        holder.inits = unconstrained_to_constrained(model, random_seed, id,
                                                    init_writer.x());
        holder.mean_pars = mean_pars;
        holder.mean_lp__ = mean_lp;

        std::string comments = comment_stream.str();
        size_t start = 0;
        size_t end = 0;
        std::string adaptation_info;
        if ((start = comments.find("# Adaptation")) != std::string::npos) {
          end = comments.find("# \n", start);
          adaptation_info = comments.substr(start,
                                            end - start);
        }
        double warmDeltaT = 0;
        double sampleDeltaT = 0;
        if ((start = comments.find("Elapsed Time: ")) != std::string::npos) {
          start += strlen("Elapsed Time: ");
          end = comments.find("seconds", start + 1);
          std::stringstream ss(comments.substr(start, end));
          ss >> warmDeltaT;

          start = comments.find("# ", end) + strlen("# ");
          end = comments.find("seconds (Sampling)", start + 1);
          ss.str(comments.substr(start, end));
          ss >> sampleDeltaT;
        }
        holder.adaptation_info = adaptation_info;

        std::vector<std::vector<double> > slst(sample_writer_ptr->sampler_values_.x().begin()+1,
                                               sample_writer_ptr->sampler_values_.x().end());

        std::vector<std::string> slst_names(sample_names.begin()+1, sample_names.end());
        slst_names.insert(slst_names.end(), sampler_names.begin(), sampler_names.end());

        holder.sampler_params = slst;
        holder.sampler_param_names = slst_names;
        holder.chain_names = fnames_oi;

        delete sample_writer_ptr;
      }
      if (args.get_method() == VARIATIONAL) {
        int grad_samples = args.get_ctrl_variational_grad_samples();
        int elbo_samples = args.get_ctrl_variational_elbo_samples();
        int max_iterations = args.get_iter();
        double tol_rel_obj = args.get_ctrl_variational_tol_rel_obj();
        double eta = args.get_ctrl_variational_eta();
        bool adapt_engaged = args.get_ctrl_variational_adapt_engaged();
        int adapt_iterations = args.get_ctrl_variational_adapt_iter();
        int eval_elbo = args.get_ctrl_variational_eval_elbo();
        int output_samples = args.get_ctrl_variational_output_samples();

        pystan_sample_writer *sample_writer_ptr
          = sample_writer_factory(&sample_stream,
                                  comment_stream, "# ",
                                  3,
                                  0,
                                  constrained_param_names.size(),
                                  output_samples + 1,
                                  0,
                                  qoi_idx);

        if (args.get_ctrl_variational_algorithm() == FULLRANK) {
          return_code = stan::services::experimental::advi
            ::fullrank(model, *init_context_ptr,
                       random_seed, id, init_radius,
                       grad_samples, elbo_samples,
                       max_iterations, tol_rel_obj, eta,
                       adapt_engaged, adapt_iterations,
                       eval_elbo, output_samples,
                       interrupt, logger, init_writer,
                       *sample_writer_ptr, diagnostic_writer);
        } else {
          return_code = stan::services::experimental::advi
            ::meanfield(model, *init_context_ptr,
                        random_seed, id, init_radius,
                        grad_samples, elbo_samples,
                        max_iterations, tol_rel_obj, eta,
                        adapt_engaged, adapt_iterations,
                        eval_elbo, output_samples,
                        interrupt, logger, init_writer,
                        *sample_writer_ptr, diagnostic_writer);
        }
        std::vector<std::vector<double> > slst(sample_writer_ptr->values_.x().begin(),
                                               sample_writer_ptr->values_.x().end());

        std::vector<double> mean_pars(slst.size() - 1);
        std::vector<std::string> mean_par_names(slst.size() - 1);
        for (size_t n = 0; n < mean_pars.size(); ++n) {
          mean_pars[n] = slst[n][0];
          mean_par_names[n] = fnames_oi[n];
        }

        for (size_t n = 0; n < slst.size(); ++n) {
          slst[n].erase(slst[n].begin());
        }

        holder.sampler_params = slst;
        holder.sampler_param_names = fnames_oi;
        holder.mean_pars = mean_pars;
        holder.mean_par_names = mean_par_names;
        holder.args = args;
        holder.inits = unconstrained_to_constrained(model, random_seed, id,
                                                    init_writer.x());
        delete sample_writer_ptr;
      }
      init_context_ptr.reset();
      metric_context_ptr.reset();
      return return_code;
    }
  }

  template <class Model, class RNG_t>
  class stan_fit {

  private:
    pystan::io::py_var_context data_;
    Model model_;
    RNG_t base_rng;
    const std::vector<std::string> names_;
    const std::vector<std::vector<unsigned int> > dims_;
    const unsigned int num_params_;

    std::vector<std::string> names_oi_; // parameters of interest
    std::vector<std::vector<unsigned int> > dims_oi_;
    std::vector<size_t> names_oi_tidx_;  // the total indexes of names2.
    // std::vector<size_t> midx_for_col2row; // indices for mapping col-major to row-major
    std::vector<unsigned int> starts_oi_;
    unsigned int num_params2_;  // total number of POI's.
    std::vector<std::string> fnames_oi_;

  private:
    /**
     * Tell if a parameter name is an element of an array parameter.
     * Note that it only supports full specified name; slicing
     * is not supported. The test only tries to see if there
     * are brackets.
     */
    bool is_flatname(const std::string& name) {
      return name.find('[') != name.npos && name.find(']') != name.npos;
    }

    /*
     * Update the parameters we are interested for the model.
     * As well, the dimensions vector for the parameters are
     * updated.
     */
    void update_param_oi0(const std::vector<std::string>& pnames) {
      names_oi_.clear();
      dims_oi_.clear();
      names_oi_tidx_.clear();

      std::vector<unsigned int> starts;
      calc_starts(dims_, starts);
      for (std::vector<std::string>::const_iterator it = pnames.begin();
           it != pnames.end();
           ++it) {
        size_t p = find_index(names_, *it);
        if (p != names_.size()) {
          names_oi_.push_back(*it);
          dims_oi_.push_back(dims_[p]);
          if (*it == "lp__") {
            names_oi_tidx_.push_back(-1); // -1 for lp__ as it is not really a parameter
            continue;
          }
          size_t i_num = calc_num_params(dims_[p]);
          size_t i_start = starts[p];
          for (size_t j = i_start; j < i_start + i_num; j++)
            names_oi_tidx_.push_back(j);
        }
      }
      calc_starts(dims_oi_, starts_oi_);
      num_params2_ = names_oi_tidx_.size();
    }

  public:
    bool update_param_oi(std::vector<std::string> pars) {
      std::vector<std::string> pnames = pars;
      if (std::find(pnames.begin(), pnames.end(), "lp__") == pnames.end())
        pnames.push_back("lp__");
      update_param_oi0(pnames);
      get_all_flatnames(names_oi_, dims_oi_, fnames_oi_, true);
      return true;
    }

    stan_fit(vars_r_t& vars_r, vars_i_t& vars_i, unsigned int random_seed) :
      data_(vars_r, vars_i),
      model_(data_, random_seed, &std::cout),
      base_rng(static_cast<boost::uint32_t>(std::time(0))),
      names_(get_param_names(model_)),
      dims_(get_param_dims(model_)),
      num_params_(calc_total_num_params(dims_)),
      names_oi_(names_),
      dims_oi_(dims_),
      num_params2_(num_params_)
    {
      for (size_t j = 0; j < num_params2_ - 1; j++)
        names_oi_tidx_.push_back(j);
      names_oi_tidx_.push_back(-1); // lp__
      calc_starts(dims_oi_, starts_oi_);
      get_all_flatnames(names_oi_, dims_oi_, fnames_oi_, true);
      // get_all_indices_col2row(dims_, midx_for_col2row);
    }

    /**
     * Transform the parameters from its defined support
     * to unconstrained space
     *
     * @param vars_r initial values for a chain
     * @param vars_i initial values for a chain
     */
    std::vector<double> unconstrain_pars(vars_r_t& vars_r, vars_i_t& vars_i) {
      pystan::io::py_var_context par_context(vars_r, vars_i);
      std::vector<int> params_i;
      std::vector<double> params_r;
      model_.transform_inits(par_context, params_i, params_r, &std::cout);
      return params_r;
    }

    std::vector<std::string> unconstrained_param_names(bool include_tparams, bool include_gqs) {
      std::vector<std::string> names;
      model_.unconstrained_param_names(names, include_tparams, include_gqs);
      return names;
    }

    std::vector<std::string> constrained_param_names(bool include_tparams, bool include_gqs) {
      std::vector<std::string> names;
      model_.constrained_param_names(names, include_tparams, include_gqs);
      return names;
    }

    /**
     * Contrary to unconstrain_pars, transform parameters
     * from unconstrained support to the constrained.
     *
     * @param params_r The real parameters on the unconstrained
     *  space.
     *
     */
    std::vector<double> constrain_pars(std::vector<double>& params_r) {
      std::vector<double> par;
      if (params_r.size() != model_.num_params_r()) {
        std::stringstream msg;
        msg << "Number of unconstrained parameters does not match "
               "that of the model ("
            << params_r.size() << " vs "
            << model_.num_params_r()
            << ").";
        throw std::domain_error(msg.str());
      }
      std::vector<int> params_i(model_.num_params_i());
      model_.write_array(base_rng, params_r, params_i, par);
      return par;
    }

    /**
     * Expose the log_prob of the model to stan_fit so R user
     * can call this function.
     *
     * @param upar The real parameters on the unconstrained
     *  space.
     */
    double log_prob(std::vector<double> upar, bool jacobian_adjust_transform, bool gradient) {
      using std::vector;
      vector<double> par_r = upar;
      if (par_r.size() != model_.num_params_r()) {
        std::stringstream msg;
        msg << "Number of unconstrained parameters does not match "
               "that of the model ("
            << par_r.size() << " vs "
            << model_.num_params_r()
            << ").";
        throw std::domain_error(msg.str());
      }
      vector<int> par_i(model_.num_params_i(), 0);
      if (!gradient) {
        if (jacobian_adjust_transform) {
          return stan::model::log_prob_propto<true>(model_, par_r, par_i, &std::cout);
        } else {
          return stan::model::log_prob_propto<false>(model_, par_r, par_i, &std::cout);
        }
      }

      std::vector<double> grad;
      double lp;
      if (jacobian_adjust_transform)
        lp = stan::model::log_prob_grad<true,true>(model_, par_r, par_i, grad, &std::cout);
      else
        lp = stan::model::log_prob_grad<true,false>(model_, par_r, par_i, grad, &std::cout);
      // RStan returns the gradient as an attribute. Python numbers don't have attributes.
      return lp;
    }

    /**
     * Expose the grad_log_prob of the model to stan_fit so R user
     * can call this function.
     *
     * @param upar The real parameters on the unconstrained
     *  space.
     * @param jacobian_adjust_transform TRUE/FALSE, whether
     *  we add the term due to the transform from constrained
     *  space to unconstrained space implicitly done in Stan.
     */
    std::vector<double> grad_log_prob(std::vector<double> upar, bool jacobian_adjust_transform) {
      std::vector<double> par_r = upar;
      if (par_r.size() != model_.num_params_r()) {
        std::stringstream msg;
        msg << "Number of unconstrained parameters does not match "
               "that of the model ("
            << par_r.size() << " vs "
            << model_.num_params_r()
            << ").";
        throw std::domain_error(msg.str());
      }
      std::vector<int> par_i(model_.num_params_i(), 0);
      std::vector<double> gradient;
      // RStan returns the lp as an attribute. Python numbers don't have attributes.
      if (jacobian_adjust_transform)
        stan::model::log_prob_grad<true,true>(model_, par_r, par_i, gradient, &std::cout);
      else
        stan::model::log_prob_grad<true,false>(model_, par_r, par_i, gradient, &std::cout);
      return gradient;
    }

    /**
     * Return the number of unconstrained parameters
     */
    int num_pars_unconstrained() {
      int n = model_.num_params_r();
      return n;
    }

    int call_sampler(StanArgs& args, StanHolder& holder) {
      int ret;
      try {
        ret = command(args, model_, holder, names_oi_tidx_,
                      fnames_oi_, base_rng);
      } catch (std::exception const& e) {
        throw std::runtime_error(e.what());
      }
      if (ret != 0) {
        throw std::runtime_error("Something went wrong after call_sampler.");
      }
      return ret; // FIXME: rstan returns holder
    }

    std::vector<std::string> param_names() const {
       return names_;
    }

    std::vector<std::string> param_names_oi() const {
       return names_oi_;
    }

    std::vector<std::vector<unsigned int> > param_dims() const {
        return dims_;
    }

    std::vector<std::vector<unsigned int> > param_dims_oi() const {
        return dims_oi_;
    }

    std::vector<std::string> param_fnames_oi() const {
       std::vector<std::string> fnames;
       get_all_flatnames(names_oi_, dims_oi_, fnames, true);
       return fnames;
    }

  };
}
#endif
