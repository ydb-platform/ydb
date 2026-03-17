#ifndef STAN_OPTIMIZATION_BFGS_HPP
#define STAN_OPTIMIZATION_BFGS_HPP

#include <stan/math/prim/mat.hpp>
#include <stan/model/log_prob_propto.hpp>
#include <stan/model/log_prob_grad.hpp>
#include <stan/optimization/bfgs_linesearch.hpp>
#include <stan/optimization/bfgs_update.hpp>
#include <stan/optimization/lbfgs_update.hpp>
#include <boost/math/special_functions/fpclassify.hpp>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>

namespace stan {
  namespace optimization {
    typedef enum {
      TERM_SUCCESS = 0,
      TERM_ABSX = 10,
      TERM_ABSF = 20,
      TERM_RELF = 21,
      TERM_ABSGRAD = 30,
      TERM_RELGRAD = 31,
      TERM_MAXIT = 40,
      TERM_LSFAIL = -1
    } TerminationCondition;

    template<typename Scalar = double>
    class ConvergenceOptions {
    public:
      ConvergenceOptions() {
        maxIts = 10000;
        fScale = 1.0;

        tolAbsX = 1e-8;
        tolAbsF = 1e-12;
        tolAbsGrad = 1e-8;

        tolRelF = 1e+4;
        tolRelGrad = 1e+3;
      }
      size_t maxIts;
      Scalar tolAbsX;
      Scalar tolAbsF;
      Scalar tolRelF;
      Scalar fScale;
      Scalar tolAbsGrad;
      Scalar tolRelGrad;
    };

    template<typename Scalar = double>
    class LSOptions {
    public:
      LSOptions() {
        c1 = 1e-4;
        c2 = 0.9;
        alpha0 = 1e-3;
        minAlpha = 1e-12;
        maxLSIts = 20;
        maxLSRestarts = 10;
      }
      Scalar c1;
      Scalar c2;
      Scalar alpha0;
      Scalar minAlpha;
      Scalar maxLSIts;
      Scalar maxLSRestarts;
    };
    template<typename FunctorType, typename QNUpdateType,
             typename Scalar = double, int DimAtCompile = Eigen::Dynamic>
    class BFGSMinimizer {
    public:
      typedef Eigen::Matrix<Scalar, DimAtCompile, 1> VectorT;
      typedef Eigen::Matrix<Scalar, DimAtCompile, DimAtCompile> HessianT;

    protected:
      FunctorType &_func;
      VectorT _gk, _gk_1, _xk_1, _xk, _pk, _pk_1;
      Scalar _fk, _fk_1, _alphak_1;
      Scalar _alpha, _alpha0;
      size_t _itNum;
      std::string _note;
      QNUpdateType _qn;

    public:
      LSOptions<Scalar> _ls_opts;
      ConvergenceOptions<Scalar> _conv_opts;

      QNUpdateType &get_qnupdate() { return _qn; }
      const QNUpdateType &get_qnupdate() const { return _qn; }

      const Scalar &curr_f() const { return _fk; }
      const VectorT &curr_x() const { return _xk; }
      const VectorT &curr_g() const { return _gk; }
      const VectorT &curr_p() const { return _pk; }

      const Scalar &prev_f() const { return _fk_1; }
      const VectorT &prev_x() const { return _xk_1; }
      const VectorT &prev_g() const { return _gk_1; }
      const VectorT &prev_p() const { return _pk_1; }
      Scalar prev_step_size() const { return _pk_1.norm()*_alphak_1; }

      inline Scalar rel_grad_norm() const {
        return -_pk.dot(_gk) / std::max(std::fabs(_fk), _conv_opts.fScale);
      }
      inline Scalar rel_obj_decrease() const {
        return std::fabs(_fk_1 - _fk) / std::max(std::fabs(_fk_1),
                                                 std::max(std::fabs(_fk),
                                                          _conv_opts.fScale));
      }

      const Scalar &alpha0() const { return _alpha0; }
      const Scalar &alpha() const { return _alpha; }
      const size_t iter_num() const { return _itNum; }

      const std::string &note() const { return _note; }

      std::string get_code_string(int retCode) {
        switch (retCode) {
          case TERM_SUCCESS:
            return std::string("Successful step completed");
          case TERM_ABSF:
            return std::string("Convergence detected: absolute change "
                               "in objective function was below tolerance");
          case TERM_RELF:
            return std::string("Convergence detected: relative change "
                               "in objective function was below tolerance");
          case TERM_ABSGRAD:
            return std::string("Convergence detected: "
                               "gradient norm is below tolerance");
          case TERM_RELGRAD:
            return std::string("Convergence detected: relative "
                               "gradient magnitude is below tolerance");
          case TERM_ABSX:
            return std::string("Convergence detected: "
                               "absolute parameter change was below tolerance");
          case TERM_MAXIT:
            return std::string("Maximum number of iterations hit, "
                               "may not be at an optima");
          case TERM_LSFAIL:
            return std::string("Line search failed to achieve a sufficient "
                               "decrease, no more progress can be made");
          default:
            return std::string("Unknown termination code");
        }
      }

      explicit BFGSMinimizer(FunctorType &f) : _func(f) { }

      void initialize(const VectorT &x0) {
        int ret;
        _xk = x0;
        ret = _func(_xk, _fk, _gk);
        if (ret) {
          throw std::runtime_error("Error evaluating initial BFGS point.");
        }
        _pk = -_gk;

        _itNum = 0;
        _note = "";
      }

      int step() {
        Scalar gradNorm, stepNorm;
        VectorT sk, yk;
        int retCode(0);
        int resetB(0);

        _itNum++;

        if (_itNum == 1) {
          resetB = 1;
          _note = "";
        } else {
          resetB = 0;
          _note = "";
        }

        while (true) {
          if (resetB) {
            // Reset the Hessian approximation
            _pk.noalias() = -_gk;
          }

          // Get an initial guess for the step size (alpha)
          if (_itNum > 1 && resetB != 2) {
            // use cubic interpolation based on the previous step
            _alpha0 = _alpha = std::min(1.0,
                                        1.01*CubicInterp(_gk_1.dot(_pk_1),
                                                         _alphak_1,
                                                         _fk - _fk_1,
                                                         _gk.dot(_pk_1),
                                                         _ls_opts.minAlpha,
                                                         1.0));
          } else {
            // On the first step (or, after a reset) use the default step size
            _alpha0 = _alpha = _ls_opts.alpha0;
          }

          // Perform the line search.  If successful, the results are in the
          // variables: _xk_1, _fk_1 and _gk_1.
          retCode = WolfeLineSearch(_func, _alpha, _xk_1, _fk_1, _gk_1,
                                    _pk, _xk, _fk, _gk,
                                    _ls_opts.c1, _ls_opts.c2,
                                    _ls_opts.minAlpha,
                                    _ls_opts.maxLSIts,
                                    _ls_opts.maxLSRestarts);
          if (retCode) {
            // Line search failed...
            if (resetB) {
              // did a Hessian reset and it still failed,
              // and nothing left to try
              retCode = TERM_LSFAIL;
              return retCode;
            } else {
              // try resetting the Hessian approximation
              resetB = 2;
              _note += "LS failed, Hessian reset";
              continue;
            }
          } else {
            break;
          }
        }

        // Swap things so that k is the most recent iterate
        std::swap(_fk, _fk_1);
        _xk.swap(_xk_1);
        _gk.swap(_gk_1);
        _pk.swap(_pk_1);

        sk.noalias() = _xk - _xk_1;
        yk.noalias() = _gk - _gk_1;

        gradNorm = _gk.norm();
        stepNorm = sk.norm();

        // Update QN approximation
        if (resetB) {
          // If the QN approximation was reset, automatically scale it
          // and update the step-size accordingly
          Scalar B0fact = _qn.update(yk, sk, true);
          _pk_1 /= B0fact;
          _alphak_1 = _alpha*B0fact;
        } else {
          _qn.update(yk, sk);
          _alphak_1 = _alpha;
        }
        // Compute search direction for next step
        _qn.search_direction(_pk, _gk);

        // Check for convergence
        if (std::fabs(_fk_1 - _fk) < _conv_opts.tolAbsF) {
          // Objective function improvement wasn't sufficient
          retCode = TERM_ABSF;
        } else if (gradNorm < _conv_opts.tolAbsGrad) {
          retCode = TERM_ABSGRAD;  // Gradient norm was below threshold
        } else if (stepNorm < _conv_opts.tolAbsX) {
          retCode = TERM_ABSX;  // Change in x was too small
        } else if (_itNum >= _conv_opts.maxIts) {
          retCode = TERM_MAXIT;  // Max number of iterations hit
        } else if (rel_obj_decrease()
                 < _conv_opts.tolRelF
                 * std::numeric_limits<Scalar>::epsilon()) {
          // Relative improvement in objective function wasn't sufficient
          retCode = TERM_RELF;
        } else if (rel_grad_norm()
                   < _conv_opts.tolRelGrad
                   * std::numeric_limits<Scalar>::epsilon()) {
          // Relative gradient norm was below threshold
          retCode = TERM_RELGRAD;
        } else {
          // Step was successful more progress to be made
          retCode = TERM_SUCCESS;
        }

        return retCode;
      }

      int minimize(VectorT &x0) {
        int retcode;
        initialize(x0);
        while (!(retcode = step()))
          continue;
        x0 = _xk;
        return retcode;
      }
    };

    template <class M>
    class ModelAdaptor {
    private:
      M& _model;
      std::vector<int> _params_i;
      std::ostream* _msgs;
      std::vector<double> _x, _g;
      size_t _fevals;

    public:
      ModelAdaptor(M& model,
                   const std::vector<int>& params_i,
                   std::ostream* msgs)
      : _model(model), _params_i(params_i), _msgs(msgs), _fevals(0) {}

      size_t fevals() const { return _fevals; }
      int operator()(const Eigen::Matrix<double, Eigen::Dynamic, 1> &x,
                     double &f) {
        using Eigen::Matrix;
        using Eigen::Dynamic;
        using stan::math::index_type;
        using stan::model::log_prob_propto;
        typedef typename index_type<Matrix<double, Dynamic, 1> >::type idx_t;

        _x.resize(x.size());
        for (idx_t i = 0; i < x.size(); i++)
          _x[i] = x[i];

        try {
          f = - log_prob_propto<false>(_model, _x, _params_i, _msgs);
        } catch (const std::exception& e) {
          if (_msgs)
            (*_msgs) << e.what() << std::endl;
          return 1;
        }

        if (boost::math::isfinite(f)) {
          return 0;
        } else {
          if (_msgs)
            *_msgs << "Error evaluating model log probability: "
                      "Non-finite function evaluation." << std::endl;
          return 2;
        }
      }
      int operator()(const Eigen::Matrix<double, Eigen::Dynamic, 1> &x,
                     double &f,
                     Eigen::Matrix<double, Eigen::Dynamic, 1> &g) {
        using Eigen::Matrix;
        using Eigen::Dynamic;
        using stan::math::index_type;
        using stan::model::log_prob_grad;
        typedef typename index_type<Matrix<double, Dynamic, 1> >::type idx_t;

        _x.resize(x.size());
        for (idx_t i = 0; i < x.size(); i++)
          _x[i] = x[i];

        _fevals++;

        try {
          f = - log_prob_grad<true, false>(_model, _x, _params_i, _g, _msgs);
        } catch (const std::exception& e) {
          if (_msgs)
            (*_msgs) << e.what() << std::endl;
          return 1;
        }

        g.resize(_g.size());
        for (size_t i = 0; i < _g.size(); i++) {
          if (!boost::math::isfinite(_g[i])) {
            if (_msgs)
              *_msgs << "Error evaluating model log probability: "
                                 "Non-finite gradient." << std::endl;
            return 3;
          }
          g[i] = -_g[i];
        }

        if (boost::math::isfinite(f)) {
          return 0;
        } else {
          if (_msgs)
            *_msgs << "Error evaluating model log probability: "
                   << "Non-finite function evaluation."
                   << std::endl;
          return 2;
        }
      }
      int df(const Eigen::Matrix<double, Eigen::Dynamic, 1> &x,
             Eigen::Matrix<double, Eigen:: Dynamic, 1> &g) {
        double f;
        return (*this)(x, f, g);
      }
    };

    template<typename M, typename QNUpdateType, typename Scalar = double,
             int DimAtCompile = Eigen::Dynamic>
    class BFGSLineSearch
      : public BFGSMinimizer<ModelAdaptor<M>, QNUpdateType,
                             Scalar, DimAtCompile> {
    private:
      ModelAdaptor<M> _adaptor;

    public:
      typedef BFGSMinimizer<ModelAdaptor<M>, QNUpdateType, Scalar, DimAtCompile>
      BFGSBase;
      typedef typename BFGSBase::VectorT vector_t;
      typedef typename stan::math::index_type<vector_t>::type idx_t;

      BFGSLineSearch(M& model,
                     const std::vector<double>& params_r,
                     const std::vector<int>& params_i,
                     std::ostream* msgs = 0)
        : BFGSBase(_adaptor),
          _adaptor(model, params_i, msgs) {
        initialize(params_r);
      }

      void initialize(const std::vector<double>& params_r) {
        Eigen::Matrix<double, Eigen::Dynamic, 1> x;
        x.resize(params_r.size());
        for (size_t i = 0; i < params_r.size(); i++)
          x[i] = params_r[i];
        BFGSBase::initialize(x);
      }

      size_t grad_evals() { return _adaptor.fevals(); }
      double logp() { return -(this->curr_f()); }
      double grad_norm() { return this->curr_g().norm(); }
      void grad(std::vector<double>& g) {
        const vector_t &cg(this->curr_g());
        g.resize(cg.size());
        for (idx_t i = 0; i < cg.size(); i++)
          g[i] = -cg[i];
      }
      void params_r(std::vector<double>& x) {
        const vector_t &cx(this->curr_x());
        x.resize(cx.size());
        for (idx_t i = 0; i < cx.size(); i++)
          x[i] = cx[i];
      }
    };

  }

}

#endif
