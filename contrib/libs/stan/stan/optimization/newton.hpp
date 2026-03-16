#ifndef STAN_OPTIMIZATION_NEWTON_HPP
#define STAN_OPTIMIZATION_NEWTON_HPP

#include <stan/model/grad_hess_log_prob.hpp>
#include <stan/model/log_prob_grad.hpp>
#include <Eigen/Dense>
#include <Eigen/Cholesky>
#include <Eigen/Eigenvalues>
#include <vector>

namespace stan {
  namespace optimization {

    typedef Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic> matrix_d;
    typedef Eigen::Matrix<double, Eigen::Dynamic, 1> vector_d;

    // Negates any positive eigenvalues in H so that H is negative
    // definite, and then solves Hu = g and stores the result into
    // g. Avoids problems due to non-log-concave distributions.
    inline void make_negative_definite_and_solve(matrix_d& H, vector_d& g) {
      Eigen::SelfAdjointEigenSolver<matrix_d> solver(H);
      matrix_d eigenvectors = solver.eigenvectors();
      vector_d eigenvalues = solver.eigenvalues();
      vector_d eigenprojections = eigenvectors.transpose() * g;
      for (int i = 0; i < g.size(); i++) {
        eigenprojections[i] = -eigenprojections[i] / fabs(eigenvalues[i]);
      }
      g = eigenvectors * eigenprojections;
    }

    template <typename M>
    double newton_step(M& model,
                       std::vector<double>& params_r,
                       std::vector<int>& params_i,
                       std::ostream* output_stream = 0) {
        std::vector<double> gradient;
        std::vector<double> hessian;

        double f0
          = stan::model::grad_hess_log_prob<true, false>(model,
                                                         params_r, params_i,
                                                         gradient, hessian);
        matrix_d H(params_r.size(), params_r.size());
        for (size_t i = 0; i < hessian.size(); i++) {
          H(i) = hessian[i];
        }
        vector_d g(params_r.size());
        for (size_t i = 0; i < gradient.size(); i++)
          g(i) = gradient[i];
        make_negative_definite_and_solve(H, g);
//         H.ldlt().solveInPlace(g);

        std::vector<double> new_params_r(params_r.size());
        double step_size = 2;
        double min_step_size = 1e-50;
        double f1 = -1e100;

        while (f1 < f0) {
          step_size *= 0.5;
          if (step_size < min_step_size)
            return f0;

          for (size_t i = 0; i < params_r.size(); i++)
            new_params_r[i] = params_r[i] - step_size * g[i];
          try {
            f1 = stan::model::log_prob_grad<true, false>(model,
                                                         new_params_r,
                                                         params_i, gradient);
          } catch (std::exception& e) {
            // FIXME:  this is not a good way to handle a general exception
            f1 = -1e100;
          }
        }
        for (size_t i = 0; i < params_r.size(); i++)
          params_r[i] = new_params_r[i];

        return f1;
    }

  }
}
#endif
