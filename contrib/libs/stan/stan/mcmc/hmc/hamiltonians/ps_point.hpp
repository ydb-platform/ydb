#ifndef STAN_MCMC_HMC_HAMILTONIANS_PS_POINT_HPP
#define STAN_MCMC_HMC_HAMILTONIANS_PS_POINT_HPP

#include <stan/callbacks/writer.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <boost/lexical_cast.hpp>
#include <Eigen/Dense>
#include <string>
#include <vector>

namespace stan {
  namespace mcmc {
    using Eigen::Dynamic;

    /**
     * Point in a generic phase space
     */
    class ps_point {
      friend class ps_point_test;

    public:
      explicit ps_point(int n)
        : q(n), p(n), V(0), g(n) {}

      ps_point(const ps_point& z)
        : q(z.q.size()), p(z.p.size()), V(z.V), g(z.g.size()) {
        fast_vector_copy_<double>(q, z.q);
        fast_vector_copy_<double>(p, z.p);
        fast_vector_copy_<double>(g, z.g);
      }

      ps_point& operator= (const ps_point& z) {
        if (this == &z)
          return *this;

        fast_vector_copy_<double>(q, z.q);

        V = z.V;

        fast_vector_copy_<double>(p, z.p);
        fast_vector_copy_<double>(g, z.g);

        return *this;
      }

      Eigen::VectorXd q;
      Eigen::VectorXd p;

      double V;
      Eigen::VectorXd g;

      virtual void get_param_names(std::vector<std::string>& model_names,
                                   std::vector<std::string>& names) {
        for (int i = 0; i < q.size(); ++i)
          names.push_back(model_names.at(i));
        for (int i = 0; i < q.size(); ++i)
          names.push_back(std::string("p_") + model_names.at(i));
        for (int i = 0; i < q.size(); ++i)
          names.push_back(std::string("g_") + model_names.at(i));
      }

      virtual void get_params(std::vector<double>& values) {
        for (int i = 0; i < q.size(); ++i)
          values.push_back(q(i));
        for (int i = 0; i < q.size(); ++i)
          values.push_back(p(i));
        for (int i = 0; i < q.size(); ++i)
          values.push_back(g(i));
      }

      /**
       * Writes the metric
       *
       * @param writer writer callback
       */
      virtual inline void
      write_metric(stan::callbacks::writer& writer) {}

    protected:
      template <typename T>
      static inline void
      fast_vector_copy_(Eigen::Matrix<T, Dynamic, 1>& v_to,
                        const Eigen::Matrix<T, Dynamic, 1>& v_from) {
        int sz = v_from.size();
        v_to.resize(sz);
        if (sz > 0)
          std::memcpy(&v_to(0), &v_from(0), v_from.size() * sizeof(double));
      }

      template <typename T>
      static inline void
      fast_matrix_copy_(Eigen::Matrix<T, Dynamic, Dynamic>& v_to,
                        const Eigen::Matrix<T, Dynamic, Dynamic>& v_from) {
        int nr = v_from.rows();
        int nc = v_from.cols();
        v_to.resize(nr, nc);
        if (nr > 0 && nc > 0)
          std::memcpy(&v_to(0), &v_from(0), v_from.size() * sizeof(double));
      }
    };

  }  // mcmc
}  // stan
#endif
