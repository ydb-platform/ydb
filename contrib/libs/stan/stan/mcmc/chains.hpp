#ifndef STAN_MCMC_CHAINS_HPP
#define STAN_MCMC_CHAINS_HPP

#include <stan/io/stan_csv_reader.hpp>
#include <stan/math/prim/mat.hpp>
#include <stan/analyze/mcmc/compute_effective_sample_size.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/tail_quantile.hpp>
#include <boost/accumulators/statistics/p_square_quantile.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/accumulators/statistics/covariance.hpp>
#include <boost/accumulators/statistics/variates/covariate.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/random/additive_combine.hpp>
#include <algorithm>
#include <cmath>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <sstream>
#include <utility>
#include <vector>
#include <cstdlib>

namespace stan {
  namespace mcmc {
    using Eigen::Dynamic;

    /**
     * An <code>mcmc::chains</code> object stores parameter names and
     * dimensionalities along with samples from multiple chains.
     *
     * <p><b>Synchronization</b>: For arbitrary concurrent use, the
     * read and write methods need to be read/write locked.  Multiple
     * writers can be used concurrently if they write to different
     * chains.  Readers for single chains need only be read/write locked
     * with writers of that chain.  For reading across chains, full
     * read/write locking is required.  Thus methods will be classified
     * as global or single-chain read or write methods.
     *
     * <p><b>Storage Order</b>: Storage is column/last-index major.
     */
    template <class RNG = boost::random::ecuyer1988>
    class chains {
    private:
      Eigen::Matrix<std::string, Dynamic, 1> param_names_;
      Eigen::Matrix<Eigen::MatrixXd, Dynamic, 1> samples_;
      Eigen::VectorXi warmup_;

      static double mean(const Eigen::VectorXd& x) {
        return (x.array() / x.size()).sum();
      }

      static double variance(const Eigen::VectorXd& x) {
        double m = mean(x);
        return ((x.array() - m) / std::sqrt((x.size() - 1.0))).square().sum();
      }

      static double sd(const Eigen::VectorXd& x) {
        return std::sqrt(variance(x));
      }


      static double covariance(const Eigen::VectorXd& x,
                               const Eigen::VectorXd& y,
                               std::ostream* err = 0) {
        if (x.rows() != y.rows() && err)
          *err << "warning: covariance of different length chains";
        using boost::accumulators::accumulator_set;
        using boost::accumulators::stats;
        using boost::accumulators::tag::variance;
        using boost::accumulators::tag::covariance;
        using boost::accumulators::tag::covariate1;

        accumulator_set<double, stats<covariance<double, covariate1> > > acc;

        int M = std::min(x.size(), y.size());
        for (int i = 0; i < M; i++)
          acc(x(i), boost::accumulators::covariate1 = y(i));

        return boost::accumulators::covariance(acc) * M / (M-1);
      }

      static double correlation(const Eigen::VectorXd& x,
                                const Eigen::VectorXd& y,
                                std::ostream* err = 0) {
        if (x.rows() != y.rows() && err)
          *err << "warning: covariance of different length chains";
        using boost::accumulators::accumulator_set;
        using boost::accumulators::stats;
        using boost::accumulators::tag::variance;
        using boost::accumulators::tag::covariance;
        using boost::accumulators::tag::covariate1;

        accumulator_set<double, stats<variance,
                                      covariance<double, covariate1> > > acc_xy;
        accumulator_set<double, stats<variance> > acc_y;

        int M = std::min(x.size(), y.size());
        for (int i = 0; i < M; i++) {
          acc_xy(x(i), boost::accumulators::covariate1 = y(i));
          acc_y(y(i));
        }

        double cov = boost::accumulators::covariance(acc_xy);
        if (cov > -1e-8 && cov < 1e-8)
          return cov;
        return cov / std::sqrt(boost::accumulators::variance(acc_xy)
                               * boost::accumulators::variance(acc_y));
      }

      static double quantile(const Eigen::VectorXd& x, const double prob) {
        using boost::accumulators::accumulator_set;
        using boost::accumulators::left;
        using boost::accumulators::quantile;
        using boost::accumulators::quantile_probability;
        using boost::accumulators::right;
        using boost::accumulators::stats;
        using boost::accumulators::tag::tail;
        using boost::accumulators::tag::tail_quantile;
        double M = x.rows();
        // size_t cache_size = std::min(prob, 1-prob)*M + 2;
        size_t cache_size = M;

        if (prob < 0.5) {
          accumulator_set<double, stats<tail_quantile<left> > >
            acc(tail<left>::cache_size = cache_size);
          for (int i = 0; i < M; i++)
            acc(x(i));
          return quantile(acc, quantile_probability = prob);
        }
        accumulator_set<double, stats<tail_quantile<right> > >
          acc(tail<right>::cache_size = cache_size);
        for (int i = 0; i < M; i++)
          acc(x(i));
        return quantile(acc, quantile_probability = prob);
      }

      static Eigen::VectorXd
      quantiles(const Eigen::VectorXd& x, const Eigen::VectorXd& probs) {
        using boost::accumulators::accumulator_set;
        using boost::accumulators::left;
        using boost::accumulators::quantile_probability;
        using boost::accumulators::quantile;
        using boost::accumulators::right;
        using boost::accumulators::stats;
        using boost::accumulators::tag::tail;
        using boost::accumulators::tag::tail_quantile;
        double M = x.rows();

        // size_t cache_size = M/2 + 2;
        size_t cache_size = M;  // 2 + 2;

        accumulator_set<double, stats<tail_quantile<left> > >
          acc_left(tail<left>::cache_size = cache_size);
        accumulator_set<double, stats<tail_quantile<right> > >
          acc_right(tail<right>::cache_size = cache_size);

        for (int i = 0; i < M; i++) {
          acc_left(x(i));
          acc_right(x(i));
        }

        Eigen::VectorXd q(probs.size());
        for (int i = 0; i < probs.size(); i++) {
          if (probs(i) < 0.5)
            q(i) = quantile(acc_left,
                            quantile_probability = probs(i));
          else
            q(i) = quantile(acc_right,
                            quantile_probability = probs(i));
        }
        return q;
      }

      static Eigen::VectorXd autocorrelation(const Eigen::VectorXd& x) {
        using std::vector;
        using stan::math::index_type;
        typedef typename index_type<vector<double> >::type idx_t;

        std::vector<double> ac;
        std::vector<double> sample(x.size());
        for (int i = 0; i < x.size(); i++)
          sample[i] = x(i);
        stan::math::autocorrelation(sample, ac);

        Eigen::VectorXd ac2(ac.size());
        for (idx_t i = 0; i < ac.size(); i++)
          ac2(i) = ac[i];
        return ac2;
      }

      static Eigen::VectorXd autocovariance(const Eigen::VectorXd& x) {
        using std::vector;
        using stan::math::index_type;
        typedef typename index_type<vector<double> >::type idx_t;

        std::vector<double> ac;
        std::vector<double> sample(x.size());
        for (int i = 0; i < x.size(); i++)
          sample[i] = x(i);
        stan::math::autocovariance(sample, ac);

        Eigen::VectorXd ac2(ac.size());
        for (idx_t i = 0; i < ac.size(); i++)
          ac2(i) = ac[i];
        return ac2;
      }

      /**
       * Return the split potential scale reduction (split R hat)
       * for the specified parameter.
       *
       * Current implementation takes the minimum number of samples
       * across chains as the number of samples per chain.
       *
       * @param VectorXd
       * @param Dynamic
       * @param samples
       *
       * @return
       */
      double
      split_potential_scale_reduction(
                                      const Eigen::Matrix<Eigen::VectorXd,
                                      Dynamic, 1> &samples) const {
        int chains = samples.size();
        int n_samples = samples(0).size();
        for (int chain = 1; chain < chains; chain++) {
          n_samples = std::min(n_samples,
                               static_cast<int>(samples(chain).size()));
        }
        if (n_samples % 2 == 1)
          n_samples--;
        int n = n_samples / 2;

        Eigen::VectorXd split_chain_mean(2*chains);
        Eigen::VectorXd split_chain_var(2*chains);

        for (int chain = 0; chain < chains; chain++) {
          split_chain_mean(2*chain) = mean(samples(chain).topRows(n));
          split_chain_mean(2*chain+1) = mean(samples(chain).bottomRows(n));

          split_chain_var(2*chain) = variance(samples(chain).topRows(n));
          split_chain_var(2*chain+1) = variance(samples(chain).bottomRows(n));
        }

        double var_between = n * variance(split_chain_mean);
        double var_within = mean(split_chain_var);

        // rewrote [(n-1)*W/n + B/n]/W as (n-1+ B/W)/n
        return sqrt((var_between/var_within + n-1)/n);
      }

    public:
      explicit chains(const Eigen::Matrix<std::string, Dynamic, 1>& param_names)
        : param_names_(param_names) { }

      explicit chains(const std::vector<std::string>& param_names)
        : param_names_(param_names.size()) {
        for (size_t i = 0; i < param_names.size(); i++)
          param_names_(i) = param_names[i];
      }

      explicit chains(const stan::io::stan_csv& stan_csv)
        : param_names_(stan_csv.header) {
        if (stan_csv.samples.rows() > 0)
          add(stan_csv);
      }

      inline int num_chains() const {
        return samples_.size();
      }

      inline int num_params() const {
        return param_names_.size();
      }

      const Eigen::Matrix<std::string, Dynamic, 1>& param_names() const {
        return param_names_;
      }

      const std::string& param_name(int j) const {
        return param_names_(j);
      }

      int index(const std::string& name) const {
        int index = -1;
        for (int i = 0; i < param_names_.size(); i++)
          if (param_names_(i) == name)
            return i;
        return index;
      }

      void set_warmup(const int chain, const int warmup) {
        warmup_(chain) = warmup;
      }

      void set_warmup(const int warmup) {
        warmup_.setConstant(warmup);
      }

      const Eigen::VectorXi& warmup() const {
        return warmup_;
      }

      int warmup(const int chain) const {
        return warmup_(chain);
      }

      int num_samples(const int chain) const {
        return samples_(chain).rows();
      }

      int num_samples() const {
        int n = 0;
        for (int chain = 0; chain < num_chains(); chain++)
          n += num_samples(chain);
        return n;
      }

      int num_kept_samples(const int chain) const {
        return num_samples(chain) - warmup(chain);
      }

      int num_kept_samples() const {
        int n = 0;
        for (int chain = 0; chain < num_chains(); chain++)
          n += num_kept_samples(chain);
        return n;
      }

      void add(const int chain,
               const Eigen::MatrixXd& sample) {
        if (sample.cols() != num_params())
          throw std::invalid_argument("add(chain, sample): number of columns"
                                      " in sample does not match chains");
        if (num_chains() == 0 || chain >= num_chains()) {
          int n = num_chains();

          // Need this block for Windows. conservativeResize
          // does not keep the references.
          Eigen::Matrix<Eigen::MatrixXd, Dynamic, 1>
            samples_copy(num_chains());
          Eigen::VectorXi warmup_copy(num_chains());
          for (int i = 0; i < n; i++) {
            samples_copy(i) = samples_(i);
            warmup_copy(i) = warmup_(i);
          }

          samples_.resize(chain+1);
          warmup_.resize(chain+1);
          for (int i = 0; i < n; i++) {
            samples_(i) = samples_copy(i);
            warmup_(i) = warmup_copy(i);
          }
          for (int i = n; i < chain+1; i++) {
            samples_(i) = Eigen::MatrixXd(0, num_params());
            warmup_(i) = 0;
          }
        }
        int row = samples_(chain).rows();
        Eigen::MatrixXd new_samples(row+sample.rows(), num_params());
        new_samples << samples_(chain), sample;
        samples_(chain) = new_samples;
      }

      void add(const Eigen::MatrixXd& sample) {
        if (sample.rows() == 0)
          return;
        if (sample.cols() != num_params())
          throw std::invalid_argument("add(sample): number of columns in"
                                      " sample does not match chains");
        add(num_chains(), sample);
      }

      /**
       * Convert a vector of vector<double> to Eigen::MatrixXd
       *
       * This method is added for the benefit of software wrapping
       * Stan (e.g., PyStan) so that it need not additionally wrap Eigen.
       *
       */
      void add(const std::vector<std::vector<double> >& sample) {
        int n_row = sample.size();
        if (n_row == 0)
          return;
        int n_col = sample[0].size();
        Eigen::MatrixXd sample_copy(n_row, n_col);
        for (int i = 0; i < n_row; i++) {
          sample_copy.row(i)
            = Eigen::VectorXd::Map(&sample[i][0], sample[0].size());
        }
        add(sample_copy);
      }

      void add(const stan::io::stan_csv& stan_csv) {
        if (stan_csv.header.size() != num_params())
          throw std::invalid_argument("add(stan_csv): number of columns in"
                                      " sample does not match chains");
        if (!param_names_.cwiseEqual(stan_csv.header).all()) {
          throw std::invalid_argument("add(stan_csv): header does not match"
                                      " chain's header");
        }
        add(stan_csv.samples);
        if (stan_csv.metadata.save_warmup)
          set_warmup(num_chains()-1, stan_csv.metadata.num_warmup);
      }

      Eigen::VectorXd samples(const int chain, const int index) const {
        return samples_(chain).col(index).bottomRows(num_kept_samples(chain));
      }

      Eigen::VectorXd samples(const int index) const {
        Eigen::VectorXd s(num_kept_samples());
        int start = 0;
        for (int chain = 0; chain < num_chains(); chain++) {
          int n = num_kept_samples(chain);
          s.middleRows(start, n) = samples_(chain).col(index).bottomRows(n);
          start += n;
        }
        return s;
      }

      Eigen::VectorXd samples(const int chain, const std::string& name) const {
        return samples(chain, index(name));
      }

      Eigen::VectorXd samples(const std::string& name) const {
        return samples(index(name));
      }

      double mean(const int chain, const int index) const {
        return mean(samples(chain, index));
      }

      double mean(const int index) const {
        return mean(samples(index));
      }

      double mean(const int chain, const std::string& name) const {
        return mean(chain, index(name));
      }

      double mean(const std::string& name) const {
        return mean(index(name));
      }

      double sd(const int chain, const int index) const {
        return sd(samples(chain, index));
      }

      double sd(const int index) const {
        return sd(samples(index));
      }

      double sd(const int chain, const std::string& name) const {
        return sd(chain, index(name));
      }

      double sd(const std::string& name) const {
        return sd(index(name));
      }

      double variance(const int chain, const int index) const {
        return variance(samples(chain, index));
      }

      double variance(const int index) const {
        return variance(samples(index));
      }

      double variance(const int chain, const std::string& name) const {
        return variance(chain, index(name));
      }

      double variance(const std::string& name) const {
        return variance(index(name));
      }

      double
      covariance(const int chain, const int index1, const int index2) const {
        return covariance(samples(chain, index1), samples(chain, index2));
      }

      double covariance(const int index1, const int index2) const {
        return covariance(samples(index1), samples(index2));
      }

      double covariance(const int chain, const std::string& name1,
                        const std::string& name2) const {
        return covariance(chain, index(name1), index(name2));
      }

      double
      covariance(const std::string& name1, const std::string& name2) const {
        return covariance(index(name1), index(name2));
      }

      double
      correlation(const int chain, const int index1, const int index2) const {
        return correlation(samples(chain, index1), samples(chain, index2));
      }

      double correlation(const int index1, const int index2) const {
        return correlation(samples(index1), samples(index2));
      }

      double correlation(const int chain, const std::string& name1,
                         const std::string& name2) const {
        return correlation(chain, index(name1), index(name2));
      }

      double
      correlation(const std::string& name1, const std::string& name2) const {
        return correlation(index(name1), index(name2));
      }

      double
      quantile(const int chain, const int index, const double prob) const {
        return quantile(samples(chain, index), prob);
      }

      double quantile(const int index, const double prob) const {
        return quantile(samples(index), prob);
      }

      double quantile(int chain, const std::string& name, double prob) const {
        return quantile(chain, index(name), prob);
      }

      double quantile(const std::string& name, const double prob) const {
        return quantile(index(name), prob);
      }

      Eigen::VectorXd
      quantiles(int chain, int index, const Eigen::VectorXd& probs) const {
        return quantiles(samples(chain, index), probs);
      }

      Eigen::VectorXd quantiles(int index, const Eigen::VectorXd& probs) const {
        return quantiles(samples(index), probs);
      }

      Eigen::VectorXd
      quantiles(int chain, const std::string& name,
                const Eigen::VectorXd& probs) const {
        return quantiles(chain, index(name), probs);
      }

      Eigen::VectorXd
      quantiles(const std::string& name, const Eigen::VectorXd& probs) const {
        return quantiles(index(name), probs);
      }

      Eigen::Vector2d central_interval(int chain, int index,
                                       double prob) const {
        double low_prob = (1-prob)/2;
        double high_prob = 1-low_prob;

        Eigen::Vector2d interval;
        interval
          << quantile(chain, index, low_prob),
          quantile(chain, index, high_prob);
        return interval;
      }

      Eigen::Vector2d central_interval(int index, double prob) const {
        double low_prob = (1-prob)/2;
        double high_prob = 1-low_prob;

        Eigen::Vector2d interval;
        interval << quantile(index, low_prob), quantile(index, high_prob);
        return interval;
      }

      Eigen::Vector2d
      central_interval(int chain, const std::string& name,
                       double prob) const {
        return central_interval(chain, index(name), prob);
      }

      Eigen::Vector2d central_interval(const std::string& name,
                                       double prob) const {
        return central_interval(index(name), prob);
      }

      Eigen::VectorXd autocorrelation(const int chain, const int index) const {
        return autocorrelation(samples(chain, index));
      }

      Eigen::VectorXd autocorrelation(int chain,
                                      const std::string& name) const {
        return autocorrelation(chain, index(name));
      }

      Eigen::VectorXd autocovariance(const int chain, const int index) const {
        return autocovariance(samples(chain, index));
      }

      Eigen::VectorXd autocovariance(int chain, const std::string& name) const {
        return autocovariance(chain, index(name));
      }

      // FIXME: reimplement using autocorrelation.
      double effective_sample_size(const int index) const {
        int n_chains = num_chains();
        std::vector<const double*> draws(n_chains);
        std::vector<size_t> sizes(n_chains);
        int n_kept_samples = 0;
        for (int chain = 0; chain < n_chains; ++chain) {
          n_kept_samples = num_kept_samples(chain);
          draws[chain]
            = samples_(chain).col(index).bottomRows(n_kept_samples).data();
          sizes[chain] = n_kept_samples;
        }
        return analyze::compute_effective_sample_size(draws, sizes);
      }

      double effective_sample_size(const std::string& name) const {
        return effective_sample_size(index(name));
      }

      double split_potential_scale_reduction(const int index) const {
        Eigen::Matrix<Eigen::VectorXd, Dynamic, 1>
          samples(num_chains());
        for (int chain = 0; chain < num_chains(); chain++) {
          samples(chain) = this->samples(chain, index);
        }
        return split_potential_scale_reduction(samples);
      }

      double split_potential_scale_reduction(const std::string& name) const {
        return split_potential_scale_reduction(index(name));
      }
    };

  }
}

#endif
