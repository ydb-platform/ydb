#ifndef STAN_SERVICES_SAMPLE_STANDALONE_GQS_HPP
#define STAN_SERVICES_SAMPLE_STANDALONE_GQS_HPP

#include <stan/callbacks/interrupt.hpp>
#include <stan/callbacks/logger.hpp>
#include <stan/callbacks/writer.hpp>
#include <stan/io/array_var_context.hpp>
#include <stan/services/error_codes.hpp>
#include <stan/services/util/create_rng.hpp>
#include <stan/services/util/gq_writer.hpp>
#include <Eigen/Dense>
#include <string>
#include <vector>

namespace stan {
namespace services {

/**
 * Find the names, dimensions of the model parameters.
 * Assembles vectors of name, dimensions for the variables
 * declared in the parameters block.
 *
 * @tparam Model type of model
 * @param[in] model model to query
 * @param[in, out] vector of parameter names
 * @param[in, out] vector of variable dimensions
 */
template <class Model>
void get_model_parameters(const Model &model,
                          std::vector<std::string> &param_names,
                          std::vector<std::vector<size_t>> &param_dimss) {
  std::vector<std::string> constrained_names;
  model.constrained_param_names(constrained_names, false, false);
  size_t num_params = constrained_names.size();
  std::vector<std::vector<size_t>> dimss;
  model.get_dims(dimss);
  size_t total = 0;
  for (size_t i = 0; i < dimss.size(); ++i) {
    param_dimss.emplace_back(dimss[i]);
    int cur_param = 1;
    for (int j = 0; j < dimss[i].size(); ++j)
      cur_param *= dimss[i][j];
    if (cur_param == 1) {
      param_names.emplace_back(constrained_names[total]);
    } else {
      int idx = constrained_names[total].find('.');
      param_names.emplace_back(constrained_names[total].substr(0, idx));
    }
    total += cur_param;
    if (total == num_params)
      break;
  }
}

/**
 * Given a set of draws from a fitted model, generate corresponding
 * quantities of interes which are written to callback writer.
 * Matrix of draws consists of one row per draw, one column per parameter.
 * Draws are processed one row at a time.
 * Return code indicates success or type of error.
 *
 * @tparam Model model class
 * @param[in] model instantiated model
 * @param[in] draws sequence of draws of constrained parameters
 * @param[in] seed seed to use for randomization
 * @param[in, out] interrupt called every iteration
 * @param[in, out] logger logger to which to write warning and error messages
 * @param[in, out] sample_writer writer to which draws are written
 * @return error code
 */
template <class Model>
int standalone_generate(const Model &model,
                        const Eigen::MatrixXd& draws,
                        unsigned int seed, callbacks::interrupt &interrupt,
                        callbacks::logger &logger,
                        callbacks::writer &sample_writer) {
  if (draws.size() == 0) {
    logger.error("Empty set of draws from fitted model.");
    return error_codes::DATAERR;
  }

  std::vector<std::string> p_names;
  model.constrained_param_names(p_names, false, false);
  std::vector<std::string> gq_names;
  model.constrained_param_names(gq_names, false, true);
  if (!(p_names.size() < gq_names.size())) {
    logger.error("Model doesn't generate any quantities of interest.");
    return error_codes::CONFIG;
  }

  std::stringstream msg;
  if (p_names.size() != draws.cols()) {
    msg << "Wrong number of parameter values in draws from fitted model.  ";
    msg << "Expecting " << p_names.size() << " columns, ";
    msg << "found " << draws.cols() << " columns.";
    std::string msgstr = msg.str();
    logger.error(msgstr);
    return error_codes::DATAERR;
  }
  util::gq_writer writer(sample_writer, logger, p_names.size());
  writer.write_gq_names(model);

  boost::ecuyer1988 rng = util::create_rng(seed, 1);
  std::vector<std::string> param_names;
  std::vector<std::vector<size_t>> param_dimss;
  get_model_parameters(model, param_names, param_dimss);

  std::vector<int> dummy_params_i;
  std::vector<double> unconstrained_params_r;
  for (size_t i = 0; i < draws.rows(); ++i) {
    dummy_params_i.clear();
    unconstrained_params_r.clear();
    try {
      stan::io::array_var_context context(param_names, draws.row(i),
                                          param_dimss);
      model.transform_inits(context, dummy_params_i, unconstrained_params_r,
                            &msg);
    } catch (const std::exception& e) {
      if (msg.str().length() > 0)
        logger.error(msg);
      logger.error(e.what());
      return error_codes::DATAERR;
    }
    interrupt();   // call out to interrupt and fail
    writer.write_gq_values(model, rng, unconstrained_params_r);
  }
  return error_codes::OK;
}

}   // namespace services
}   // namespace stan
#endif
