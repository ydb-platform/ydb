#ifndef STAN_LANG_AST_NODE_MAP_RECT_DEF_HPP
#define STAN_LANG_AST_NODE_MAP_RECT_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
namespace lang {

int map_rect::CALL_ID_ = 0;

stan::lang::map_rect::map_rect() : call_id_(-1) { }

stan::lang::map_rect::map_rect(const std::string& fun_name,
               const expression& shared_params,
               const expression& job_params, const expression& job_data_r,
               const expression& job_data_i)
    : call_id_(-1), fun_name_(fun_name), shared_params_(shared_params),
      job_params_(job_params), job_data_r_(job_data_r),
      job_data_i_(job_data_i) {
}

// can't just construct with nullary and assign because of call ID
stan::lang::map_rect::map_rect(const map_rect& mr)
    : call_id_(mr.call_id_), fun_name_(mr.fun_name_),
      shared_params_(mr.shared_params_), job_params_(mr.job_params_),
      job_data_r_(mr.job_data_r_), job_data_i_(mr.job_data_i_) { }

map_rect& stan::lang::map_rect::operator=(const map_rect& mr) {
  call_id_ = mr.call_id_;
  fun_name_ = mr.fun_name_;
  shared_params_ = mr.shared_params_;
  job_params_ = mr.job_params_;
  job_data_r_ = mr.job_data_r_;
  job_data_i_ = mr.job_data_i_;
  return *this;
}

void stan::lang::map_rect::register_id() {
  call_id_ = ++CALL_ID_;
  registered_calls().emplace_back(call_id_, fun_name_);
}

}
}
#endif
