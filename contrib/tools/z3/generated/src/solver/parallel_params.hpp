// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct parallel_params {
  params_ref const & p;
  params_ref g;
  parallel_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("parallel")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("enable", CPK_BOOL, "enable parallel solver by default on selected tactics (for QF_BV)", "false","parallel");
    d.insert("threads.max", CPK_UINT, "caps maximal number of threads below the number of processors", "10000","parallel");
    d.insert("conquer.batch_size", CPK_UINT, "number of cubes to batch together for fast conquer", "100","parallel");
    d.insert("conquer.restart.max", CPK_UINT, "maximal number of restarts during conquer phase", "5","parallel");
    d.insert("conquer.delay", CPK_UINT, "delay of cubes until applying conquer", "10","parallel");
    d.insert("conquer.backtrack_frequency", CPK_UINT, "frequency to apply core minimization during conquer", "10","parallel");
    d.insert("simplify.exp", CPK_DOUBLE, "restart and inprocess max is multiplied by simplify.exp ^ depth", "1","parallel");
    d.insert("simplify.max_conflicts", CPK_UINT, "maximal number of conflicts during simplification phase", "4294967295","parallel");
    d.insert("simplify.restart.max", CPK_UINT, "maximal number of restarts during simplification phase", "5000","parallel");
    d.insert("simplify.inprocess.max", CPK_UINT, "maximal number of inprocessing steps during simplification", "2","parallel");
  }
  /*
     REG_MODULE_PARAMS('parallel', 'parallel_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('parallel', 'parameters for parallel solver')
  */
  bool enable() const { return p.get_bool("enable", g, false); }
  unsigned threads_max() const { return p.get_uint("threads.max", g, 10000u); }
  unsigned conquer_batch_size() const { return p.get_uint("conquer.batch_size", g, 100u); }
  unsigned conquer_restart_max() const { return p.get_uint("conquer.restart.max", g, 5u); }
  unsigned conquer_delay() const { return p.get_uint("conquer.delay", g, 10u); }
  unsigned conquer_backtrack_frequency() const { return p.get_uint("conquer.backtrack_frequency", g, 10u); }
  double simplify_exp() const { return p.get_double("simplify.exp", g, 1); }
  unsigned simplify_max_conflicts() const { return p.get_uint("simplify.max_conflicts", g, 4294967295u); }
  unsigned simplify_restart_max() const { return p.get_uint("simplify.restart.max", g, 5000u); }
  unsigned simplify_inprocess_max() const { return p.get_uint("simplify.inprocess.max", g, 2u); }
};
