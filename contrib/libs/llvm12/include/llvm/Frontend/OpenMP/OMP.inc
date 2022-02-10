#ifdef GEN_FLANG_DIRECTIVE_CLAUSE_SETS
#undef GEN_FLANG_DIRECTIVE_CLAUSE_SETS

namespace llvm {
namespace omp {

  // Sets for allocate

  static OmpClauseSet allowedClauses_OMPD_allocate {
    llvm::omp::Clause::OMPC_allocator,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_allocate {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_allocate {
  };

  static OmpClauseSet requiredClauses_OMPD_allocate {
  };

  // Sets for assumes

  static OmpClauseSet allowedClauses_OMPD_assumes {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_assumes {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_assumes {
  };

  static OmpClauseSet requiredClauses_OMPD_assumes {
  };

  // Sets for atomic

  static OmpClauseSet allowedClauses_OMPD_atomic {
    llvm::omp::Clause::OMPC_read,
    llvm::omp::Clause::OMPC_write,
    llvm::omp::Clause::OMPC_update,
    llvm::omp::Clause::OMPC_capture,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_atomic {
    llvm::omp::Clause::OMPC_seq_cst,
    llvm::omp::Clause::OMPC_acq_rel,
    llvm::omp::Clause::OMPC_acquire,
    llvm::omp::Clause::OMPC_release,
    llvm::omp::Clause::OMPC_relaxed,
    llvm::omp::Clause::OMPC_hint,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_atomic {
  };

  static OmpClauseSet requiredClauses_OMPD_atomic {
  };

  // Sets for barrier

  static OmpClauseSet allowedClauses_OMPD_barrier {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_barrier {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_barrier {
  };

  static OmpClauseSet requiredClauses_OMPD_barrier {
  };

  // Sets for begin assumes

  static OmpClauseSet allowedClauses_OMPD_begin_assumes {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_begin_assumes {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_begin_assumes {
  };

  static OmpClauseSet requiredClauses_OMPD_begin_assumes {
  };

  // Sets for begin declare variant

  static OmpClauseSet allowedClauses_OMPD_begin_declare_variant {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_begin_declare_variant {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_begin_declare_variant {
  };

  static OmpClauseSet requiredClauses_OMPD_begin_declare_variant {
  };

  // Sets for cancel

  static OmpClauseSet allowedClauses_OMPD_cancel {
    llvm::omp::Clause::OMPC_if,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_cancel {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_cancel {
  };

  static OmpClauseSet requiredClauses_OMPD_cancel {
  };

  // Sets for cancellation point

  static OmpClauseSet allowedClauses_OMPD_cancellation_point {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_cancellation_point {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_cancellation_point {
  };

  static OmpClauseSet requiredClauses_OMPD_cancellation_point {
  };

  // Sets for critical

  static OmpClauseSet allowedClauses_OMPD_critical {
    llvm::omp::Clause::OMPC_hint,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_critical {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_critical {
  };

  static OmpClauseSet requiredClauses_OMPD_critical {
  };

  // Sets for declare mapper

  static OmpClauseSet allowedClauses_OMPD_declare_mapper {
    llvm::omp::Clause::OMPC_map,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_declare_mapper {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_declare_mapper {
  };

  static OmpClauseSet requiredClauses_OMPD_declare_mapper {
  };

  // Sets for declare reduction

  static OmpClauseSet allowedClauses_OMPD_declare_reduction {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_declare_reduction {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_declare_reduction {
  };

  static OmpClauseSet requiredClauses_OMPD_declare_reduction {
  };

  // Sets for declare simd

  static OmpClauseSet allowedClauses_OMPD_declare_simd {
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_uniform,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_declare_simd {
    llvm::omp::Clause::OMPC_simdlen,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_declare_simd {
    llvm::omp::Clause::OMPC_inbranch,
    llvm::omp::Clause::OMPC_notinbranch,
  };

  static OmpClauseSet requiredClauses_OMPD_declare_simd {
  };

  // Sets for declare target

  static OmpClauseSet allowedClauses_OMPD_declare_target {
    llvm::omp::Clause::OMPC_to,
    llvm::omp::Clause::OMPC_link,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_declare_target {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_declare_target {
  };

  static OmpClauseSet requiredClauses_OMPD_declare_target {
  };

  // Sets for declare variant

  static OmpClauseSet allowedClauses_OMPD_declare_variant {
    llvm::omp::Clause::OMPC_match,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_declare_variant {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_declare_variant {
  };

  static OmpClauseSet requiredClauses_OMPD_declare_variant {
  };

  // Sets for depobj

  static OmpClauseSet allowedClauses_OMPD_depobj {
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_destroy,
    llvm::omp::Clause::OMPC_update,
    llvm::omp::Clause::OMPC_depobj,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_depobj {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_depobj {
  };

  static OmpClauseSet requiredClauses_OMPD_depobj {
  };

  // Sets for distribute

  static OmpClauseSet allowedClauses_OMPD_distribute {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_allocate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_distribute {
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_distribute {
  };

  static OmpClauseSet requiredClauses_OMPD_distribute {
  };

  // Sets for distribute parallel do

  static OmpClauseSet allowedClauses_OMPD_distribute_parallel_do {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_linear,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_distribute_parallel_do {
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_ordered,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_distribute_parallel_do {
  };

  static OmpClauseSet requiredClauses_OMPD_distribute_parallel_do {
  };

  // Sets for distribute parallel do simd

  static OmpClauseSet allowedClauses_OMPD_distribute_parallel_do_simd {
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_distribute_parallel_do_simd {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_distribute_parallel_do_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_distribute_parallel_do_simd {
  };

  // Sets for distribute parallel for

  static OmpClauseSet allowedClauses_OMPD_distribute_parallel_for {
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_distribute_parallel_for {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_distribute_parallel_for {
  };

  static OmpClauseSet requiredClauses_OMPD_distribute_parallel_for {
  };

  // Sets for distribute parallel for simd

  static OmpClauseSet allowedClauses_OMPD_distribute_parallel_for_simd {
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_distribute_parallel_for_simd {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_distribute_parallel_for_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_distribute_parallel_for_simd {
  };

  // Sets for distribute simd

  static OmpClauseSet allowedClauses_OMPD_distribute_simd {
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_reduction,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_distribute_simd {
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_distribute_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_distribute_simd {
  };

  // Sets for do

  static OmpClauseSet allowedClauses_OMPD_do {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_reduction,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_do {
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_nowait,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_do {
  };

  static OmpClauseSet requiredClauses_OMPD_do {
  };

  // Sets for do simd

  static OmpClauseSet allowedClauses_OMPD_do_simd {
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_reduction,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_do_simd {
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_nowait,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_do_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_do_simd {
  };

  // Sets for end assumes

  static OmpClauseSet allowedClauses_OMPD_end_assumes {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_end_assumes {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_end_assumes {
  };

  static OmpClauseSet requiredClauses_OMPD_end_assumes {
  };

  // Sets for end declare target

  static OmpClauseSet allowedClauses_OMPD_end_declare_target {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_end_declare_target {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_end_declare_target {
  };

  static OmpClauseSet requiredClauses_OMPD_end_declare_target {
  };

  // Sets for end declare variant

  static OmpClauseSet allowedClauses_OMPD_end_declare_variant {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_end_declare_variant {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_end_declare_variant {
  };

  static OmpClauseSet requiredClauses_OMPD_end_declare_variant {
  };

  // Sets for end do

  static OmpClauseSet allowedClauses_OMPD_end_do {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_end_do {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_end_do {
  };

  static OmpClauseSet requiredClauses_OMPD_end_do {
  };

  // Sets for end do simd

  static OmpClauseSet allowedClauses_OMPD_end_do_simd {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_end_do_simd {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_end_do_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_end_do_simd {
  };

  // Sets for end sections

  static OmpClauseSet allowedClauses_OMPD_end_sections {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_end_sections {
    llvm::omp::Clause::OMPC_nowait,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_end_sections {
  };

  static OmpClauseSet requiredClauses_OMPD_end_sections {
  };

  // Sets for end single

  static OmpClauseSet allowedClauses_OMPD_end_single {
    llvm::omp::Clause::OMPC_copyprivate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_end_single {
    llvm::omp::Clause::OMPC_nowait,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_end_single {
  };

  static OmpClauseSet requiredClauses_OMPD_end_single {
  };

  // Sets for end workshare

  static OmpClauseSet allowedClauses_OMPD_end_workshare {
    llvm::omp::Clause::OMPC_nowait,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_end_workshare {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_end_workshare {
  };

  static OmpClauseSet requiredClauses_OMPD_end_workshare {
  };

  // Sets for flush

  static OmpClauseSet allowedClauses_OMPD_flush {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_flush {
    llvm::omp::Clause::OMPC_acq_rel,
    llvm::omp::Clause::OMPC_acquire,
    llvm::omp::Clause::OMPC_release,
    llvm::omp::Clause::OMPC_flush,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_flush {
  };

  static OmpClauseSet requiredClauses_OMPD_flush {
  };

  // Sets for for

  static OmpClauseSet allowedClauses_OMPD_for {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_for {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_for {
  };

  static OmpClauseSet requiredClauses_OMPD_for {
  };

  // Sets for for simd

  static OmpClauseSet allowedClauses_OMPD_for_simd {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_for_simd {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_for_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_for_simd {
  };

  // Sets for master

  static OmpClauseSet allowedClauses_OMPD_master {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_master {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_master {
  };

  static OmpClauseSet requiredClauses_OMPD_master {
  };

  // Sets for master taskloop

  static OmpClauseSet allowedClauses_OMPD_master_taskloop {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_final,
    llvm::omp::Clause::OMPC_untied,
    llvm::omp::Clause::OMPC_mergeable,
    llvm::omp::Clause::OMPC_priority,
    llvm::omp::Clause::OMPC_grainsize,
    llvm::omp::Clause::OMPC_nogroup,
    llvm::omp::Clause::OMPC_num_tasks,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_in_reduction,
    llvm::omp::Clause::OMPC_allocate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_master_taskloop {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_master_taskloop {
  };

  static OmpClauseSet requiredClauses_OMPD_master_taskloop {
  };

  // Sets for master taskloop simd

  static OmpClauseSet allowedClauses_OMPD_master_taskloop_simd {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_final,
    llvm::omp::Clause::OMPC_untied,
    llvm::omp::Clause::OMPC_mergeable,
    llvm::omp::Clause::OMPC_priority,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_grainsize,
    llvm::omp::Clause::OMPC_nogroup,
    llvm::omp::Clause::OMPC_num_tasks,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_in_reduction,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_master_taskloop_simd {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_master_taskloop_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_master_taskloop_simd {
  };

  // Sets for ordered

  static OmpClauseSet allowedClauses_OMPD_ordered {
    llvm::omp::Clause::OMPC_threads,
    llvm::omp::Clause::OMPC_simd,
    llvm::omp::Clause::OMPC_depend,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_ordered {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_ordered {
  };

  static OmpClauseSet requiredClauses_OMPD_ordered {
  };

  // Sets for parallel

  static OmpClauseSet allowedClauses_OMPD_parallel {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_allocate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_parallel {
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_parallel {
  };

  static OmpClauseSet requiredClauses_OMPD_parallel {
  };

  // Sets for parallel do

  static OmpClauseSet allowedClauses_OMPD_parallel_do {
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_linear,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_parallel_do {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_collapse,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_parallel_do {
  };

  static OmpClauseSet requiredClauses_OMPD_parallel_do {
  };

  // Sets for parallel do simd

  static OmpClauseSet allowedClauses_OMPD_parallel_do_simd {
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_parallel_do_simd {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_parallel_do_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_parallel_do_simd {
  };

  // Sets for parallel for

  static OmpClauseSet allowedClauses_OMPD_parallel_for {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_parallel_for {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_parallel_for {
  };

  static OmpClauseSet requiredClauses_OMPD_parallel_for {
  };

  // Sets for parallel for simd

  static OmpClauseSet allowedClauses_OMPD_parallel_for_simd {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_parallel_for_simd {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_parallel_for_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_parallel_for_simd {
  };

  // Sets for parallel master

  static OmpClauseSet allowedClauses_OMPD_parallel_master {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_allocate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_parallel_master {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_parallel_master {
  };

  static OmpClauseSet requiredClauses_OMPD_parallel_master {
  };

  // Sets for parallel master taskloop

  static OmpClauseSet allowedClauses_OMPD_parallel_master_taskloop {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_final,
    llvm::omp::Clause::OMPC_untied,
    llvm::omp::Clause::OMPC_mergeable,
    llvm::omp::Clause::OMPC_priority,
    llvm::omp::Clause::OMPC_grainsize,
    llvm::omp::Clause::OMPC_nogroup,
    llvm::omp::Clause::OMPC_num_tasks,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_copyin,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_parallel_master_taskloop {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_parallel_master_taskloop {
  };

  static OmpClauseSet requiredClauses_OMPD_parallel_master_taskloop {
  };

  // Sets for parallel master taskloop simd

  static OmpClauseSet allowedClauses_OMPD_parallel_master_taskloop_simd {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_final,
    llvm::omp::Clause::OMPC_untied,
    llvm::omp::Clause::OMPC_mergeable,
    llvm::omp::Clause::OMPC_priority,
    llvm::omp::Clause::OMPC_grainsize,
    llvm::omp::Clause::OMPC_nogroup,
    llvm::omp::Clause::OMPC_num_tasks,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_parallel_master_taskloop_simd {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_parallel_master_taskloop_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_parallel_master_taskloop_simd {
  };

  // Sets for parallel sections

  static OmpClauseSet allowedClauses_OMPD_parallel_sections {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_allocate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_parallel_sections {
    llvm::omp::Clause::OMPC_num_threads,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_parallel_sections {
  };

  static OmpClauseSet requiredClauses_OMPD_parallel_sections {
  };

  // Sets for parallel workshare

  static OmpClauseSet allowedClauses_OMPD_parallel_workshare {
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_shared,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_parallel_workshare {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_parallel_workshare {
  };

  static OmpClauseSet requiredClauses_OMPD_parallel_workshare {
  };

  // Sets for requires

  static OmpClauseSet allowedClauses_OMPD_requires {
    llvm::omp::Clause::OMPC_unified_address,
    llvm::omp::Clause::OMPC_unified_shared_memory,
    llvm::omp::Clause::OMPC_reverse_offload,
    llvm::omp::Clause::OMPC_dynamic_allocators,
    llvm::omp::Clause::OMPC_atomic_default_mem_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_requires {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_requires {
  };

  static OmpClauseSet requiredClauses_OMPD_requires {
  };

  // Sets for scan

  static OmpClauseSet allowedClauses_OMPD_scan {
    llvm::omp::Clause::OMPC_inclusive,
    llvm::omp::Clause::OMPC_exclusive,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_scan {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_scan {
  };

  static OmpClauseSet requiredClauses_OMPD_scan {
  };

  // Sets for section

  static OmpClauseSet allowedClauses_OMPD_section {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_section {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_section {
  };

  static OmpClauseSet requiredClauses_OMPD_section {
  };

  // Sets for sections

  static OmpClauseSet allowedClauses_OMPD_sections {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_allocate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_sections {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_sections {
  };

  static OmpClauseSet requiredClauses_OMPD_sections {
  };

  // Sets for simd

  static OmpClauseSet allowedClauses_OMPD_simd {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_simd {
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_if,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_simd {
  };

  // Sets for single

  static OmpClauseSet allowedClauses_OMPD_single {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_copyprivate,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_allocate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_single {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_single {
  };

  static OmpClauseSet requiredClauses_OMPD_single {
  };

  // Sets for target

  static OmpClauseSet allowedClauses_OMPD_target {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_uses_allocators,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target {
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_nowait,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target {
  };

  static OmpClauseSet requiredClauses_OMPD_target {
  };

  // Sets for target data

  static OmpClauseSet allowedClauses_OMPD_target_data {
    llvm::omp::Clause::OMPC_use_device_ptr,
    llvm::omp::Clause::OMPC_use_device_addr,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_data {
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_if,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_data {
  };

  static OmpClauseSet requiredClauses_OMPD_target_data {
    llvm::omp::Clause::OMPC_map,
  };

  // Sets for target enter data

  static OmpClauseSet allowedClauses_OMPD_target_enter_data {
    llvm::omp::Clause::OMPC_depend,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_enter_data {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_nowait,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_enter_data {
  };

  static OmpClauseSet requiredClauses_OMPD_target_enter_data {
    llvm::omp::Clause::OMPC_map,
  };

  // Sets for target exit data

  static OmpClauseSet allowedClauses_OMPD_target_exit_data {
    llvm::omp::Clause::OMPC_depend,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_exit_data {
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_nowait,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_exit_data {
  };

  static OmpClauseSet requiredClauses_OMPD_target_exit_data {
    llvm::omp::Clause::OMPC_map,
  };

  // Sets for target parallel

  static OmpClauseSet allowedClauses_OMPD_target_parallel {
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_uses_allocators,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_parallel {
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_parallel {
  };

  static OmpClauseSet requiredClauses_OMPD_target_parallel {
  };

  // Sets for target parallel do

  static OmpClauseSet allowedClauses_OMPD_target_parallel_do {
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_allocator,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_uses_allocators,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_copyin,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_parallel_do {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_nowait,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_parallel_do {
  };

  static OmpClauseSet requiredClauses_OMPD_target_parallel_do {
  };

  // Sets for target parallel do simd

  static OmpClauseSet allowedClauses_OMPD_target_parallel_do_simd {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_uses_allocators,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_parallel_do_simd {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_parallel_do_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_target_parallel_do_simd {
  };

  // Sets for target parallel for

  static OmpClauseSet allowedClauses_OMPD_target_parallel_for {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_uses_allocators,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_parallel_for {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_parallel_for {
  };

  static OmpClauseSet requiredClauses_OMPD_target_parallel_for {
  };

  // Sets for target parallel for simd

  static OmpClauseSet allowedClauses_OMPD_target_parallel_for_simd {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_uses_allocators,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_parallel_for_simd {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_parallel_for_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_target_parallel_for_simd {
  };

  // Sets for target simd

  static OmpClauseSet allowedClauses_OMPD_target_simd {
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_uses_allocators,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_simd {
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_schedule,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_target_simd {
  };

  // Sets for target teams

  static OmpClauseSet allowedClauses_OMPD_target_teams {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_uses_allocators,
    llvm::omp::Clause::OMPC_shared,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_teams {
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_teams {
  };

  static OmpClauseSet requiredClauses_OMPD_target_teams {
  };

  // Sets for target teams distribute

  static OmpClauseSet allowedClauses_OMPD_target_teams_distribute {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_uses_allocators,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_lastprivate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_teams_distribute {
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_teams_distribute {
  };

  static OmpClauseSet requiredClauses_OMPD_target_teams_distribute {
  };

  // Sets for target teams distribute parallel do

  static OmpClauseSet allowedClauses_OMPD_target_teams_distribute_parallel_do {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_uses_allocators,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_teams_distribute_parallel_do {
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_schedule,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_teams_distribute_parallel_do {
  };

  static OmpClauseSet requiredClauses_OMPD_target_teams_distribute_parallel_do {
  };

  // Sets for target teams distribute parallel do simd

  static OmpClauseSet allowedClauses_OMPD_target_teams_distribute_parallel_do_simd {
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_uses_allocators,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_nontemporal,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_teams_distribute_parallel_do_simd {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_teams_distribute_parallel_do_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_target_teams_distribute_parallel_do_simd {
  };

  // Sets for target teams distribute parallel for

  static OmpClauseSet allowedClauses_OMPD_target_teams_distribute_parallel_for {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_uses_allocators,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_teams_distribute_parallel_for {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_teams_distribute_parallel_for {
  };

  static OmpClauseSet requiredClauses_OMPD_target_teams_distribute_parallel_for {
  };

  // Sets for target teams distribute parallel for simd

  static OmpClauseSet allowedClauses_OMPD_target_teams_distribute_parallel_for_simd {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_uses_allocators,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_teams_distribute_parallel_for_simd {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_teams_distribute_parallel_for_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_target_teams_distribute_parallel_for_simd {
  };

  // Sets for target teams distribute simd

  static OmpClauseSet allowedClauses_OMPD_target_teams_distribute_simd {
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_is_device_ptr,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_map,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_uses_allocators,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_teams_distribute_simd {
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_defaultmap,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_teams_distribute_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_target_teams_distribute_simd {
  };

  // Sets for target update

  static OmpClauseSet allowedClauses_OMPD_target_update {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_device,
    llvm::omp::Clause::OMPC_to,
    llvm::omp::Clause::OMPC_from,
    llvm::omp::Clause::OMPC_nowait,
    llvm::omp::Clause::OMPC_depend,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_target_update {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_target_update {
  };

  static OmpClauseSet requiredClauses_OMPD_target_update {
  };

  // Sets for task

  static OmpClauseSet allowedClauses_OMPD_task {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_untied,
    llvm::omp::Clause::OMPC_mergeable,
    llvm::omp::Clause::OMPC_depend,
    llvm::omp::Clause::OMPC_in_reduction,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_detach,
    llvm::omp::Clause::OMPC_affinity,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_task {
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_final,
    llvm::omp::Clause::OMPC_priority,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_task {
  };

  static OmpClauseSet requiredClauses_OMPD_task {
  };

  // Sets for taskgroup

  static OmpClauseSet allowedClauses_OMPD_taskgroup {
    llvm::omp::Clause::OMPC_task_reduction,
    llvm::omp::Clause::OMPC_allocate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_taskgroup {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_taskgroup {
  };

  static OmpClauseSet requiredClauses_OMPD_taskgroup {
  };

  // Sets for taskloop

  static OmpClauseSet allowedClauses_OMPD_taskloop {
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_untied,
    llvm::omp::Clause::OMPC_mergeable,
    llvm::omp::Clause::OMPC_nogroup,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_in_reduction,
    llvm::omp::Clause::OMPC_allocate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_taskloop {
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_final,
    llvm::omp::Clause::OMPC_priority,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_taskloop {
    llvm::omp::Clause::OMPC_grainsize,
    llvm::omp::Clause::OMPC_num_tasks,
  };

  static OmpClauseSet requiredClauses_OMPD_taskloop {
  };

  // Sets for taskloop simd

  static OmpClauseSet allowedClauses_OMPD_taskloop_simd {
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_in_reduction,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_mergeable,
    llvm::omp::Clause::OMPC_nogroup,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_untied,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_taskloop_simd {
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_final,
    llvm::omp::Clause::OMPC_priority,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_taskloop_simd {
    llvm::omp::Clause::OMPC_grainsize,
    llvm::omp::Clause::OMPC_num_tasks,
  };

  static OmpClauseSet requiredClauses_OMPD_taskloop_simd {
  };

  // Sets for taskwait

  static OmpClauseSet allowedClauses_OMPD_taskwait {
    llvm::omp::Clause::OMPC_depend,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_taskwait {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_taskwait {
  };

  static OmpClauseSet requiredClauses_OMPD_taskwait {
  };

  // Sets for taskyield

  static OmpClauseSet allowedClauses_OMPD_taskyield {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_taskyield {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_taskyield {
  };

  static OmpClauseSet requiredClauses_OMPD_taskyield {
  };

  // Sets for teams

  static OmpClauseSet allowedClauses_OMPD_teams {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_allocate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_teams {
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_teams {
  };

  static OmpClauseSet requiredClauses_OMPD_teams {
  };

  // Sets for teams distribute

  static OmpClauseSet allowedClauses_OMPD_teams_distribute {
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_allocate,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_teams_distribute {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_teams_distribute {
  };

  static OmpClauseSet requiredClauses_OMPD_teams_distribute {
  };

  // Sets for teams distribute parallel do

  static OmpClauseSet allowedClauses_OMPD_teams_distribute_parallel_do {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_linear,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_teams_distribute_parallel_do {
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_ordered,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_schedule,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_teams_distribute_parallel_do {
  };

  static OmpClauseSet requiredClauses_OMPD_teams_distribute_parallel_do {
  };

  // Sets for teams distribute parallel do simd

  static OmpClauseSet allowedClauses_OMPD_teams_distribute_parallel_do_simd {
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_nontemporal,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_teams_distribute_parallel_do_simd {
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_if,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_teams_distribute_parallel_do_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_teams_distribute_parallel_do_simd {
  };

  // Sets for teams distribute parallel for

  static OmpClauseSet allowedClauses_OMPD_teams_distribute_parallel_for {
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
    llvm::omp::Clause::OMPC_copyin,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_teams_distribute_parallel_for {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_teams_distribute_parallel_for {
  };

  static OmpClauseSet requiredClauses_OMPD_teams_distribute_parallel_for {
  };

  // Sets for teams distribute parallel for simd

  static OmpClauseSet allowedClauses_OMPD_teams_distribute_parallel_for_simd {
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_threads,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_proc_bind,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_shared,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_schedule,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_thread_limit,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_teams_distribute_parallel_for_simd {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_teams_distribute_parallel_for_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_teams_distribute_parallel_for_simd {
  };

  // Sets for teams distribute simd

  static OmpClauseSet allowedClauses_OMPD_teams_distribute_simd {
    llvm::omp::Clause::OMPC_aligned,
    llvm::omp::Clause::OMPC_allocate,
    llvm::omp::Clause::OMPC_firstprivate,
    llvm::omp::Clause::OMPC_lastprivate,
    llvm::omp::Clause::OMPC_linear,
    llvm::omp::Clause::OMPC_nontemporal,
    llvm::omp::Clause::OMPC_order,
    llvm::omp::Clause::OMPC_private,
    llvm::omp::Clause::OMPC_reduction,
    llvm::omp::Clause::OMPC_shared,
  };

  static OmpClauseSet allowedOnceClauses_OMPD_teams_distribute_simd {
    llvm::omp::Clause::OMPC_collapse,
    llvm::omp::Clause::OMPC_default,
    llvm::omp::Clause::OMPC_dist_schedule,
    llvm::omp::Clause::OMPC_if,
    llvm::omp::Clause::OMPC_num_teams,
    llvm::omp::Clause::OMPC_safelen,
    llvm::omp::Clause::OMPC_simdlen,
    llvm::omp::Clause::OMPC_thread_limit,
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_teams_distribute_simd {
  };

  static OmpClauseSet requiredClauses_OMPD_teams_distribute_simd {
  };

  // Sets for threadprivate

  static OmpClauseSet allowedClauses_OMPD_threadprivate {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_threadprivate {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_threadprivate {
  };

  static OmpClauseSet requiredClauses_OMPD_threadprivate {
  };

  // Sets for unknown

  static OmpClauseSet allowedClauses_OMPD_unknown {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_unknown {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_unknown {
  };

  static OmpClauseSet requiredClauses_OMPD_unknown {
  };

  // Sets for workshare

  static OmpClauseSet allowedClauses_OMPD_workshare {
  };

  static OmpClauseSet allowedOnceClauses_OMPD_workshare {
  };

  static OmpClauseSet allowedExclusiveClauses_OMPD_workshare {
  };

  static OmpClauseSet requiredClauses_OMPD_workshare {
  };
} // namespace omp
} // namespace llvm

#endif // GEN_FLANG_DIRECTIVE_CLAUSE_SETS

#ifdef GEN_FLANG_DIRECTIVE_CLAUSE_MAP
#undef GEN_FLANG_DIRECTIVE_CLAUSE_MAP

{
  {llvm::omp::Directive::OMPD_allocate,
    {
      llvm::omp::allowedClauses_OMPD_allocate,
      llvm::omp::allowedOnceClauses_OMPD_allocate,
      llvm::omp::allowedExclusiveClauses_OMPD_allocate,
      llvm::omp::requiredClauses_OMPD_allocate,
    }
  },
  {llvm::omp::Directive::OMPD_assumes,
    {
      llvm::omp::allowedClauses_OMPD_assumes,
      llvm::omp::allowedOnceClauses_OMPD_assumes,
      llvm::omp::allowedExclusiveClauses_OMPD_assumes,
      llvm::omp::requiredClauses_OMPD_assumes,
    }
  },
  {llvm::omp::Directive::OMPD_atomic,
    {
      llvm::omp::allowedClauses_OMPD_atomic,
      llvm::omp::allowedOnceClauses_OMPD_atomic,
      llvm::omp::allowedExclusiveClauses_OMPD_atomic,
      llvm::omp::requiredClauses_OMPD_atomic,
    }
  },
  {llvm::omp::Directive::OMPD_barrier,
    {
      llvm::omp::allowedClauses_OMPD_barrier,
      llvm::omp::allowedOnceClauses_OMPD_barrier,
      llvm::omp::allowedExclusiveClauses_OMPD_barrier,
      llvm::omp::requiredClauses_OMPD_barrier,
    }
  },
  {llvm::omp::Directive::OMPD_begin_assumes,
    {
      llvm::omp::allowedClauses_OMPD_begin_assumes,
      llvm::omp::allowedOnceClauses_OMPD_begin_assumes,
      llvm::omp::allowedExclusiveClauses_OMPD_begin_assumes,
      llvm::omp::requiredClauses_OMPD_begin_assumes,
    }
  },
  {llvm::omp::Directive::OMPD_begin_declare_variant,
    {
      llvm::omp::allowedClauses_OMPD_begin_declare_variant,
      llvm::omp::allowedOnceClauses_OMPD_begin_declare_variant,
      llvm::omp::allowedExclusiveClauses_OMPD_begin_declare_variant,
      llvm::omp::requiredClauses_OMPD_begin_declare_variant,
    }
  },
  {llvm::omp::Directive::OMPD_cancel,
    {
      llvm::omp::allowedClauses_OMPD_cancel,
      llvm::omp::allowedOnceClauses_OMPD_cancel,
      llvm::omp::allowedExclusiveClauses_OMPD_cancel,
      llvm::omp::requiredClauses_OMPD_cancel,
    }
  },
  {llvm::omp::Directive::OMPD_cancellation_point,
    {
      llvm::omp::allowedClauses_OMPD_cancellation_point,
      llvm::omp::allowedOnceClauses_OMPD_cancellation_point,
      llvm::omp::allowedExclusiveClauses_OMPD_cancellation_point,
      llvm::omp::requiredClauses_OMPD_cancellation_point,
    }
  },
  {llvm::omp::Directive::OMPD_critical,
    {
      llvm::omp::allowedClauses_OMPD_critical,
      llvm::omp::allowedOnceClauses_OMPD_critical,
      llvm::omp::allowedExclusiveClauses_OMPD_critical,
      llvm::omp::requiredClauses_OMPD_critical,
    }
  },
  {llvm::omp::Directive::OMPD_declare_mapper,
    {
      llvm::omp::allowedClauses_OMPD_declare_mapper,
      llvm::omp::allowedOnceClauses_OMPD_declare_mapper,
      llvm::omp::allowedExclusiveClauses_OMPD_declare_mapper,
      llvm::omp::requiredClauses_OMPD_declare_mapper,
    }
  },
  {llvm::omp::Directive::OMPD_declare_reduction,
    {
      llvm::omp::allowedClauses_OMPD_declare_reduction,
      llvm::omp::allowedOnceClauses_OMPD_declare_reduction,
      llvm::omp::allowedExclusiveClauses_OMPD_declare_reduction,
      llvm::omp::requiredClauses_OMPD_declare_reduction,
    }
  },
  {llvm::omp::Directive::OMPD_declare_simd,
    {
      llvm::omp::allowedClauses_OMPD_declare_simd,
      llvm::omp::allowedOnceClauses_OMPD_declare_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_declare_simd,
      llvm::omp::requiredClauses_OMPD_declare_simd,
    }
  },
  {llvm::omp::Directive::OMPD_declare_target,
    {
      llvm::omp::allowedClauses_OMPD_declare_target,
      llvm::omp::allowedOnceClauses_OMPD_declare_target,
      llvm::omp::allowedExclusiveClauses_OMPD_declare_target,
      llvm::omp::requiredClauses_OMPD_declare_target,
    }
  },
  {llvm::omp::Directive::OMPD_declare_variant,
    {
      llvm::omp::allowedClauses_OMPD_declare_variant,
      llvm::omp::allowedOnceClauses_OMPD_declare_variant,
      llvm::omp::allowedExclusiveClauses_OMPD_declare_variant,
      llvm::omp::requiredClauses_OMPD_declare_variant,
    }
  },
  {llvm::omp::Directive::OMPD_depobj,
    {
      llvm::omp::allowedClauses_OMPD_depobj,
      llvm::omp::allowedOnceClauses_OMPD_depobj,
      llvm::omp::allowedExclusiveClauses_OMPD_depobj,
      llvm::omp::requiredClauses_OMPD_depobj,
    }
  },
  {llvm::omp::Directive::OMPD_distribute,
    {
      llvm::omp::allowedClauses_OMPD_distribute,
      llvm::omp::allowedOnceClauses_OMPD_distribute,
      llvm::omp::allowedExclusiveClauses_OMPD_distribute,
      llvm::omp::requiredClauses_OMPD_distribute,
    }
  },
  {llvm::omp::Directive::OMPD_distribute_parallel_do,
    {
      llvm::omp::allowedClauses_OMPD_distribute_parallel_do,
      llvm::omp::allowedOnceClauses_OMPD_distribute_parallel_do,
      llvm::omp::allowedExclusiveClauses_OMPD_distribute_parallel_do,
      llvm::omp::requiredClauses_OMPD_distribute_parallel_do,
    }
  },
  {llvm::omp::Directive::OMPD_distribute_parallel_do_simd,
    {
      llvm::omp::allowedClauses_OMPD_distribute_parallel_do_simd,
      llvm::omp::allowedOnceClauses_OMPD_distribute_parallel_do_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_distribute_parallel_do_simd,
      llvm::omp::requiredClauses_OMPD_distribute_parallel_do_simd,
    }
  },
  {llvm::omp::Directive::OMPD_distribute_parallel_for,
    {
      llvm::omp::allowedClauses_OMPD_distribute_parallel_for,
      llvm::omp::allowedOnceClauses_OMPD_distribute_parallel_for,
      llvm::omp::allowedExclusiveClauses_OMPD_distribute_parallel_for,
      llvm::omp::requiredClauses_OMPD_distribute_parallel_for,
    }
  },
  {llvm::omp::Directive::OMPD_distribute_parallel_for_simd,
    {
      llvm::omp::allowedClauses_OMPD_distribute_parallel_for_simd,
      llvm::omp::allowedOnceClauses_OMPD_distribute_parallel_for_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_distribute_parallel_for_simd,
      llvm::omp::requiredClauses_OMPD_distribute_parallel_for_simd,
    }
  },
  {llvm::omp::Directive::OMPD_distribute_simd,
    {
      llvm::omp::allowedClauses_OMPD_distribute_simd,
      llvm::omp::allowedOnceClauses_OMPD_distribute_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_distribute_simd,
      llvm::omp::requiredClauses_OMPD_distribute_simd,
    }
  },
  {llvm::omp::Directive::OMPD_do,
    {
      llvm::omp::allowedClauses_OMPD_do,
      llvm::omp::allowedOnceClauses_OMPD_do,
      llvm::omp::allowedExclusiveClauses_OMPD_do,
      llvm::omp::requiredClauses_OMPD_do,
    }
  },
  {llvm::omp::Directive::OMPD_do_simd,
    {
      llvm::omp::allowedClauses_OMPD_do_simd,
      llvm::omp::allowedOnceClauses_OMPD_do_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_do_simd,
      llvm::omp::requiredClauses_OMPD_do_simd,
    }
  },
  {llvm::omp::Directive::OMPD_end_assumes,
    {
      llvm::omp::allowedClauses_OMPD_end_assumes,
      llvm::omp::allowedOnceClauses_OMPD_end_assumes,
      llvm::omp::allowedExclusiveClauses_OMPD_end_assumes,
      llvm::omp::requiredClauses_OMPD_end_assumes,
    }
  },
  {llvm::omp::Directive::OMPD_end_declare_target,
    {
      llvm::omp::allowedClauses_OMPD_end_declare_target,
      llvm::omp::allowedOnceClauses_OMPD_end_declare_target,
      llvm::omp::allowedExclusiveClauses_OMPD_end_declare_target,
      llvm::omp::requiredClauses_OMPD_end_declare_target,
    }
  },
  {llvm::omp::Directive::OMPD_end_declare_variant,
    {
      llvm::omp::allowedClauses_OMPD_end_declare_variant,
      llvm::omp::allowedOnceClauses_OMPD_end_declare_variant,
      llvm::omp::allowedExclusiveClauses_OMPD_end_declare_variant,
      llvm::omp::requiredClauses_OMPD_end_declare_variant,
    }
  },
  {llvm::omp::Directive::OMPD_end_do,
    {
      llvm::omp::allowedClauses_OMPD_end_do,
      llvm::omp::allowedOnceClauses_OMPD_end_do,
      llvm::omp::allowedExclusiveClauses_OMPD_end_do,
      llvm::omp::requiredClauses_OMPD_end_do,
    }
  },
  {llvm::omp::Directive::OMPD_end_do_simd,
    {
      llvm::omp::allowedClauses_OMPD_end_do_simd,
      llvm::omp::allowedOnceClauses_OMPD_end_do_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_end_do_simd,
      llvm::omp::requiredClauses_OMPD_end_do_simd,
    }
  },
  {llvm::omp::Directive::OMPD_end_sections,
    {
      llvm::omp::allowedClauses_OMPD_end_sections,
      llvm::omp::allowedOnceClauses_OMPD_end_sections,
      llvm::omp::allowedExclusiveClauses_OMPD_end_sections,
      llvm::omp::requiredClauses_OMPD_end_sections,
    }
  },
  {llvm::omp::Directive::OMPD_end_single,
    {
      llvm::omp::allowedClauses_OMPD_end_single,
      llvm::omp::allowedOnceClauses_OMPD_end_single,
      llvm::omp::allowedExclusiveClauses_OMPD_end_single,
      llvm::omp::requiredClauses_OMPD_end_single,
    }
  },
  {llvm::omp::Directive::OMPD_end_workshare,
    {
      llvm::omp::allowedClauses_OMPD_end_workshare,
      llvm::omp::allowedOnceClauses_OMPD_end_workshare,
      llvm::omp::allowedExclusiveClauses_OMPD_end_workshare,
      llvm::omp::requiredClauses_OMPD_end_workshare,
    }
  },
  {llvm::omp::Directive::OMPD_flush,
    {
      llvm::omp::allowedClauses_OMPD_flush,
      llvm::omp::allowedOnceClauses_OMPD_flush,
      llvm::omp::allowedExclusiveClauses_OMPD_flush,
      llvm::omp::requiredClauses_OMPD_flush,
    }
  },
  {llvm::omp::Directive::OMPD_for,
    {
      llvm::omp::allowedClauses_OMPD_for,
      llvm::omp::allowedOnceClauses_OMPD_for,
      llvm::omp::allowedExclusiveClauses_OMPD_for,
      llvm::omp::requiredClauses_OMPD_for,
    }
  },
  {llvm::omp::Directive::OMPD_for_simd,
    {
      llvm::omp::allowedClauses_OMPD_for_simd,
      llvm::omp::allowedOnceClauses_OMPD_for_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_for_simd,
      llvm::omp::requiredClauses_OMPD_for_simd,
    }
  },
  {llvm::omp::Directive::OMPD_master,
    {
      llvm::omp::allowedClauses_OMPD_master,
      llvm::omp::allowedOnceClauses_OMPD_master,
      llvm::omp::allowedExclusiveClauses_OMPD_master,
      llvm::omp::requiredClauses_OMPD_master,
    }
  },
  {llvm::omp::Directive::OMPD_master_taskloop,
    {
      llvm::omp::allowedClauses_OMPD_master_taskloop,
      llvm::omp::allowedOnceClauses_OMPD_master_taskloop,
      llvm::omp::allowedExclusiveClauses_OMPD_master_taskloop,
      llvm::omp::requiredClauses_OMPD_master_taskloop,
    }
  },
  {llvm::omp::Directive::OMPD_master_taskloop_simd,
    {
      llvm::omp::allowedClauses_OMPD_master_taskloop_simd,
      llvm::omp::allowedOnceClauses_OMPD_master_taskloop_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_master_taskloop_simd,
      llvm::omp::requiredClauses_OMPD_master_taskloop_simd,
    }
  },
  {llvm::omp::Directive::OMPD_ordered,
    {
      llvm::omp::allowedClauses_OMPD_ordered,
      llvm::omp::allowedOnceClauses_OMPD_ordered,
      llvm::omp::allowedExclusiveClauses_OMPD_ordered,
      llvm::omp::requiredClauses_OMPD_ordered,
    }
  },
  {llvm::omp::Directive::OMPD_parallel,
    {
      llvm::omp::allowedClauses_OMPD_parallel,
      llvm::omp::allowedOnceClauses_OMPD_parallel,
      llvm::omp::allowedExclusiveClauses_OMPD_parallel,
      llvm::omp::requiredClauses_OMPD_parallel,
    }
  },
  {llvm::omp::Directive::OMPD_parallel_do,
    {
      llvm::omp::allowedClauses_OMPD_parallel_do,
      llvm::omp::allowedOnceClauses_OMPD_parallel_do,
      llvm::omp::allowedExclusiveClauses_OMPD_parallel_do,
      llvm::omp::requiredClauses_OMPD_parallel_do,
    }
  },
  {llvm::omp::Directive::OMPD_parallel_do_simd,
    {
      llvm::omp::allowedClauses_OMPD_parallel_do_simd,
      llvm::omp::allowedOnceClauses_OMPD_parallel_do_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_parallel_do_simd,
      llvm::omp::requiredClauses_OMPD_parallel_do_simd,
    }
  },
  {llvm::omp::Directive::OMPD_parallel_for,
    {
      llvm::omp::allowedClauses_OMPD_parallel_for,
      llvm::omp::allowedOnceClauses_OMPD_parallel_for,
      llvm::omp::allowedExclusiveClauses_OMPD_parallel_for,
      llvm::omp::requiredClauses_OMPD_parallel_for,
    }
  },
  {llvm::omp::Directive::OMPD_parallel_for_simd,
    {
      llvm::omp::allowedClauses_OMPD_parallel_for_simd,
      llvm::omp::allowedOnceClauses_OMPD_parallel_for_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_parallel_for_simd,
      llvm::omp::requiredClauses_OMPD_parallel_for_simd,
    }
  },
  {llvm::omp::Directive::OMPD_parallel_master,
    {
      llvm::omp::allowedClauses_OMPD_parallel_master,
      llvm::omp::allowedOnceClauses_OMPD_parallel_master,
      llvm::omp::allowedExclusiveClauses_OMPD_parallel_master,
      llvm::omp::requiredClauses_OMPD_parallel_master,
    }
  },
  {llvm::omp::Directive::OMPD_parallel_master_taskloop,
    {
      llvm::omp::allowedClauses_OMPD_parallel_master_taskloop,
      llvm::omp::allowedOnceClauses_OMPD_parallel_master_taskloop,
      llvm::omp::allowedExclusiveClauses_OMPD_parallel_master_taskloop,
      llvm::omp::requiredClauses_OMPD_parallel_master_taskloop,
    }
  },
  {llvm::omp::Directive::OMPD_parallel_master_taskloop_simd,
    {
      llvm::omp::allowedClauses_OMPD_parallel_master_taskloop_simd,
      llvm::omp::allowedOnceClauses_OMPD_parallel_master_taskloop_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_parallel_master_taskloop_simd,
      llvm::omp::requiredClauses_OMPD_parallel_master_taskloop_simd,
    }
  },
  {llvm::omp::Directive::OMPD_parallel_sections,
    {
      llvm::omp::allowedClauses_OMPD_parallel_sections,
      llvm::omp::allowedOnceClauses_OMPD_parallel_sections,
      llvm::omp::allowedExclusiveClauses_OMPD_parallel_sections,
      llvm::omp::requiredClauses_OMPD_parallel_sections,
    }
  },
  {llvm::omp::Directive::OMPD_parallel_workshare,
    {
      llvm::omp::allowedClauses_OMPD_parallel_workshare,
      llvm::omp::allowedOnceClauses_OMPD_parallel_workshare,
      llvm::omp::allowedExclusiveClauses_OMPD_parallel_workshare,
      llvm::omp::requiredClauses_OMPD_parallel_workshare,
    }
  },
  {llvm::omp::Directive::OMPD_requires,
    {
      llvm::omp::allowedClauses_OMPD_requires,
      llvm::omp::allowedOnceClauses_OMPD_requires,
      llvm::omp::allowedExclusiveClauses_OMPD_requires,
      llvm::omp::requiredClauses_OMPD_requires,
    }
  },
  {llvm::omp::Directive::OMPD_scan,
    {
      llvm::omp::allowedClauses_OMPD_scan,
      llvm::omp::allowedOnceClauses_OMPD_scan,
      llvm::omp::allowedExclusiveClauses_OMPD_scan,
      llvm::omp::requiredClauses_OMPD_scan,
    }
  },
  {llvm::omp::Directive::OMPD_section,
    {
      llvm::omp::allowedClauses_OMPD_section,
      llvm::omp::allowedOnceClauses_OMPD_section,
      llvm::omp::allowedExclusiveClauses_OMPD_section,
      llvm::omp::requiredClauses_OMPD_section,
    }
  },
  {llvm::omp::Directive::OMPD_sections,
    {
      llvm::omp::allowedClauses_OMPD_sections,
      llvm::omp::allowedOnceClauses_OMPD_sections,
      llvm::omp::allowedExclusiveClauses_OMPD_sections,
      llvm::omp::requiredClauses_OMPD_sections,
    }
  },
  {llvm::omp::Directive::OMPD_simd,
    {
      llvm::omp::allowedClauses_OMPD_simd,
      llvm::omp::allowedOnceClauses_OMPD_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_simd,
      llvm::omp::requiredClauses_OMPD_simd,
    }
  },
  {llvm::omp::Directive::OMPD_single,
    {
      llvm::omp::allowedClauses_OMPD_single,
      llvm::omp::allowedOnceClauses_OMPD_single,
      llvm::omp::allowedExclusiveClauses_OMPD_single,
      llvm::omp::requiredClauses_OMPD_single,
    }
  },
  {llvm::omp::Directive::OMPD_target,
    {
      llvm::omp::allowedClauses_OMPD_target,
      llvm::omp::allowedOnceClauses_OMPD_target,
      llvm::omp::allowedExclusiveClauses_OMPD_target,
      llvm::omp::requiredClauses_OMPD_target,
    }
  },
  {llvm::omp::Directive::OMPD_target_data,
    {
      llvm::omp::allowedClauses_OMPD_target_data,
      llvm::omp::allowedOnceClauses_OMPD_target_data,
      llvm::omp::allowedExclusiveClauses_OMPD_target_data,
      llvm::omp::requiredClauses_OMPD_target_data,
    }
  },
  {llvm::omp::Directive::OMPD_target_enter_data,
    {
      llvm::omp::allowedClauses_OMPD_target_enter_data,
      llvm::omp::allowedOnceClauses_OMPD_target_enter_data,
      llvm::omp::allowedExclusiveClauses_OMPD_target_enter_data,
      llvm::omp::requiredClauses_OMPD_target_enter_data,
    }
  },
  {llvm::omp::Directive::OMPD_target_exit_data,
    {
      llvm::omp::allowedClauses_OMPD_target_exit_data,
      llvm::omp::allowedOnceClauses_OMPD_target_exit_data,
      llvm::omp::allowedExclusiveClauses_OMPD_target_exit_data,
      llvm::omp::requiredClauses_OMPD_target_exit_data,
    }
  },
  {llvm::omp::Directive::OMPD_target_parallel,
    {
      llvm::omp::allowedClauses_OMPD_target_parallel,
      llvm::omp::allowedOnceClauses_OMPD_target_parallel,
      llvm::omp::allowedExclusiveClauses_OMPD_target_parallel,
      llvm::omp::requiredClauses_OMPD_target_parallel,
    }
  },
  {llvm::omp::Directive::OMPD_target_parallel_do,
    {
      llvm::omp::allowedClauses_OMPD_target_parallel_do,
      llvm::omp::allowedOnceClauses_OMPD_target_parallel_do,
      llvm::omp::allowedExclusiveClauses_OMPD_target_parallel_do,
      llvm::omp::requiredClauses_OMPD_target_parallel_do,
    }
  },
  {llvm::omp::Directive::OMPD_target_parallel_do_simd,
    {
      llvm::omp::allowedClauses_OMPD_target_parallel_do_simd,
      llvm::omp::allowedOnceClauses_OMPD_target_parallel_do_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_target_parallel_do_simd,
      llvm::omp::requiredClauses_OMPD_target_parallel_do_simd,
    }
  },
  {llvm::omp::Directive::OMPD_target_parallel_for,
    {
      llvm::omp::allowedClauses_OMPD_target_parallel_for,
      llvm::omp::allowedOnceClauses_OMPD_target_parallel_for,
      llvm::omp::allowedExclusiveClauses_OMPD_target_parallel_for,
      llvm::omp::requiredClauses_OMPD_target_parallel_for,
    }
  },
  {llvm::omp::Directive::OMPD_target_parallel_for_simd,
    {
      llvm::omp::allowedClauses_OMPD_target_parallel_for_simd,
      llvm::omp::allowedOnceClauses_OMPD_target_parallel_for_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_target_parallel_for_simd,
      llvm::omp::requiredClauses_OMPD_target_parallel_for_simd,
    }
  },
  {llvm::omp::Directive::OMPD_target_simd,
    {
      llvm::omp::allowedClauses_OMPD_target_simd,
      llvm::omp::allowedOnceClauses_OMPD_target_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_target_simd,
      llvm::omp::requiredClauses_OMPD_target_simd,
    }
  },
  {llvm::omp::Directive::OMPD_target_teams,
    {
      llvm::omp::allowedClauses_OMPD_target_teams,
      llvm::omp::allowedOnceClauses_OMPD_target_teams,
      llvm::omp::allowedExclusiveClauses_OMPD_target_teams,
      llvm::omp::requiredClauses_OMPD_target_teams,
    }
  },
  {llvm::omp::Directive::OMPD_target_teams_distribute,
    {
      llvm::omp::allowedClauses_OMPD_target_teams_distribute,
      llvm::omp::allowedOnceClauses_OMPD_target_teams_distribute,
      llvm::omp::allowedExclusiveClauses_OMPD_target_teams_distribute,
      llvm::omp::requiredClauses_OMPD_target_teams_distribute,
    }
  },
  {llvm::omp::Directive::OMPD_target_teams_distribute_parallel_do,
    {
      llvm::omp::allowedClauses_OMPD_target_teams_distribute_parallel_do,
      llvm::omp::allowedOnceClauses_OMPD_target_teams_distribute_parallel_do,
      llvm::omp::allowedExclusiveClauses_OMPD_target_teams_distribute_parallel_do,
      llvm::omp::requiredClauses_OMPD_target_teams_distribute_parallel_do,
    }
  },
  {llvm::omp::Directive::OMPD_target_teams_distribute_parallel_do_simd,
    {
      llvm::omp::allowedClauses_OMPD_target_teams_distribute_parallel_do_simd,
      llvm::omp::allowedOnceClauses_OMPD_target_teams_distribute_parallel_do_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_target_teams_distribute_parallel_do_simd,
      llvm::omp::requiredClauses_OMPD_target_teams_distribute_parallel_do_simd,
    }
  },
  {llvm::omp::Directive::OMPD_target_teams_distribute_parallel_for,
    {
      llvm::omp::allowedClauses_OMPD_target_teams_distribute_parallel_for,
      llvm::omp::allowedOnceClauses_OMPD_target_teams_distribute_parallel_for,
      llvm::omp::allowedExclusiveClauses_OMPD_target_teams_distribute_parallel_for,
      llvm::omp::requiredClauses_OMPD_target_teams_distribute_parallel_for,
    }
  },
  {llvm::omp::Directive::OMPD_target_teams_distribute_parallel_for_simd,
    {
      llvm::omp::allowedClauses_OMPD_target_teams_distribute_parallel_for_simd,
      llvm::omp::allowedOnceClauses_OMPD_target_teams_distribute_parallel_for_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_target_teams_distribute_parallel_for_simd,
      llvm::omp::requiredClauses_OMPD_target_teams_distribute_parallel_for_simd,
    }
  },
  {llvm::omp::Directive::OMPD_target_teams_distribute_simd,
    {
      llvm::omp::allowedClauses_OMPD_target_teams_distribute_simd,
      llvm::omp::allowedOnceClauses_OMPD_target_teams_distribute_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_target_teams_distribute_simd,
      llvm::omp::requiredClauses_OMPD_target_teams_distribute_simd,
    }
  },
  {llvm::omp::Directive::OMPD_target_update,
    {
      llvm::omp::allowedClauses_OMPD_target_update,
      llvm::omp::allowedOnceClauses_OMPD_target_update,
      llvm::omp::allowedExclusiveClauses_OMPD_target_update,
      llvm::omp::requiredClauses_OMPD_target_update,
    }
  },
  {llvm::omp::Directive::OMPD_task,
    {
      llvm::omp::allowedClauses_OMPD_task,
      llvm::omp::allowedOnceClauses_OMPD_task,
      llvm::omp::allowedExclusiveClauses_OMPD_task,
      llvm::omp::requiredClauses_OMPD_task,
    }
  },
  {llvm::omp::Directive::OMPD_taskgroup,
    {
      llvm::omp::allowedClauses_OMPD_taskgroup,
      llvm::omp::allowedOnceClauses_OMPD_taskgroup,
      llvm::omp::allowedExclusiveClauses_OMPD_taskgroup,
      llvm::omp::requiredClauses_OMPD_taskgroup,
    }
  },
  {llvm::omp::Directive::OMPD_taskloop,
    {
      llvm::omp::allowedClauses_OMPD_taskloop,
      llvm::omp::allowedOnceClauses_OMPD_taskloop,
      llvm::omp::allowedExclusiveClauses_OMPD_taskloop,
      llvm::omp::requiredClauses_OMPD_taskloop,
    }
  },
  {llvm::omp::Directive::OMPD_taskloop_simd,
    {
      llvm::omp::allowedClauses_OMPD_taskloop_simd,
      llvm::omp::allowedOnceClauses_OMPD_taskloop_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_taskloop_simd,
      llvm::omp::requiredClauses_OMPD_taskloop_simd,
    }
  },
  {llvm::omp::Directive::OMPD_taskwait,
    {
      llvm::omp::allowedClauses_OMPD_taskwait,
      llvm::omp::allowedOnceClauses_OMPD_taskwait,
      llvm::omp::allowedExclusiveClauses_OMPD_taskwait,
      llvm::omp::requiredClauses_OMPD_taskwait,
    }
  },
  {llvm::omp::Directive::OMPD_taskyield,
    {
      llvm::omp::allowedClauses_OMPD_taskyield,
      llvm::omp::allowedOnceClauses_OMPD_taskyield,
      llvm::omp::allowedExclusiveClauses_OMPD_taskyield,
      llvm::omp::requiredClauses_OMPD_taskyield,
    }
  },
  {llvm::omp::Directive::OMPD_teams,
    {
      llvm::omp::allowedClauses_OMPD_teams,
      llvm::omp::allowedOnceClauses_OMPD_teams,
      llvm::omp::allowedExclusiveClauses_OMPD_teams,
      llvm::omp::requiredClauses_OMPD_teams,
    }
  },
  {llvm::omp::Directive::OMPD_teams_distribute,
    {
      llvm::omp::allowedClauses_OMPD_teams_distribute,
      llvm::omp::allowedOnceClauses_OMPD_teams_distribute,
      llvm::omp::allowedExclusiveClauses_OMPD_teams_distribute,
      llvm::omp::requiredClauses_OMPD_teams_distribute,
    }
  },
  {llvm::omp::Directive::OMPD_teams_distribute_parallel_do,
    {
      llvm::omp::allowedClauses_OMPD_teams_distribute_parallel_do,
      llvm::omp::allowedOnceClauses_OMPD_teams_distribute_parallel_do,
      llvm::omp::allowedExclusiveClauses_OMPD_teams_distribute_parallel_do,
      llvm::omp::requiredClauses_OMPD_teams_distribute_parallel_do,
    }
  },
  {llvm::omp::Directive::OMPD_teams_distribute_parallel_do_simd,
    {
      llvm::omp::allowedClauses_OMPD_teams_distribute_parallel_do_simd,
      llvm::omp::allowedOnceClauses_OMPD_teams_distribute_parallel_do_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_teams_distribute_parallel_do_simd,
      llvm::omp::requiredClauses_OMPD_teams_distribute_parallel_do_simd,
    }
  },
  {llvm::omp::Directive::OMPD_teams_distribute_parallel_for,
    {
      llvm::omp::allowedClauses_OMPD_teams_distribute_parallel_for,
      llvm::omp::allowedOnceClauses_OMPD_teams_distribute_parallel_for,
      llvm::omp::allowedExclusiveClauses_OMPD_teams_distribute_parallel_for,
      llvm::omp::requiredClauses_OMPD_teams_distribute_parallel_for,
    }
  },
  {llvm::omp::Directive::OMPD_teams_distribute_parallel_for_simd,
    {
      llvm::omp::allowedClauses_OMPD_teams_distribute_parallel_for_simd,
      llvm::omp::allowedOnceClauses_OMPD_teams_distribute_parallel_for_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_teams_distribute_parallel_for_simd,
      llvm::omp::requiredClauses_OMPD_teams_distribute_parallel_for_simd,
    }
  },
  {llvm::omp::Directive::OMPD_teams_distribute_simd,
    {
      llvm::omp::allowedClauses_OMPD_teams_distribute_simd,
      llvm::omp::allowedOnceClauses_OMPD_teams_distribute_simd,
      llvm::omp::allowedExclusiveClauses_OMPD_teams_distribute_simd,
      llvm::omp::requiredClauses_OMPD_teams_distribute_simd,
    }
  },
  {llvm::omp::Directive::OMPD_threadprivate,
    {
      llvm::omp::allowedClauses_OMPD_threadprivate,
      llvm::omp::allowedOnceClauses_OMPD_threadprivate,
      llvm::omp::allowedExclusiveClauses_OMPD_threadprivate,
      llvm::omp::requiredClauses_OMPD_threadprivate,
    }
  },
  {llvm::omp::Directive::OMPD_unknown,
    {
      llvm::omp::allowedClauses_OMPD_unknown,
      llvm::omp::allowedOnceClauses_OMPD_unknown,
      llvm::omp::allowedExclusiveClauses_OMPD_unknown,
      llvm::omp::requiredClauses_OMPD_unknown,
    }
  },
  {llvm::omp::Directive::OMPD_workshare,
    {
      llvm::omp::allowedClauses_OMPD_workshare,
      llvm::omp::allowedOnceClauses_OMPD_workshare,
      llvm::omp::allowedExclusiveClauses_OMPD_workshare,
      llvm::omp::requiredClauses_OMPD_workshare,
    }
  },
}

#endif // GEN_FLANG_DIRECTIVE_CLAUSE_MAP

#ifdef GEN_FLANG_CLAUSE_PARSER_CLASSES
#undef GEN_FLANG_CLAUSE_PARSER_CLASSES

EMPTY_CLASS(AcqRel);
EMPTY_CLASS(Acquire);
EMPTY_CLASS(Affinity);
WRAPPER_CLASS(Aligned, OmpAlignedClause);
WRAPPER_CLASS(Allocate, OmpAllocateClause);
WRAPPER_CLASS(Allocator, ScalarIntExpr);
EMPTY_CLASS(AtomicDefaultMemOrder);
EMPTY_CLASS(Capture);
WRAPPER_CLASS(Collapse, ScalarIntConstantExpr);
WRAPPER_CLASS(Copyprivate, OmpObjectList);
WRAPPER_CLASS(Copyin, OmpObjectList);
WRAPPER_CLASS(Default, OmpDefaultClause);
WRAPPER_CLASS(Defaultmap, OmpDefaultmapClause);
WRAPPER_CLASS(Depend, OmpDependClause);
EMPTY_CLASS(Depobj);
EMPTY_CLASS(Destroy);
EMPTY_CLASS(Detach);
WRAPPER_CLASS(Device, ScalarIntExpr);
EMPTY_CLASS(DeviceType);
WRAPPER_CLASS(DistSchedule, std::optional<ScalarIntExpr>);
EMPTY_CLASS(DynamicAllocators);
EMPTY_CLASS(Exclusive);
WRAPPER_CLASS(Final, ScalarLogicalExpr);
WRAPPER_CLASS(Firstprivate, OmpObjectList);
EMPTY_CLASS(Flush);
WRAPPER_CLASS(From, OmpObjectList);
WRAPPER_CLASS(Grainsize, ScalarIntExpr);
WRAPPER_CLASS(Hint, ConstantExpr);
WRAPPER_CLASS(If, OmpIfClause);
EMPTY_CLASS(InReduction);
EMPTY_CLASS(Inbranch);
EMPTY_CLASS(Inclusive);
WRAPPER_CLASS(IsDevicePtr, std::list<Name>);
WRAPPER_CLASS(Lastprivate, OmpObjectList);
WRAPPER_CLASS(Linear, OmpLinearClause);
WRAPPER_CLASS(Link, OmpObjectList);
WRAPPER_CLASS(Map, OmpMapClause);
EMPTY_CLASS(Match);
EMPTY_CLASS(Mergeable);
EMPTY_CLASS(Nogroup);
EMPTY_CLASS(Nowait);
EMPTY_CLASS(Nontemporal);
EMPTY_CLASS(Notinbranch);
WRAPPER_CLASS(NumTasks, ScalarIntExpr);
WRAPPER_CLASS(NumTeams, ScalarIntExpr);
WRAPPER_CLASS(NumThreads, ScalarIntExpr);
EMPTY_CLASS(Order);
WRAPPER_CLASS(Ordered, std::optional<ScalarIntConstantExpr>);
WRAPPER_CLASS(Priority, ScalarIntExpr);
WRAPPER_CLASS(Private, OmpObjectList);
WRAPPER_CLASS(ProcBind, OmpProcBindClause);
EMPTY_CLASS(Read);
WRAPPER_CLASS(Reduction, OmpReductionClause);
EMPTY_CLASS(Relaxed);
EMPTY_CLASS(Release);
EMPTY_CLASS(ReverseOffload);
WRAPPER_CLASS(Safelen, ScalarIntConstantExpr);
WRAPPER_CLASS(Schedule, OmpScheduleClause);
EMPTY_CLASS(SeqCst);
WRAPPER_CLASS(Shared, OmpObjectList);
EMPTY_CLASS(Simd);
WRAPPER_CLASS(Simdlen, ScalarIntConstantExpr);
WRAPPER_CLASS(TaskReduction, OmpReductionClause);
WRAPPER_CLASS(ThreadLimit, ScalarIntExpr);
EMPTY_CLASS(Threadprivate);
EMPTY_CLASS(Threads);
WRAPPER_CLASS(To, OmpObjectList);
EMPTY_CLASS(UnifiedAddress);
EMPTY_CLASS(UnifiedSharedMemory);
WRAPPER_CLASS(Uniform, std::list<Name>);
EMPTY_CLASS(Unknown);
EMPTY_CLASS(Untied);
EMPTY_CLASS(Update);
EMPTY_CLASS(UseDeviceAddr);
WRAPPER_CLASS(UseDevicePtr, std::list<Name>);
EMPTY_CLASS(UsesAllocators);
EMPTY_CLASS(Write);

#endif // GEN_FLANG_CLAUSE_PARSER_CLASSES

#ifdef GEN_FLANG_CLAUSE_PARSER_CLASSES_LIST
#undef GEN_FLANG_CLAUSE_PARSER_CLASSES_LIST

AcqRel
, Acquire
, Affinity
, Aligned
, Allocate
, Allocator
, AtomicDefaultMemOrder
, Capture
, Collapse
, Copyprivate
, Copyin
, Default
, Defaultmap
, Depend
, Depobj
, Destroy
, Detach
, Device
, DeviceType
, DistSchedule
, DynamicAllocators
, Exclusive
, Final
, Firstprivate
, Flush
, From
, Grainsize
, Hint
, If
, InReduction
, Inbranch
, Inclusive
, IsDevicePtr
, Lastprivate
, Linear
, Link
, Map
, Match
, Mergeable
, Nogroup
, Nowait
, Nontemporal
, Notinbranch
, NumTasks
, NumTeams
, NumThreads
, Order
, Ordered
, Priority
, Private
, ProcBind
, Read
, Reduction
, Relaxed
, Release
, ReverseOffload
, Safelen
, Schedule
, SeqCst
, Shared
, Simd
, Simdlen
, TaskReduction
, ThreadLimit
, Threadprivate
, Threads
, To
, UnifiedAddress
, UnifiedSharedMemory
, Uniform
, Unknown
, Untied
, Update
, UseDeviceAddr
, UseDevicePtr
, UsesAllocators
, Write

#endif // GEN_FLANG_CLAUSE_PARSER_CLASSES_LIST

#ifdef GEN_FLANG_DUMP_PARSE_TREE_CLAUSES
#undef GEN_FLANG_DUMP_PARSE_TREE_CLAUSES

NODE(OmpClause, AcqRel)
NODE(OmpClause, Acquire)
NODE(OmpClause, Affinity)
NODE(OmpClause, Aligned)
NODE(OmpClause, Allocate)
NODE(OmpClause, Allocator)
NODE(OmpClause, AtomicDefaultMemOrder)
NODE(OmpClause, Capture)
NODE(OmpClause, Collapse)
NODE(OmpClause, Copyprivate)
NODE(OmpClause, Copyin)
NODE(OmpClause, Default)
NODE(OmpClause, Defaultmap)
NODE(OmpClause, Depend)
NODE(OmpClause, Depobj)
NODE(OmpClause, Destroy)
NODE(OmpClause, Detach)
NODE(OmpClause, Device)
NODE(OmpClause, DeviceType)
NODE(OmpClause, DistSchedule)
NODE(OmpClause, DynamicAllocators)
NODE(OmpClause, Exclusive)
NODE(OmpClause, Final)
NODE(OmpClause, Firstprivate)
NODE(OmpClause, Flush)
NODE(OmpClause, From)
NODE(OmpClause, Grainsize)
NODE(OmpClause, Hint)
NODE(OmpClause, If)
NODE(OmpClause, InReduction)
NODE(OmpClause, Inbranch)
NODE(OmpClause, Inclusive)
NODE(OmpClause, IsDevicePtr)
NODE(OmpClause, Lastprivate)
NODE(OmpClause, Linear)
NODE(OmpClause, Link)
NODE(OmpClause, Map)
NODE(OmpClause, Match)
NODE(OmpClause, Mergeable)
NODE(OmpClause, Nogroup)
NODE(OmpClause, Nowait)
NODE(OmpClause, Nontemporal)
NODE(OmpClause, Notinbranch)
NODE(OmpClause, NumTasks)
NODE(OmpClause, NumTeams)
NODE(OmpClause, NumThreads)
NODE(OmpClause, Order)
NODE(OmpClause, Ordered)
NODE(OmpClause, Priority)
NODE(OmpClause, Private)
NODE(OmpClause, ProcBind)
NODE(OmpClause, Read)
NODE(OmpClause, Reduction)
NODE(OmpClause, Relaxed)
NODE(OmpClause, Release)
NODE(OmpClause, ReverseOffload)
NODE(OmpClause, Safelen)
NODE(OmpClause, Schedule)
NODE(OmpClause, SeqCst)
NODE(OmpClause, Shared)
NODE(OmpClause, Simd)
NODE(OmpClause, Simdlen)
NODE(OmpClause, TaskReduction)
NODE(OmpClause, ThreadLimit)
NODE(OmpClause, Threadprivate)
NODE(OmpClause, Threads)
NODE(OmpClause, To)
NODE(OmpClause, UnifiedAddress)
NODE(OmpClause, UnifiedSharedMemory)
NODE(OmpClause, Uniform)
NODE(OmpClause, Unknown)
NODE(OmpClause, Untied)
NODE(OmpClause, Update)
NODE(OmpClause, UseDeviceAddr)
NODE(OmpClause, UseDevicePtr)
NODE(OmpClause, UsesAllocators)
NODE(OmpClause, Write)

#endif // GEN_FLANG_DUMP_PARSE_TREE_CLAUSES

#ifdef GEN_FLANG_CLAUSE_UNPARSE
#undef GEN_FLANG_CLAUSE_UNPARSE

void Before(const OmpClause::AcqRel &) { Word("ACQ_REL"); }
void Before(const OmpClause::Acquire &) { Word("ACQUIRE"); }
void Before(const OmpClause::Affinity &) { Word("AFFINITY"); }
void Unparse(const OmpClause::Aligned &x) {
  Word("ALIGNED");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Allocate &x) {
  Word("ALLOCATE");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Allocator &x) {
  Word("ALLOCATOR");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::AtomicDefaultMemOrder &) { Word("ATOMIC_DEFAULT_MEM_ORDER"); }
void Before(const OmpClause::Capture &) { Word("CAPTURE"); }
void Unparse(const OmpClause::Collapse &x) {
  Word("COLLAPSE");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Copyprivate &x) {
  Word("COPYPRIVATE");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Copyin &x) {
  Word("COPYIN");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Default &x) {
  Word("DEFAULT");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Defaultmap &x) {
  Word("DEFAULTMAP");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Depend &x) {
  Word("DEPEND");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::Depobj &) { Word("DEPOBJ"); }
void Before(const OmpClause::Destroy &) { Word("DESTROY"); }
void Before(const OmpClause::Detach &) { Word("DETACH"); }
void Unparse(const OmpClause::Device &x) {
  Word("DEVICE");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::DeviceType &) { Word("DEVICE_TYPE"); }
void Unparse(const OmpClause::DistSchedule &x) {
  Word("DIST_SCHEDULE");
  Walk("(", x.v, ")");
}
void Before(const OmpClause::DynamicAllocators &) { Word("DYNAMIC_ALLOCATORS"); }
void Before(const OmpClause::Exclusive &) { Word("EXCLUSIVE"); }
void Unparse(const OmpClause::Final &x) {
  Word("FINAL");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Firstprivate &x) {
  Word("FIRSTPRIVATE");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::Flush &) { Word("FLUSH"); }
void Unparse(const OmpClause::From &x) {
  Word("FROM");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Grainsize &x) {
  Word("GRAINSIZE");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Hint &x) {
  Word("HINT");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::If &x) {
  Word("IF");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::InReduction &) { Word("IN_REDUCTION"); }
void Before(const OmpClause::Inbranch &) { Word("INBRANCH"); }
void Before(const OmpClause::Inclusive &) { Word("INCLUSIVE"); }
void Unparse(const OmpClause::IsDevicePtr &x) {
  Word("IS_DEVICE_PTR");
  Put("(");
  Walk(x.v, ",");
  Put(")");
}
void Unparse(const OmpClause::Lastprivate &x) {
  Word("LASTPRIVATE");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Linear &x) {
  Word("LINEAR");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Link &x) {
  Word("LINK");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Map &x) {
  Word("MAP");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::Match &) { Word("MATCH"); }
void Before(const OmpClause::Mergeable &) { Word("MERGEABLE"); }
void Before(const OmpClause::Nogroup &) { Word("NOGROUP"); }
void Before(const OmpClause::Nowait &) { Word("NOWAIT"); }
void Before(const OmpClause::Nontemporal &) { Word("NONTEMPORAL"); }
void Before(const OmpClause::Notinbranch &) { Word("NOTINBRANCH"); }
void Unparse(const OmpClause::NumTasks &x) {
  Word("NUM_TASKS");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::NumTeams &x) {
  Word("NUM_TEAMS");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::NumThreads &x) {
  Word("NUM_THREADS");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::Order &) { Word("ORDER"); }
void Unparse(const OmpClause::Ordered &x) {
  Word("ORDERED");
  Walk("(", x.v, ")");
}
void Unparse(const OmpClause::Priority &x) {
  Word("PRIORITY");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Private &x) {
  Word("PRIVATE");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::ProcBind &x) {
  Word("PROC_BIND");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::Read &) { Word("READ"); }
void Unparse(const OmpClause::Reduction &x) {
  Word("REDUCTION");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::Relaxed &) { Word("RELAXED"); }
void Before(const OmpClause::Release &) { Word("RELEASE"); }
void Before(const OmpClause::ReverseOffload &) { Word("REVERSE_OFFLOAD"); }
void Unparse(const OmpClause::Safelen &x) {
  Word("SAFELEN");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::Schedule &x) {
  Word("SCHEDULE");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::SeqCst &) { Word("SEQ_CST"); }
void Unparse(const OmpClause::Shared &x) {
  Word("SHARED");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::Simd &) { Word("SIMD"); }
void Unparse(const OmpClause::Simdlen &x) {
  Word("SIMDLEN");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::TaskReduction &x) {
  Word("TASK_REDUCTION");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Unparse(const OmpClause::ThreadLimit &x) {
  Word("THREAD_LIMIT");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::Threadprivate &) { Word("THREADPRIVATE"); }
void Before(const OmpClause::Threads &) { Word("THREADS"); }
void Unparse(const OmpClause::To &x) {
  Word("TO");
  Put("(");
  Walk(x.v);
  Put(")");
}
void Before(const OmpClause::UnifiedAddress &) { Word("UNIFIED_ADDRESS"); }
void Before(const OmpClause::UnifiedSharedMemory &) { Word("UNIFIED_SHARED_MEMORY"); }
void Unparse(const OmpClause::Uniform &x) {
  Word("UNIFORM");
  Put("(");
  Walk(x.v, ",");
  Put(")");
}
void Before(const OmpClause::Unknown &) { Word("UNKNOWN"); }
void Before(const OmpClause::Untied &) { Word("UNTIED"); }
void Before(const OmpClause::Update &) { Word("UPDATE"); }
void Before(const OmpClause::UseDeviceAddr &) { Word("USE_DEVICE_ADDR"); }
void Unparse(const OmpClause::UseDevicePtr &x) {
  Word("USE_DEVICE_PTR");
  Put("(");
  Walk(x.v, ",");
  Put(")");
}
void Before(const OmpClause::UsesAllocators &) { Word("USES_ALLOCATORS"); }
void Before(const OmpClause::Write &) { Word("WRITE"); }

#endif // GEN_FLANG_CLAUSE_UNPARSE

#ifdef GEN_CLANG_CLAUSE_CLASS
#undef GEN_CLANG_CLAUSE_CLASS

#ifndef CLAUSE
#define CLAUSE(Enum, Str, Implicit)
#endif
#ifndef CLAUSE_CLASS
#define CLAUSE_CLASS(Enum, Str, Class)
#endif
#ifndef CLAUSE_NO_CLASS
#define CLAUSE_NO_CLASS(Enum, Str)
#endif

#define __CLAUSE(Name, Class)                      \
  CLAUSE(OMPC_##Name, #Name, /* Implicit */ false) \
  CLAUSE_CLASS(OMPC_##Name, #Name, Class)
#define __CLAUSE_NO_CLASS(Name)                    \
  CLAUSE(OMPC_##Name, #Name, /* Implicit */ false) \
  CLAUSE_NO_CLASS(OMPC_##Name, #Name)
#define __IMPLICIT_CLAUSE_CLASS(Name, Str, Class)  \
  CLAUSE(OMPC_##Name, Str, /* Implicit */ true)    \
  CLAUSE_CLASS(OMPC_##Name, Str, Class)
#define __IMPLICIT_CLAUSE_NO_CLASS(Name, Str)      \
  CLAUSE(OMPC_##Name, Str, /* Implicit */ true)    \
  CLAUSE_NO_CLASS(OMPC_##Name, Str)

__CLAUSE(acq_rel, OMPAcqRelClause)
__CLAUSE(acquire, OMPAcquireClause)
__CLAUSE(affinity, OMPAffinityClause)
__CLAUSE(aligned, OMPAlignedClause)
__CLAUSE(allocate, OMPAllocateClause)
__CLAUSE(allocator, OMPAllocatorClause)
__CLAUSE(atomic_default_mem_order, OMPAtomicDefaultMemOrderClause)
__CLAUSE(capture, OMPCaptureClause)
__CLAUSE(collapse, OMPCollapseClause)
__CLAUSE(copyprivate, OMPCopyprivateClause)
__CLAUSE(copyin, OMPCopyinClause)
__CLAUSE(default, OMPDefaultClause)
__CLAUSE(defaultmap, OMPDefaultmapClause)
__CLAUSE(depend, OMPDependClause)
__IMPLICIT_CLAUSE_CLASS(depobj, "depobj", OMPDepobjClause)
__CLAUSE(destroy, OMPDestroyClause)
__CLAUSE(detach, OMPDetachClause)
__CLAUSE(device, OMPDeviceClause)
__CLAUSE_NO_CLASS(device_type)
__CLAUSE(dist_schedule, OMPDistScheduleClause)
__CLAUSE(dynamic_allocators, OMPDynamicAllocatorsClause)
__CLAUSE(exclusive, OMPExclusiveClause)
__CLAUSE(final, OMPFinalClause)
__CLAUSE(firstprivate, OMPFirstprivateClause)
__IMPLICIT_CLAUSE_CLASS(flush, "flush", OMPFlushClause)
__CLAUSE(from, OMPFromClause)
__CLAUSE(grainsize, OMPGrainsizeClause)
__CLAUSE(hint, OMPHintClause)
__CLAUSE(if, OMPIfClause)
__CLAUSE(in_reduction, OMPInReductionClause)
__CLAUSE_NO_CLASS(inbranch)
__CLAUSE(inclusive, OMPInclusiveClause)
__CLAUSE(is_device_ptr, OMPIsDevicePtrClause)
__CLAUSE(lastprivate, OMPLastprivateClause)
__CLAUSE(linear, OMPLinearClause)
__CLAUSE_NO_CLASS(link)
__CLAUSE(map, OMPMapClause)
__CLAUSE_NO_CLASS(match)
__CLAUSE(mergeable, OMPMergeableClause)
__CLAUSE(nogroup, OMPNogroupClause)
__CLAUSE(nowait, OMPNowaitClause)
__CLAUSE(nontemporal, OMPNontemporalClause)
__CLAUSE_NO_CLASS(notinbranch)
__CLAUSE(num_tasks, OMPNumTasksClause)
__CLAUSE(num_teams, OMPNumTeamsClause)
__CLAUSE(num_threads, OMPNumThreadsClause)
__CLAUSE(order, OMPOrderClause)
__CLAUSE(ordered, OMPOrderedClause)
__CLAUSE(priority, OMPPriorityClause)
__CLAUSE(private, OMPPrivateClause)
__CLAUSE(proc_bind, OMPProcBindClause)
__CLAUSE(read, OMPReadClause)
__CLAUSE(reduction, OMPReductionClause)
__CLAUSE(relaxed, OMPRelaxedClause)
__CLAUSE(release, OMPReleaseClause)
__CLAUSE(reverse_offload, OMPReverseOffloadClause)
__CLAUSE(safelen, OMPSafelenClause)
__CLAUSE(schedule, OMPScheduleClause)
__CLAUSE(seq_cst, OMPSeqCstClause)
__CLAUSE(shared, OMPSharedClause)
__CLAUSE(simd, OMPSIMDClause)
__CLAUSE(simdlen, OMPSimdlenClause)
__CLAUSE(task_reduction, OMPTaskReductionClause)
__CLAUSE(thread_limit, OMPThreadLimitClause)
__IMPLICIT_CLAUSE_NO_CLASS(threadprivate, "threadprivate")
__CLAUSE(threads, OMPThreadsClause)
__CLAUSE(to, OMPToClause)
__CLAUSE(unified_address, OMPUnifiedAddressClause)
__CLAUSE(unified_shared_memory, OMPUnifiedSharedMemoryClause)
__CLAUSE_NO_CLASS(uniform)
__IMPLICIT_CLAUSE_NO_CLASS(unknown, "unknown")
__CLAUSE(untied, OMPUntiedClause)
__CLAUSE(update, OMPUpdateClause)
__CLAUSE(use_device_addr, OMPUseDeviceAddrClause)
__CLAUSE(use_device_ptr, OMPUseDevicePtrClause)
__CLAUSE(uses_allocators, OMPUsesAllocatorsClause)
__CLAUSE(write, OMPWriteClause)

#undef __IMPLICIT_CLAUSE_NO_CLASS
#undef __IMPLICIT_CLAUSE_CLASS
#undef __CLAUSE
#undef CLAUSE_NO_CLASS
#undef CLAUSE_CLASS
#undef CLAUSE

#endif // GEN_CLANG_CLAUSE_CLASS

