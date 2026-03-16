LIBRARY()

VERSION(1.10.1)

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/libs/llvm8/include
    contrib/libs/llvm8/lib/Analysis
    contrib/libs/llvm8/lib/AsmParser
    contrib/libs/llvm8/lib/BinaryFormat
    contrib/libs/llvm8/lib/Bitcode/Reader
    contrib/libs/llvm8/lib/Bitcode/Writer
    contrib/libs/llvm8/lib/CodeGen
    contrib/libs/llvm8/lib/CodeGen/AsmPrinter
    contrib/libs/llvm8/lib/CodeGen/GlobalISel
    contrib/libs/llvm8/lib/CodeGen/SelectionDAG
    contrib/libs/llvm8/lib/DebugInfo/CodeView
    contrib/libs/llvm8/lib/DebugInfo/MSF
    contrib/libs/llvm8/lib/Demangle
    contrib/libs/llvm8/lib/ExecutionEngine
    contrib/libs/llvm8/lib/ExecutionEngine/Orc
    contrib/libs/llvm8/lib/ExecutionEngine/RuntimeDyld
    contrib/libs/llvm8/lib/IR
    contrib/libs/llvm8/lib/IRReader
    contrib/libs/llvm8/lib/Linker
    contrib/libs/llvm8/lib/MC
    contrib/libs/llvm8/lib/MC/MCDisassembler
    contrib/libs/llvm8/lib/MC/MCParser
    contrib/libs/llvm8/lib/Object
    contrib/libs/llvm8/lib/ProfileData
    contrib/libs/llvm8/lib/Support
    contrib/libs/llvm8/lib/Target/NVPTX
    contrib/libs/llvm8/lib/Target/X86
    contrib/libs/llvm8/lib/Target/X86/Disassembler
    contrib/libs/llvm8/lib/Transforms/IPO
    contrib/libs/llvm8/lib/Transforms/InstCombine
    contrib/libs/llvm8/lib/Transforms/Instrumentation
    contrib/libs/llvm8/lib/Transforms/ObjCARC
    contrib/libs/llvm8/lib/Transforms/Scalar
    contrib/libs/llvm8/lib/Transforms/Utils
    contrib/libs/llvm8/lib/Transforms/Vectorize
    contrib/libs/eigen
    contrib/libs/re2
    contrib/deprecated/tf/proto
)

ADDINCL(
    contrib/libs/eigen
    contrib/deprecated/tf
)

CFLAGS(
    -DEIGEN_AVOID_STL_ARRAY
    -DEIGEN_MPL2_ONLY
    -DGRPC_ARES=0
    -DPB_FIELD_16BIT=1
    -DSQLITE_OMIT_DEPRECATED
    -DSQLITE_OMIT_LOAD_EXTENSION
    -DTENSORFLOW_EAGER_USE_XLA
    -DTF_USE_SNAPPY
    -D_FORCE_INLINES
)

IF (TENSORFLOW_WITH_CUDA)
    CFLAGS(
        -DGOOGLE_CUDA=1
        -I$CUDA_ROOT/extras/CUPTI/include
    )
    CUDA_NVCC_FLAGS(
        -gencode
        arch=compute_61,code=sm_61
        -gencode
        arch=compute_70,code=sm_70
        --expt-relaxed-constexpr
        --ftz=true
    )
ENDIF()

SRCDIR(contrib/deprecated/tf)

SRCS(
    tensorflow/compiler/jit/build_xla_launch_ops_pass.cc
    tensorflow/compiler/jit/defs.cc
    tensorflow/compiler/jit/encapsulate_subgraphs_pass.cc
    tensorflow/compiler/jit/graphcycles/graphcycles.cc
    tensorflow/compiler/jit/legacy_flags/parallel_check_op_flags.cc
    tensorflow/compiler/jit/legacy_flags/xla_device_flags.cc
    tensorflow/compiler/jit/mark_for_compilation_pass.cc
    tensorflow/compiler/jit/shape_inference_helpers.cc
    tensorflow/compiler/jit/xla_cluster_util.cc
    tensorflow/compiler/jit/xla_compilation_cache.cc
    tensorflow/compiler/jit/xla_compile_on_demand_op.cc
    tensorflow/compiler/jit/xla_device.cc
    tensorflow/compiler/jit/xla_device_context.cc
    tensorflow/compiler/jit/xla_device_ops.cc
    tensorflow/compiler/jit/xla_launch_util.cc
    tensorflow/compiler/jit/xla_tensor.cc
    tensorflow/compiler/tf2xla/const_analysis.cc
    tensorflow/compiler/tf2xla/dump_graph.cc
    tensorflow/compiler/tf2xla/dump_graph_flags.cc
    tensorflow/compiler/tf2xla/functionalize_control_flow.cc
    tensorflow/compiler/tf2xla/graph_compiler.cc
    tensorflow/compiler/tf2xla/kernels/cwise_ops.cc
    tensorflow/compiler/tf2xla/kernels/reduction_ops_common.cc
    tensorflow/compiler/tf2xla/kernels/shape_util.cc
    tensorflow/compiler/tf2xla/legacy_flags/backend_registration_flags.cc
    tensorflow/compiler/tf2xla/lib/batch_dot.cc
    tensorflow/compiler/tf2xla/lib/cholesky.cc
    tensorflow/compiler/tf2xla/lib/qr.cc
    tensorflow/compiler/tf2xla/lib/random.cc
    tensorflow/compiler/tf2xla/lib/scatter.cc
    tensorflow/compiler/tf2xla/lib/triangular_solve.cc
    tensorflow/compiler/tf2xla/lib/util.cc
    tensorflow/compiler/tf2xla/lib/while_loop.cc
    tensorflow/compiler/tf2xla/literal_util.cc
    tensorflow/compiler/tf2xla/shape_util.cc
    tensorflow/compiler/tf2xla/sharding_util.cc
    tensorflow/compiler/tf2xla/str_util.cc
    tensorflow/compiler/tf2xla/tf2xla_util.cc
    tensorflow/compiler/tf2xla/type_util.cc
    tensorflow/compiler/tf2xla/xla_compilation_device.cc
    tensorflow/compiler/tf2xla/xla_compiler.cc
    tensorflow/compiler/tf2xla/xla_context.cc
    tensorflow/compiler/tf2xla/xla_helpers.cc
    tensorflow/compiler/tf2xla/xla_op_kernel.cc
    tensorflow/compiler/tf2xla/xla_op_registry.cc
    tensorflow/compiler/tf2xla/xla_resource.cc
    tensorflow/compiler/xla/client/client.cc
    tensorflow/compiler/xla/client/client_library.cc
    tensorflow/compiler/xla/client/compile_only_client.cc
    tensorflow/compiler/xla/client/executable_build_options.cc
    tensorflow/compiler/xla/client/global_data.cc
    tensorflow/compiler/xla/client/lib/arithmetic.cc
    tensorflow/compiler/xla/client/lib/constants.cc
    tensorflow/compiler/xla/client/lib/math.cc
    tensorflow/compiler/xla/client/lib/numeric.cc
    tensorflow/compiler/xla/client/lib/prng.cc
    tensorflow/compiler/xla/client/local_client.cc
    tensorflow/compiler/xla/client/padding.cc
    tensorflow/compiler/xla/client/sharding_builder.cc
    tensorflow/compiler/xla/client/xla_client/xla_builder.cc
    tensorflow/compiler/xla/client/xla_client/xla_computation.cc
    tensorflow/compiler/xla/executable_run_options.cc
    tensorflow/compiler/xla/execution_options_util.cc
    tensorflow/compiler/xla/index_util.cc
    tensorflow/compiler/xla/layout_util.cc
    tensorflow/compiler/xla/legacy_flags/debug_options_flags.cc
    tensorflow/compiler/xla/literal.cc
    tensorflow/compiler/xla/literal_util.cc
    tensorflow/compiler/xla/metric_table_report.cc
    tensorflow/compiler/xla/primitive_util.cc
    tensorflow/compiler/xla/protobuf_util.cc
    tensorflow/compiler/xla/service/algebraic_simplifier.cc
    tensorflow/compiler/xla/service/allocation_tracker.cc
    tensorflow/compiler/xla/service/backend.cc
    tensorflow/compiler/xla/service/batch_dot_simplification.cc
    tensorflow/compiler/xla/service/batchnorm_expander.cc
    tensorflow/compiler/xla/service/buffer_assignment.cc
    tensorflow/compiler/xla/service/buffer_liveness.cc
    tensorflow/compiler/xla/service/buffer_value.cc
    tensorflow/compiler/xla/service/call_graph.cc
    tensorflow/compiler/xla/service/call_inliner.cc
    tensorflow/compiler/xla/service/channel_tracker.cc
    tensorflow/compiler/xla/service/compile_only_service.cc
    tensorflow/compiler/xla/service/compiler.cc
    tensorflow/compiler/xla/service/computation_layout.cc
    tensorflow/compiler/xla/service/conditional_simplifier.cc
    tensorflow/compiler/xla/service/copy_insertion.cc
    tensorflow/compiler/xla/service/cpu/compiler_functor.cc
    tensorflow/compiler/xla/service/cpu/conv_canonicalization.cc
    tensorflow/compiler/xla/service/cpu/cpu_copy_insertion.cc
    tensorflow/compiler/xla/service/cpu/cpu_executable.cc
    tensorflow/compiler/xla/service/cpu/cpu_hlo_support_checker.cc
    tensorflow/compiler/xla/service/cpu/cpu_instruction_fusion.cc
    tensorflow/compiler/xla/service/cpu/cpu_layout_assignment.cc
    tensorflow/compiler/xla/service/cpu/cpu_options.cc
    tensorflow/compiler/xla/service/cpu/cpu_runtime.cc
    tensorflow/compiler/xla/service/cpu/custom_call_target_registry.cc
    tensorflow/compiler/xla/service/cpu/disassembler.cc
    tensorflow/compiler/xla/service/cpu/dot_op_emitter.cc
    tensorflow/compiler/xla/service/cpu/elemental_ir_emitter.cc
    tensorflow/compiler/xla/service/cpu/ir_emission_utils.cc
    tensorflow/compiler/xla/service/cpu/ir_emitter.cc
    tensorflow/compiler/xla/service/cpu/ir_function.cc
    tensorflow/compiler/xla/service/cpu/llvm_ir_runtime.cc
    tensorflow/compiler/xla/service/cpu/orc_jit_memory_mapper.cc
    tensorflow/compiler/xla/service/cpu/parallel_loop_emitter.cc
    tensorflow/compiler/xla/service/cpu/parallel_task_assignment.cc
    tensorflow/compiler/xla/service/cpu/runtime_conv2d.cc
    tensorflow/compiler/xla/service/cpu/runtime_conv2d_mkl.cc
    tensorflow/compiler/xla/service/cpu/runtime_fft.cc
    tensorflow/compiler/xla/service/cpu/runtime_fork_join.cc
    tensorflow/compiler/xla/service/cpu/runtime_fp16.cc
    tensorflow/compiler/xla/service/cpu/runtime_matmul.cc
    tensorflow/compiler/xla/service/cpu/runtime_matmul_mkl.cc
    tensorflow/compiler/xla/service/cpu/runtime_single_threaded_conv2d.cc
    tensorflow/compiler/xla/service/cpu/runtime_single_threaded_fft.cc
    tensorflow/compiler/xla/service/cpu/runtime_single_threaded_matmul.cc
    tensorflow/compiler/xla/service/cpu/shape_partition.cc
    tensorflow/compiler/xla/service/cpu/target_machine_features.cc
    tensorflow/compiler/xla/service/cpu/vector_support_library.cc
    tensorflow/compiler/xla/service/cpu/windows_compatibility.cc
    tensorflow/compiler/xla/service/cpu/xfeed_manager.cc
    tensorflow/compiler/xla/service/device_memory_allocator.cc
    tensorflow/compiler/xla/service/dfs_hlo_visitor.cc
    tensorflow/compiler/xla/service/dot_decomposer.cc
    tensorflow/compiler/xla/service/elemental_ir_emitter.cc
    tensorflow/compiler/xla/service/executable.cc
    tensorflow/compiler/xla/service/execution_tracker.cc
    tensorflow/compiler/xla/service/flatten_call_graph.cc
    tensorflow/compiler/xla/service/generic_transfer_manager.cc
    tensorflow/compiler/xla/service/gpu/parallel_loop_emitter.cc
    tensorflow/compiler/xla/service/gpu/partition_assignment.cc
    tensorflow/compiler/xla/service/heap_simulator.cc
    tensorflow/compiler/xla/service/hlo_alias_analysis.cc
    tensorflow/compiler/xla/service/hlo_buffer.cc
    tensorflow/compiler/xla/service/hlo_computation.cc
    tensorflow/compiler/xla/service/hlo_constant_folding.cc
    tensorflow/compiler/xla/service/hlo_cost_analysis.cc
    tensorflow/compiler/xla/service/hlo_creation_utils.cc
    tensorflow/compiler/xla/service/hlo_cse.cc
    tensorflow/compiler/xla/service/hlo_dataflow_analysis.cc
    tensorflow/compiler/xla/service/hlo_dce.cc
    tensorflow/compiler/xla/service/hlo_domain_map.cc
    tensorflow/compiler/xla/service/hlo_element_type_converter.cc
    tensorflow/compiler/xla/service/hlo_evaluator.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_bfloat16.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_bool.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_complex64.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_double.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_float.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_half.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_int32.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_int64.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_int8.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_uint32.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_uint64.cc
    tensorflow/compiler/xla/service/hlo_evaluator_typed_visitor_uint8.cc
    tensorflow/compiler/xla/service/hlo_execution_profile.cc
    tensorflow/compiler/xla/service/hlo_graph_dumper.cc
    tensorflow/compiler/xla/service/hlo_instruction.cc
    tensorflow/compiler/xla/service/hlo_instructions.cc
    tensorflow/compiler/xla/service/hlo_module.cc
    tensorflow/compiler/xla/service/hlo_module_config.cc
    tensorflow/compiler/xla/service/hlo_opcode.cc
    tensorflow/compiler/xla/service/hlo_ordering.cc
    tensorflow/compiler/xla/service/hlo_pass_pipeline.cc
    tensorflow/compiler/xla/service/hlo_profile_printer.cc
    tensorflow/compiler/xla/service/hlo_proto_util.cc
    tensorflow/compiler/xla/service/hlo_query.cc
    tensorflow/compiler/xla/service/hlo_reachability.cc
    tensorflow/compiler/xla/service/hlo_scheduling.cc
    tensorflow/compiler/xla/service/hlo_sharding.cc
    tensorflow/compiler/xla/service/hlo_subcomputation_unification.cc
    tensorflow/compiler/xla/service/hlo_tfgraph_builder.cc
    tensorflow/compiler/xla/service/hlo_value.cc
    tensorflow/compiler/xla/service/hlo_verifier.cc
    tensorflow/compiler/xla/service/human_readable_profile_builder.cc
    tensorflow/compiler/xla/service/indexed_array_analysis.cc
    tensorflow/compiler/xla/service/inliner.cc
    tensorflow/compiler/xla/service/instruction_fusion.cc
    tensorflow/compiler/xla/service/interpreter/platform_id.cc
    tensorflow/compiler/xla/service/layout_assignment.cc
    tensorflow/compiler/xla/service/llvm_compiler.cc
    tensorflow/compiler/xla/service/llvm_ir/alias_analysis.cc
    tensorflow/compiler/xla/service/llvm_ir/fused_ir_emitter.cc
    tensorflow/compiler/xla/service/llvm_ir/ir_array.cc
    tensorflow/compiler/xla/service/llvm_ir/kernel_support_library.cc
    tensorflow/compiler/xla/service/llvm_ir/kernel_tiling.cc
    tensorflow/compiler/xla/service/llvm_ir/llvm_loop.cc
    tensorflow/compiler/xla/service/llvm_ir/llvm_util.cc
    tensorflow/compiler/xla/service/llvm_ir/loop_emitter.cc
    tensorflow/compiler/xla/service/llvm_ir/ops.cc
    tensorflow/compiler/xla/service/llvm_ir/tuple_ops.cc
    tensorflow/compiler/xla/service/local_service.cc
    tensorflow/compiler/xla/service/logical_buffer.cc
    tensorflow/compiler/xla/service/logical_buffer_analysis.cc
    tensorflow/compiler/xla/service/name_uniquer.cc
    tensorflow/compiler/xla/service/owning_device_memory.cc
    tensorflow/compiler/xla/service/platform_util.cc
    tensorflow/compiler/xla/service/reduce_precision_insertion.cc
    tensorflow/compiler/xla/service/reshape_mover.cc
    tensorflow/compiler/xla/service/service.cc
    tensorflow/compiler/xla/service/shape_inference.cc
    tensorflow/compiler/xla/service/shaped_buffer.cc
    tensorflow/compiler/xla/service/source_map_util.cc
    tensorflow/compiler/xla/service/transfer_manager.cc
    tensorflow/compiler/xla/service/transpose_folding.cc
    tensorflow/compiler/xla/service/tuple_points_to_analysis.cc
    tensorflow/compiler/xla/service/tuple_simplifier.cc
    tensorflow/compiler/xla/service/tuple_util.cc
    tensorflow/compiler/xla/service/while_loop_constant_sinking.cc
    tensorflow/compiler/xla/service/while_loop_invariant_code_motion.cc
    tensorflow/compiler/xla/service/while_loop_simplifier.cc
    tensorflow/compiler/xla/service/while_util.cc
    tensorflow/compiler/xla/service/zero_sized_hlo_elimination.cc
    tensorflow/compiler/xla/shape_layout.cc
    tensorflow/compiler/xla/shape_util.cc
    tensorflow/compiler/xla/sparse_index_array.cc
    tensorflow/compiler/xla/status_macros.cc
    tensorflow/compiler/xla/util.cc
    tensorflow/compiler/xla/window_util.cc
    tensorflow/core/platform/default/human_readable_json.cc
    GLOBAL tensorflow/compiler/jit/jit_compilation_pass_registration.cc
    GLOBAL tensorflow/compiler/jit/kernels/parallel_check_op.cc
    GLOBAL tensorflow/compiler/jit/kernels/xla_launch_op.cc
    GLOBAL tensorflow/compiler/jit/ops/parallel_check_op.cc
    GLOBAL tensorflow/compiler/jit/ops/xla_ops.cc
    GLOBAL tensorflow/compiler/jit/xla_cpu_device.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/aggregate_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/arg_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/batch_matmul_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/batch_norm_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/batchtospace_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/bcast_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/bias_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/binary_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/bucketize_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/cast_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/categorical_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/cholesky_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/clip_by_value_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/concat_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/const_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/conv_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/cross_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/depthtospace_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/diag_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/dynamic_slice_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/dynamic_stitch_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/elu_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/extract_image_patches_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/fake_quantize_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/fft_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/fill_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/function_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/gather_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/identity_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/if_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/image_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/image_resize_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/index_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/l2loss_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/listdiff_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/lrn_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/matmul_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/matrix_band_part_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/matrix_set_diag_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/matrix_triangular_solve_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/mirror_pad_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/no_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/one_hot_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/pack_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/pad_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/pooling_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/qr_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/quantize_and_dequantize_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/random_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/reduce_window_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/reduction_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/relu_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/reshape_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/retval_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/reverse_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/reverse_sequence_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/scan_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/scatter_nd_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/segment_reduction_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/select_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/sendrecv_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/sequence_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/shape_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/slice_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/softmax_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/sort_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/spacetobatch_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/spacetodepth_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/sparse_to_dense_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/split_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/stack_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/stateless_random_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/strided_slice_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/tensor_array_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/tile_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/topk_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/training_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/transpose_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/unary_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/unpack_op.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/variable_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/kernels/while_op.cc
    GLOBAL tensorflow/compiler/tf2xla/ops/xla_ops.cc
    GLOBAL tensorflow/compiler/tf2xla/xla_cpu_backend.cc
    GLOBAL tensorflow/compiler/xla/service/computation_placer.cc
    GLOBAL tensorflow/compiler/xla/service/cpu/cpu_compiler.cc
    GLOBAL tensorflow/compiler/xla/service/cpu/cpu_transfer_manager.cc
    GLOBAL tensorflow/compiler/xla/service/cpu/simple_orc_jit.cc
)

IF (TENSORFLOW_WITH_CUDA)
    SRCS(
        tensorflow/compiler/xla/service/gpu/buffer_allocations.cc
        tensorflow/compiler/xla/service/gpu/conditional_thunk.cc
        tensorflow/compiler/xla/service/gpu/convolution_thunk.cc
        tensorflow/compiler/xla/service/gpu/copy_thunk.cc
        tensorflow/compiler/xla/service/gpu/cudnn_batchnorm_rewriter.cc
        tensorflow/compiler/xla/service/gpu/cudnn_batchnorm_thunk.cc
        tensorflow/compiler/xla/service/gpu/cudnn_convolution_algorithm_picker.cc
        tensorflow/compiler/xla/service/gpu/cudnn_convolution_rewriter.cc
        tensorflow/compiler/xla/service/gpu/cudnn_convolution_runner.cc
        tensorflow/compiler/xla/service/gpu/elemental_ir_emitter.cc
        tensorflow/compiler/xla/service/gpu/fft_thunk.cc
        tensorflow/compiler/xla/service/gpu/for_thunk.cc
        tensorflow/compiler/xla/service/gpu/fusion_merger.cc
        tensorflow/compiler/xla/service/gpu/gemm_thunk.cc
        tensorflow/compiler/xla/service/gpu/gpu_constants.cc
        tensorflow/compiler/xla/service/gpu/gpu_copy_insertion.cc
        tensorflow/compiler/xla/service/gpu/gpu_executable.cc
        tensorflow/compiler/xla/service/gpu/gpu_hlo_support_checker.cc
        tensorflow/compiler/xla/service/gpu/gpu_layout_assignment.cc
        tensorflow/compiler/xla/service/gpu/gpu_options.cc
        tensorflow/compiler/xla/service/gpu/hlo_execution_profiler.cc
        tensorflow/compiler/xla/service/gpu/hlo_schedule.cc
        tensorflow/compiler/xla/service/gpu/hlo_to_ir_bindings.cc
        tensorflow/compiler/xla/service/gpu/infeed_manager.cc
        tensorflow/compiler/xla/service/gpu/infeed_thunk.cc
        tensorflow/compiler/xla/service/gpu/instruction_fusion.cc
        tensorflow/compiler/xla/service/gpu/ir_emission_utils.cc
        tensorflow/compiler/xla/service/gpu/ir_emitter.cc
        tensorflow/compiler/xla/service/gpu/ir_emitter_nested.cc
        tensorflow/compiler/xla/service/gpu/ir_emitter_unnested.cc
        tensorflow/compiler/xla/service/gpu/kernel_thunk.cc
        tensorflow/compiler/xla/service/gpu/llvm_gpu_backend/dump_ir_pass.cc
        tensorflow/compiler/xla/service/gpu/llvm_gpu_backend/gpu_backend_lib.cc
        tensorflow/compiler/xla/service/gpu/llvm_gpu_backend/utils.cc
        tensorflow/compiler/xla/service/gpu/memset_thunk.cc
        tensorflow/compiler/xla/service/gpu/multi_output_fusion.cc
        tensorflow/compiler/xla/service/gpu/outfeed_manager.cc
        tensorflow/compiler/xla/service/gpu/outfeed_thunk.cc
        tensorflow/compiler/xla/service/gpu/pad_insertion.cc
        tensorflow/compiler/xla/service/gpu/sequential_thunk.cc
        tensorflow/compiler/xla/service/gpu/stream_assignment.cc
        tensorflow/compiler/xla/service/gpu/stream_executor_util.cc
        tensorflow/compiler/xla/service/gpu/thunk_schedule.cc
        tensorflow/compiler/xla/service/gpu/tuple_thunk.cc
        tensorflow/compiler/xla/service/gpu/while_thunk.cc
        tensorflow/compiler/xla/service/gpu/while_transformer.cc
        tensorflow/compiler/xla/service/multi_output_fusion.cc
        tensorflow/core/platform/cuda_libdevice_path.cc
        tensorflow/core/platform/default/cuda_libdevice_path.cc
        GLOBAL tensorflow/compiler/jit/xla_gpu_device.cc
        GLOBAL tensorflow/compiler/tf2xla/xla_gpu_backend.cc
        GLOBAL tensorflow/compiler/xla/service/gpu/gpu_compiler.cc
        GLOBAL tensorflow/compiler/xla/service/gpu/gpu_transfer_manager.cc
    )
ENDIF()

END()
