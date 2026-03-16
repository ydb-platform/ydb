LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(1.10.1)

LICENSE(Apache-2.0)

PROVIDES(tensorflow_all_ops)

NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/libs/eigen
    contrib/libs/farmhash
    contrib/libs/giflib
    contrib/libs/grpc
    contrib/libs/highwayhash
    contrib/libs/libpng
    contrib/libs/nsync
    contrib/libs/opencv
    contrib/libs/protobuf
    contrib/libs/re2
    contrib/libs/snappy
    contrib/libs/sqlite3
    contrib/deprecated/tf/grpc_proto
    contrib/deprecated/tf/proto
    contrib/libs/libjpeg-turbo
    contrib/libs/zlib
)

IF (TENSORFLOW_WITH_XLA)
    PEERDIR(
        contrib/deprecated/tf/tensorflow/compiler
    )
ENDIF()

ADDINCL(
    contrib/libs/eigen
    contrib/libs/farmhash
    contrib/libs/giflib
    contrib/libs/highwayhash
    contrib/libs/libpng
    contrib/libs/sqlite3
    contrib/libs/libjpeg-turbo
    contrib/restricted/google/gemmlowp
    GLOBAL contrib/deprecated/tf
)

INCLUDE(${ARCADIA_ROOT}/contrib/deprecated/tf/cuda.inc)

CFLAGS(
    -DEIGEN_AVOID_STL_ARRAY
    -DEIGEN_MPL2_ONLY
    -DGRPC_ARES=0
    -DPB_FIELD_16BIT=1
    -DSQLITE_OMIT_DEPRECATED
    -DSQLITE_OMIT_LOAD_EXTENSION
    -DTF_USE_SNAPPY
    -D_FORCE_INLINES
)

IF (TENSORFLOW_WITH_XLA)
    CFLAGS(
        -DTENSORFLOW_EAGER_USE_XLA
    )
ENDIF()

SRCDIR(contrib/deprecated/tf)

SRCS(
    GLOBAL tensorflow/cc/gradients/array_grad.cc
    GLOBAL tensorflow/cc/gradients/data_flow_grad.cc
    GLOBAL tensorflow/cc/gradients/math_grad.cc
    GLOBAL tensorflow/cc/gradients/nn_grad.cc
    GLOBAL tensorflow/contrib/data/kernels/csv_dataset_op.cc
    GLOBAL tensorflow/contrib/data/kernels/directed_interleave_dataset_op.cc
    GLOBAL tensorflow/contrib/data/kernels/ignore_errors_dataset_op.cc
    GLOBAL tensorflow/contrib/data/kernels/prefetching_kernels.cc
    GLOBAL tensorflow/contrib/data/kernels/threadpool_dataset_op.cc
    GLOBAL tensorflow/contrib/data/kernels/unique_dataset_op.cc
    GLOBAL tensorflow/contrib/data/ops/dataset_ops.cc
    GLOBAL tensorflow/contrib/image/kernels/adjust_hsv_in_yiq_op.cc
    GLOBAL tensorflow/contrib/image/kernels/bipartite_match_op.cc
    GLOBAL tensorflow/contrib/image/kernels/image_ops.cc
    GLOBAL tensorflow/contrib/image/kernels/segmentation_ops.cc
    GLOBAL tensorflow/contrib/image/kernels/single_image_random_dot_stereograms_ops.cc
    GLOBAL tensorflow/contrib/image/ops/distort_image_ops.cc
    GLOBAL tensorflow/contrib/image/ops/image_ops.cc
    GLOBAL tensorflow/contrib/image/ops/single_image_random_dot_stereograms_ops.cc
    GLOBAL tensorflow/contrib/memory_stats/kernels/memory_stats_ops.cc
    GLOBAL tensorflow/contrib/memory_stats/ops/memory_stats_ops.cc
    GLOBAL tensorflow/contrib/seq2seq/kernels/beam_search_ops.cc
    GLOBAL tensorflow/contrib/seq2seq/ops/beam_search_ops.cc
    GLOBAL tensorflow/contrib/tpu/ops/cross_replica_ops.cc
    GLOBAL tensorflow/contrib/tpu/ops/heartbeat_ops.cc
    GLOBAL tensorflow/contrib/tpu/ops/host_compute_ops.cc
    GLOBAL tensorflow/contrib/tpu/ops/infeed_ops.cc
    GLOBAL tensorflow/contrib/tpu/ops/outfeed_ops.cc
    GLOBAL tensorflow/contrib/tpu/ops/replication_ops.cc
    GLOBAL tensorflow/contrib/tpu/ops/tpu_configuration_ops.cc
    GLOBAL tensorflow/contrib/tpu/ops/tpu_embedding_ops.cc
    GLOBAL tensorflow/core/common_runtime/accumulate_n_optimizer.cc
    GLOBAL tensorflow/core/common_runtime/direct_session.cc
    GLOBAL tensorflow/core/common_runtime/gpu/gpu_device_factory.cc
    GLOBAL tensorflow/core/common_runtime/lower_if_op.cc
    GLOBAL tensorflow/core/common_runtime/parallel_concat_optimizer.cc
    GLOBAL tensorflow/core/common_runtime/sycl/sycl_device_factory.cc
    GLOBAL tensorflow/core/common_runtime/threadpool_device.cc
    GLOBAL tensorflow/core/common_runtime/threadpool_device_factory.cc
    GLOBAL tensorflow/core/graph/mkl_layout_pass.cc
    GLOBAL tensorflow/core/graph/mkl_tfconversion_pass.cc
    GLOBAL tensorflow/core/grappler/optimizers/data/function_rename.cc
    GLOBAL tensorflow/core/grappler/optimizers/data/map_and_batch_fusion.cc
    GLOBAL tensorflow/core/grappler/optimizers/data/noop_elimination.cc
    GLOBAL tensorflow/core/grappler/optimizers/data/shuffle_and_repeat_fusion.cc
    GLOBAL tensorflow/core/grappler/optimizers/gpu_swapping_kernels.cc
    GLOBAL tensorflow/core/grappler/optimizers/gpu_swapping_ops.cc
    GLOBAL tensorflow/core/kernels/adjust_contrast_op.cc
    GLOBAL tensorflow/core/kernels/adjust_hue_op.cc
    GLOBAL tensorflow/core/kernels/adjust_saturation_op.cc
    GLOBAL tensorflow/core/kernels/aggregate_ops.cc
    GLOBAL tensorflow/core/kernels/argmax_op.cc
    GLOBAL tensorflow/core/kernels/as_string_op.cc
    GLOBAL tensorflow/core/kernels/attention_ops.cc
    GLOBAL tensorflow/core/kernels/avgpooling_op.cc
    GLOBAL tensorflow/core/kernels/barrier_ops.cc
    GLOBAL tensorflow/core/kernels/base64_ops.cc
    GLOBAL tensorflow/core/kernels/batch_kernels.cc
    GLOBAL tensorflow/core/kernels/batch_matmul_op_complex.cc
    GLOBAL tensorflow/core/kernels/batch_matmul_op_real.cc
    GLOBAL tensorflow/core/kernels/batch_norm_op.cc
    GLOBAL tensorflow/core/kernels/batchtospace_op.cc
    GLOBAL tensorflow/core/kernels/bcast_ops.cc
    GLOBAL tensorflow/core/kernels/betainc_op.cc
    GLOBAL tensorflow/core/kernels/bias_op.cc
    GLOBAL tensorflow/core/kernels/bincount_op.cc
    GLOBAL tensorflow/core/kernels/bitcast_op.cc
    GLOBAL tensorflow/core/kernels/boosted_trees/prediction_ops.cc
    GLOBAL tensorflow/core/kernels/boosted_trees/resource_ops.cc
    GLOBAL tensorflow/core/kernels/boosted_trees/stats_ops.cc
    GLOBAL tensorflow/core/kernels/boosted_trees/training_ops.cc
    GLOBAL tensorflow/core/kernels/broadcast_to_op.cc
    GLOBAL tensorflow/core/kernels/bucketize_op.cc
    GLOBAL tensorflow/core/kernels/candidate_sampler_ops.cc
    GLOBAL tensorflow/core/kernels/cast_op.cc
    GLOBAL tensorflow/core/kernels/check_numerics_op.cc
    GLOBAL tensorflow/core/kernels/cholesky_grad.cc
    GLOBAL tensorflow/core/kernels/cholesky_op.cc
    GLOBAL tensorflow/core/kernels/collective_ops.cc
    GLOBAL tensorflow/core/kernels/colorspace_op.cc
    GLOBAL tensorflow/core/kernels/compare_and_bitpack_op.cc
    GLOBAL tensorflow/core/kernels/concat_lib_cpu.cc
    GLOBAL tensorflow/core/kernels/concat_lib_gpu.cc
    GLOBAL tensorflow/core/kernels/concat_op.cc
    GLOBAL tensorflow/core/kernels/conditional_accumulator_base_op.cc
    GLOBAL tensorflow/core/kernels/conditional_accumulator_op.cc
    GLOBAL tensorflow/core/kernels/constant_op.cc
    GLOBAL tensorflow/core/kernels/control_flow_ops.cc
    GLOBAL tensorflow/core/kernels/conv_grad_filter_ops.cc
    GLOBAL tensorflow/core/kernels/conv_grad_input_ops.cc
    GLOBAL tensorflow/core/kernels/conv_grad_ops_3d.cc
    GLOBAL tensorflow/core/kernels/conv_ops.cc
    GLOBAL tensorflow/core/kernels/conv_ops_3d.cc
    GLOBAL tensorflow/core/kernels/conv_ops_fused.cc
    GLOBAL tensorflow/core/kernels/conv_ops_using_gemm.cc
    GLOBAL tensorflow/core/kernels/count_up_to_op.cc
    GLOBAL tensorflow/core/kernels/crop_and_resize_op.cc
    GLOBAL tensorflow/core/kernels/cross_op.cc
    GLOBAL tensorflow/core/kernels/ctc_decoder_ops.cc
    GLOBAL tensorflow/core/kernels/ctc_loss_op.cc
    GLOBAL tensorflow/core/kernels/cudnn_rnn_ops.cc
    GLOBAL tensorflow/core/kernels/cwise_op_abs.cc
    GLOBAL tensorflow/core/kernels/cwise_op_acos.cc
    GLOBAL tensorflow/core/kernels/cwise_op_acosh.cc
    GLOBAL tensorflow/core/kernels/cwise_op_add_1.cc
    GLOBAL tensorflow/core/kernels/cwise_op_add_2.cc
    GLOBAL tensorflow/core/kernels/cwise_op_arg.cc
    GLOBAL tensorflow/core/kernels/cwise_op_asin.cc
    GLOBAL tensorflow/core/kernels/cwise_op_asinh.cc
    GLOBAL tensorflow/core/kernels/cwise_op_atan.cc
    GLOBAL tensorflow/core/kernels/cwise_op_atan2.cc
    GLOBAL tensorflow/core/kernels/cwise_op_atanh.cc
    GLOBAL tensorflow/core/kernels/cwise_op_bessel.cc
    GLOBAL tensorflow/core/kernels/cwise_op_bitwise_and.cc
    GLOBAL tensorflow/core/kernels/cwise_op_bitwise_or.cc
    GLOBAL tensorflow/core/kernels/cwise_op_bitwise_xor.cc
    GLOBAL tensorflow/core/kernels/cwise_op_ceil.cc
    GLOBAL tensorflow/core/kernels/cwise_op_clip.cc
    GLOBAL tensorflow/core/kernels/cwise_op_complex.cc
    GLOBAL tensorflow/core/kernels/cwise_op_conj.cc
    GLOBAL tensorflow/core/kernels/cwise_op_cos.cc
    GLOBAL tensorflow/core/kernels/cwise_op_cosh.cc
    GLOBAL tensorflow/core/kernels/cwise_op_digamma.cc
    GLOBAL tensorflow/core/kernels/cwise_op_div.cc
    GLOBAL tensorflow/core/kernels/cwise_op_equal_to_1.cc
    GLOBAL tensorflow/core/kernels/cwise_op_equal_to_2.cc
    GLOBAL tensorflow/core/kernels/cwise_op_erf.cc
    GLOBAL tensorflow/core/kernels/cwise_op_erfc.cc
    GLOBAL tensorflow/core/kernels/cwise_op_exp.cc
    GLOBAL tensorflow/core/kernels/cwise_op_expm1.cc
    GLOBAL tensorflow/core/kernels/cwise_op_floor.cc
    GLOBAL tensorflow/core/kernels/cwise_op_floor_div.cc
    GLOBAL tensorflow/core/kernels/cwise_op_floor_mod.cc
    GLOBAL tensorflow/core/kernels/cwise_op_greater.cc
    GLOBAL tensorflow/core/kernels/cwise_op_greater_equal.cc
    GLOBAL tensorflow/core/kernels/cwise_op_igammas.cc
    GLOBAL tensorflow/core/kernels/cwise_op_imag.cc
    GLOBAL tensorflow/core/kernels/cwise_op_invert.cc
    GLOBAL tensorflow/core/kernels/cwise_op_isfinite.cc
    GLOBAL tensorflow/core/kernels/cwise_op_isinf.cc
    GLOBAL tensorflow/core/kernels/cwise_op_isnan.cc
    GLOBAL tensorflow/core/kernels/cwise_op_left_shift.cc
    GLOBAL tensorflow/core/kernels/cwise_op_less.cc
    GLOBAL tensorflow/core/kernels/cwise_op_less_equal.cc
    GLOBAL tensorflow/core/kernels/cwise_op_lgamma.cc
    GLOBAL tensorflow/core/kernels/cwise_op_log.cc
    GLOBAL tensorflow/core/kernels/cwise_op_log1p.cc
    GLOBAL tensorflow/core/kernels/cwise_op_logical_and.cc
    GLOBAL tensorflow/core/kernels/cwise_op_logical_not.cc
    GLOBAL tensorflow/core/kernels/cwise_op_logical_or.cc
    GLOBAL tensorflow/core/kernels/cwise_op_maximum.cc
    GLOBAL tensorflow/core/kernels/cwise_op_minimum.cc
    GLOBAL tensorflow/core/kernels/cwise_op_mod.cc
    GLOBAL tensorflow/core/kernels/cwise_op_mul_1.cc
    GLOBAL tensorflow/core/kernels/cwise_op_mul_2.cc
    GLOBAL tensorflow/core/kernels/cwise_op_neg.cc
    GLOBAL tensorflow/core/kernels/cwise_op_not_equal_to_1.cc
    GLOBAL tensorflow/core/kernels/cwise_op_not_equal_to_2.cc
    GLOBAL tensorflow/core/kernels/cwise_op_pow.cc
    GLOBAL tensorflow/core/kernels/cwise_op_random_grad.cc
    GLOBAL tensorflow/core/kernels/cwise_op_real.cc
    GLOBAL tensorflow/core/kernels/cwise_op_reciprocal.cc
    GLOBAL tensorflow/core/kernels/cwise_op_right_shift.cc
    GLOBAL tensorflow/core/kernels/cwise_op_rint.cc
    GLOBAL tensorflow/core/kernels/cwise_op_round.cc
    GLOBAL tensorflow/core/kernels/cwise_op_rsqrt.cc
    GLOBAL tensorflow/core/kernels/cwise_op_select.cc
    GLOBAL tensorflow/core/kernels/cwise_op_sigmoid.cc
    GLOBAL tensorflow/core/kernels/cwise_op_sign.cc
    GLOBAL tensorflow/core/kernels/cwise_op_sin.cc
    GLOBAL tensorflow/core/kernels/cwise_op_sinh.cc
    GLOBAL tensorflow/core/kernels/cwise_op_sqrt.cc
    GLOBAL tensorflow/core/kernels/cwise_op_square.cc
    GLOBAL tensorflow/core/kernels/cwise_op_squared_difference.cc
    GLOBAL tensorflow/core/kernels/cwise_op_sub.cc
    GLOBAL tensorflow/core/kernels/cwise_op_tan.cc
    GLOBAL tensorflow/core/kernels/cwise_op_tanh.cc
    GLOBAL tensorflow/core/kernels/cwise_op_zeta.cc
    GLOBAL tensorflow/core/kernels/data/batch_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/cache_dataset_ops.cc
    GLOBAL tensorflow/core/kernels/data/concatenate_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/dataset_ops.cc
    GLOBAL tensorflow/core/kernels/data/dense_to_sparse_batch_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/filter_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/flat_map_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/generator_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/group_by_reducer_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/group_by_window_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/interleave_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/iterator_ops.cc
    GLOBAL tensorflow/core/kernels/data/map_and_batch_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/map_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/optimize_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/padded_batch_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/parallel_interleave_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/parallel_map_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/prefetch_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/random_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/range_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/reader_dataset_ops.cc
    GLOBAL tensorflow/core/kernels/data/repeat_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/scan_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/shuffle_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/skip_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/slide_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/sparse_tensor_slice_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/sql_dataset_ops.cc
    GLOBAL tensorflow/core/kernels/data/stats_aggregator_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/stats_aggregator_ops.cc
    GLOBAL tensorflow/core/kernels/data/stats_dataset_ops.cc
    GLOBAL tensorflow/core/kernels/data/take_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/tensor_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/tensor_queue_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/tensor_slice_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/unbatch_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/window_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data/writer_ops.cc
    GLOBAL tensorflow/core/kernels/data/zip_dataset_op.cc
    GLOBAL tensorflow/core/kernels/data_format_ops.cc
    GLOBAL tensorflow/core/kernels/debug_ops.cc
    GLOBAL tensorflow/core/kernels/decode_bmp_op.cc
    GLOBAL tensorflow/core/kernels/decode_compressed_op.cc
    GLOBAL tensorflow/core/kernels/decode_csv_op.cc
    GLOBAL tensorflow/core/kernels/decode_image_op.cc
    GLOBAL tensorflow/core/kernels/decode_proto_op.cc
    GLOBAL tensorflow/core/kernels/decode_raw_op.cc
    GLOBAL tensorflow/core/kernels/decode_wav_op.cc
    GLOBAL tensorflow/core/kernels/dense_update_ops.cc
    GLOBAL tensorflow/core/kernels/depthtospace_op.cc
    GLOBAL tensorflow/core/kernels/depthwise_conv_grad_op.cc
    GLOBAL tensorflow/core/kernels/depthwise_conv_op.cc
    GLOBAL tensorflow/core/kernels/dequantize_op.cc
    GLOBAL tensorflow/core/kernels/deserialize_sparse_string_op.cc
    GLOBAL tensorflow/core/kernels/deserialize_sparse_variant_op.cc
    GLOBAL tensorflow/core/kernels/determinant_op.cc
    GLOBAL tensorflow/core/kernels/diag_op.cc
    GLOBAL tensorflow/core/kernels/dilation_ops.cc
    GLOBAL tensorflow/core/kernels/draw_bounding_box_op.cc
    GLOBAL tensorflow/core/kernels/dynamic_partition_op.cc
    GLOBAL tensorflow/core/kernels/dynamic_stitch_op.cc
    GLOBAL tensorflow/core/kernels/edit_distance_op.cc
    GLOBAL tensorflow/core/kernels/encode_jpeg_op.cc
    GLOBAL tensorflow/core/kernels/encode_png_op.cc
    GLOBAL tensorflow/core/kernels/encode_proto_op.cc
    GLOBAL tensorflow/core/kernels/encode_wav_op.cc
    GLOBAL tensorflow/core/kernels/example_parsing_ops.cc
    GLOBAL tensorflow/core/kernels/extract_image_patches_op.cc
    GLOBAL tensorflow/core/kernels/extract_jpeg_shape_op.cc
    GLOBAL tensorflow/core/kernels/fact_op.cc
    GLOBAL tensorflow/core/kernels/fake_quant_ops.cc
    GLOBAL tensorflow/core/kernels/fft_ops.cc
    GLOBAL tensorflow/core/kernels/fifo_queue_op.cc
    GLOBAL tensorflow/core/kernels/fixed_length_record_reader_op.cc
    GLOBAL tensorflow/core/kernels/fractional_avg_pool_op.cc
    GLOBAL tensorflow/core/kernels/fractional_max_pool_op.cc
    GLOBAL tensorflow/core/kernels/function_ops.cc
    GLOBAL tensorflow/core/kernels/functional_ops.cc
    GLOBAL tensorflow/core/kernels/fused_batch_norm_op.cc
    GLOBAL tensorflow/core/kernels/gather_nd_op.cc
    GLOBAL tensorflow/core/kernels/gather_op.cc
    GLOBAL tensorflow/core/kernels/generate_vocab_remapping_op.cc
    GLOBAL tensorflow/core/kernels/guarantee_const_op.cc
    GLOBAL tensorflow/core/kernels/histogram_op.cc
    GLOBAL tensorflow/core/kernels/identity_n_op.cc
    GLOBAL tensorflow/core/kernels/identity_op.cc
    GLOBAL tensorflow/core/kernels/identity_reader_op.cc
    GLOBAL tensorflow/core/kernels/immutable_constant_op.cc
    GLOBAL tensorflow/core/kernels/in_topk_op.cc
    GLOBAL tensorflow/core/kernels/inplace_ops.cc
    GLOBAL tensorflow/core/kernels/l2loss_op.cc
    GLOBAL tensorflow/core/kernels/list_kernels.cc
    GLOBAL tensorflow/core/kernels/listdiff_op.cc
    # GLOBAL tensorflow/core/kernels/lmdb_reader_op.cc
    GLOBAL tensorflow/core/kernels/load_and_remap_matrix_op.cc
    GLOBAL tensorflow/core/kernels/logging_ops.cc
    GLOBAL tensorflow/core/kernels/lookup_table_init_op.cc
    GLOBAL tensorflow/core/kernels/lookup_table_op.cc
    GLOBAL tensorflow/core/kernels/lrn_op.cc
    GLOBAL tensorflow/core/kernels/map_stage_op.cc
    GLOBAL tensorflow/core/kernels/matching_files_op.cc
    GLOBAL tensorflow/core/kernels/matmul_op.cc
    GLOBAL tensorflow/core/kernels/matrix_band_part_op.cc
    GLOBAL tensorflow/core/kernels/matrix_diag_op.cc
    GLOBAL tensorflow/core/kernels/matrix_exponential_op.cc
    GLOBAL tensorflow/core/kernels/matrix_inverse_op.cc
    GLOBAL tensorflow/core/kernels/matrix_logarithm_op.cc
    GLOBAL tensorflow/core/kernels/matrix_set_diag_op.cc
    GLOBAL tensorflow/core/kernels/matrix_solve_ls_op_complex128.cc
    GLOBAL tensorflow/core/kernels/matrix_solve_ls_op_complex64.cc
    GLOBAL tensorflow/core/kernels/matrix_solve_ls_op_double.cc
    GLOBAL tensorflow/core/kernels/matrix_solve_ls_op_float.cc
    GLOBAL tensorflow/core/kernels/matrix_solve_op.cc
    GLOBAL tensorflow/core/kernels/matrix_triangular_solve_op.cc
    GLOBAL tensorflow/core/kernels/maxpooling_op.cc
    GLOBAL tensorflow/core/kernels/mfcc_op.cc
    GLOBAL tensorflow/core/kernels/mirror_pad_op.cc
    GLOBAL tensorflow/core/kernels/multinomial_op.cc
    GLOBAL tensorflow/core/kernels/mutex_ops.cc
    GLOBAL tensorflow/core/kernels/neon/neon_depthwise_conv_op.cc
    GLOBAL tensorflow/core/kernels/no_op.cc
    GLOBAL tensorflow/core/kernels/non_max_suppression_op.cc
    GLOBAL tensorflow/core/kernels/nth_element_op.cc
    GLOBAL tensorflow/core/kernels/one_hot_op.cc
    GLOBAL tensorflow/core/kernels/pack_op.cc
    GLOBAL tensorflow/core/kernels/pad_op.cc
    GLOBAL tensorflow/core/kernels/padding_fifo_queue_op.cc
    GLOBAL tensorflow/core/kernels/parameterized_truncated_normal_op.cc
    GLOBAL tensorflow/core/kernels/parse_tensor_op.cc
    GLOBAL tensorflow/core/kernels/partitioned_function_ops.cc
    GLOBAL tensorflow/core/kernels/pooling_ops_3d.cc
    GLOBAL tensorflow/core/kernels/population_count_op.cc
    GLOBAL tensorflow/core/kernels/priority_queue_op.cc
    GLOBAL tensorflow/core/kernels/qr_op_complex128.cc
    GLOBAL tensorflow/core/kernels/qr_op_complex64.cc
    GLOBAL tensorflow/core/kernels/qr_op_double.cc
    GLOBAL tensorflow/core/kernels/qr_op_float.cc
    GLOBAL tensorflow/core/kernels/quantize_and_dequantize_op.cc
    GLOBAL tensorflow/core/kernels/quantize_down_and_shrink_range.cc
    GLOBAL tensorflow/core/kernels/quantize_op.cc
    GLOBAL tensorflow/core/kernels/quantized_activation_ops.cc
    GLOBAL tensorflow/core/kernels/quantized_add_op.cc
    GLOBAL tensorflow/core/kernels/quantized_batch_norm_op.cc
    GLOBAL tensorflow/core/kernels/quantized_bias_add_op.cc
    GLOBAL tensorflow/core/kernels/quantized_concat_op.cc
    GLOBAL tensorflow/core/kernels/quantized_conv_ops.cc
    GLOBAL tensorflow/core/kernels/quantized_instance_norm.cc
    GLOBAL tensorflow/core/kernels/quantized_matmul_op.cc
    GLOBAL tensorflow/core/kernels/quantized_mul_op.cc
    GLOBAL tensorflow/core/kernels/quantized_pooling_ops.cc
    GLOBAL tensorflow/core/kernels/quantized_reshape_op.cc
    GLOBAL tensorflow/core/kernels/quantized_resize_bilinear_op.cc
    GLOBAL tensorflow/core/kernels/queue_ops.cc
    GLOBAL tensorflow/core/kernels/random_crop_op.cc
    GLOBAL tensorflow/core/kernels/random_op.cc
    GLOBAL tensorflow/core/kernels/random_poisson_op.cc
    GLOBAL tensorflow/core/kernels/random_shuffle_op.cc
    GLOBAL tensorflow/core/kernels/random_shuffle_queue_op.cc
    GLOBAL tensorflow/core/kernels/reader_ops.cc
    GLOBAL tensorflow/core/kernels/record_input_op.cc
    GLOBAL tensorflow/core/kernels/reduce_join_op.cc
    GLOBAL tensorflow/core/kernels/reduction_ops_all.cc
    GLOBAL tensorflow/core/kernels/reduction_ops_any.cc
    GLOBAL tensorflow/core/kernels/reduction_ops_max.cc
    GLOBAL tensorflow/core/kernels/reduction_ops_mean.cc
    GLOBAL tensorflow/core/kernels/reduction_ops_min.cc
    GLOBAL tensorflow/core/kernels/reduction_ops_prod.cc
    GLOBAL tensorflow/core/kernels/reduction_ops_sum.cc
    GLOBAL tensorflow/core/kernels/regex_full_match_op.cc
    GLOBAL tensorflow/core/kernels/regex_replace_op.cc
    GLOBAL tensorflow/core/kernels/relu_op.cc
    GLOBAL tensorflow/core/kernels/remote_fused_graph_execute_op.cc
    GLOBAL tensorflow/core/kernels/requantization_range_op.cc
    GLOBAL tensorflow/core/kernels/requantize.cc
    GLOBAL tensorflow/core/kernels/reshape_op.cc
    GLOBAL tensorflow/core/kernels/resize_area_op.cc
    GLOBAL tensorflow/core/kernels/resize_bicubic_op.cc
    GLOBAL tensorflow/core/kernels/resize_bilinear_op.cc
    GLOBAL tensorflow/core/kernels/resize_nearest_neighbor_op.cc
    GLOBAL tensorflow/core/kernels/resource_variable_ops.cc
    GLOBAL tensorflow/core/kernels/restore_op.cc
    GLOBAL tensorflow/core/kernels/reverse_op.cc
    GLOBAL tensorflow/core/kernels/reverse_sequence_op.cc
    GLOBAL tensorflow/core/kernels/roll_op.cc
    GLOBAL tensorflow/core/kernels/rpc_op.cc
    GLOBAL tensorflow/core/kernels/sample_distorted_bounding_box_op.cc
    GLOBAL tensorflow/core/kernels/save_op.cc
    GLOBAL tensorflow/core/kernels/save_restore_v2_ops.cc
    GLOBAL tensorflow/core/kernels/scan_ops.cc
    GLOBAL tensorflow/core/kernels/scatter_nd_op.cc
    GLOBAL tensorflow/core/kernels/scatter_op.cc
    GLOBAL tensorflow/core/kernels/scoped_allocator_ops.cc
    GLOBAL tensorflow/core/kernels/sdca_ops.cc
    GLOBAL tensorflow/core/kernels/segment_reduction_ops.cc
    GLOBAL tensorflow/core/kernels/self_adjoint_eig_op.cc
    GLOBAL tensorflow/core/kernels/self_adjoint_eig_v2_op_complex128.cc
    GLOBAL tensorflow/core/kernels/self_adjoint_eig_v2_op_complex64.cc
    GLOBAL tensorflow/core/kernels/self_adjoint_eig_v2_op_double.cc
    GLOBAL tensorflow/core/kernels/self_adjoint_eig_v2_op_float.cc
    GLOBAL tensorflow/core/kernels/self_adjoint_eig_v2_op_gpu.cc
    GLOBAL tensorflow/core/kernels/sendrecv_ops.cc
    GLOBAL tensorflow/core/kernels/sequence_ops.cc
    GLOBAL tensorflow/core/kernels/serialize_sparse_op.cc
    GLOBAL tensorflow/core/kernels/session_ops.cc
    GLOBAL tensorflow/core/kernels/set_kernels.cc
    GLOBAL tensorflow/core/kernels/shape_ops.cc
    GLOBAL tensorflow/core/kernels/slice_op.cc
    GLOBAL tensorflow/core/kernels/snapshot_op.cc
    GLOBAL tensorflow/core/kernels/softmax_op.cc
    GLOBAL tensorflow/core/kernels/softplus_op.cc
    GLOBAL tensorflow/core/kernels/softsign_op.cc
    GLOBAL tensorflow/core/kernels/spacetobatch_op.cc
    GLOBAL tensorflow/core/kernels/spacetodepth_op.cc
    GLOBAL tensorflow/core/kernels/sparse_add_grad_op.cc
    GLOBAL tensorflow/core/kernels/sparse_add_op.cc
    GLOBAL tensorflow/core/kernels/sparse_concat_op.cc
    GLOBAL tensorflow/core/kernels/sparse_conditional_accumulator_op.cc
    GLOBAL tensorflow/core/kernels/sparse_cross_op.cc
    GLOBAL tensorflow/core/kernels/sparse_dense_binary_op_shared.cc
    GLOBAL tensorflow/core/kernels/sparse_fill_empty_rows_op.cc
    GLOBAL tensorflow/core/kernels/sparse_matmul_op.cc
    GLOBAL tensorflow/core/kernels/sparse_reduce_op.cc
    GLOBAL tensorflow/core/kernels/sparse_reorder_op.cc
    GLOBAL tensorflow/core/kernels/sparse_reshape_op.cc
    GLOBAL tensorflow/core/kernels/sparse_slice_grad_op.cc
    GLOBAL tensorflow/core/kernels/sparse_slice_op.cc
    GLOBAL tensorflow/core/kernels/sparse_softmax_op.cc
    GLOBAL tensorflow/core/kernels/sparse_sparse_binary_op_shared.cc
    GLOBAL tensorflow/core/kernels/sparse_split_op.cc
    GLOBAL tensorflow/core/kernels/sparse_tensor_dense_add_op.cc
    GLOBAL tensorflow/core/kernels/sparse_tensor_dense_matmul_op.cc
    GLOBAL tensorflow/core/kernels/sparse_tensors_map_ops.cc
    GLOBAL tensorflow/core/kernels/sparse_to_dense_op.cc
    GLOBAL tensorflow/core/kernels/sparse_xent_op.cc
    # GLOBAL tensorflow/core/kernels/spectrogram_op.cc
    GLOBAL tensorflow/core/kernels/split_op.cc
    GLOBAL tensorflow/core/kernels/split_v_op.cc
    GLOBAL tensorflow/core/kernels/stack_ops.cc
    GLOBAL tensorflow/core/kernels/stage_op.cc
    GLOBAL tensorflow/core/kernels/stateless_random_ops.cc
    GLOBAL tensorflow/core/kernels/strided_slice_op.cc
    GLOBAL tensorflow/core/kernels/string_join_op.cc
    GLOBAL tensorflow/core/kernels/string_split_op.cc
    GLOBAL tensorflow/core/kernels/string_strip_op.cc
    GLOBAL tensorflow/core/kernels/string_to_hash_bucket_op.cc
    GLOBAL tensorflow/core/kernels/string_to_number_op.cc
    GLOBAL tensorflow/core/kernels/substr_op.cc
    GLOBAL tensorflow/core/kernels/summary_audio_op.cc
    GLOBAL tensorflow/core/kernels/summary_image_op.cc
    GLOBAL tensorflow/core/kernels/summary_kernels.cc
    GLOBAL tensorflow/core/kernels/summary_op.cc
    GLOBAL tensorflow/core/kernels/summary_tensor_op.cc
    GLOBAL tensorflow/core/kernels/svd_op_complex128.cc
    GLOBAL tensorflow/core/kernels/svd_op_complex64.cc
    GLOBAL tensorflow/core/kernels/svd_op_double.cc
    GLOBAL tensorflow/core/kernels/svd_op_float.cc
    GLOBAL tensorflow/core/kernels/tensor_array_ops.cc
    GLOBAL tensorflow/core/kernels/text_line_reader_op.cc
    GLOBAL tensorflow/core/kernels/tf_record_reader_op.cc
    GLOBAL tensorflow/core/kernels/tile_ops.cc
    GLOBAL tensorflow/core/kernels/topk_op.cc
    GLOBAL tensorflow/core/kernels/training_ops.cc
    GLOBAL tensorflow/core/kernels/transpose_op.cc
    GLOBAL tensorflow/core/kernels/unary_ops_composition.cc
    GLOBAL tensorflow/core/kernels/unique_op.cc
    GLOBAL tensorflow/core/kernels/unpack_op.cc
    GLOBAL tensorflow/core/kernels/unravel_index_op.cc
    GLOBAL tensorflow/core/kernels/variable_ops.cc
    GLOBAL tensorflow/core/kernels/where_op.cc
    GLOBAL tensorflow/core/kernels/whole_file_read_ops.cc
    GLOBAL tensorflow/core/kernels/word2vec_kernels.cc
    GLOBAL tensorflow/core/kernels/xent_op.cc
    GLOBAL tensorflow/core/ops/array_grad.cc
    GLOBAL tensorflow/core/ops/array_ops.cc
    GLOBAL tensorflow/core/ops/audio_ops.cc
    GLOBAL tensorflow/core/ops/batch_ops.cc
    GLOBAL tensorflow/core/ops/bitwise_ops.cc
    GLOBAL tensorflow/core/ops/boosted_trees_ops.cc
    GLOBAL tensorflow/core/ops/candidate_sampling_ops.cc
    GLOBAL tensorflow/core/ops/checkpoint_ops.cc
    GLOBAL tensorflow/core/ops/collective_ops.cc
    GLOBAL tensorflow/core/ops/control_flow_ops.cc
    GLOBAL tensorflow/core/ops/ctc_ops.cc
    GLOBAL tensorflow/core/ops/cudnn_rnn_ops.cc
    GLOBAL tensorflow/core/ops/data_flow_ops.cc
    GLOBAL tensorflow/core/ops/dataset_ops.cc
    GLOBAL tensorflow/core/ops/debug_ops.cc
    GLOBAL tensorflow/core/ops/decode_proto_ops.cc
    GLOBAL tensorflow/core/ops/encode_proto_ops.cc
    GLOBAL tensorflow/core/ops/function_ops.cc
    GLOBAL tensorflow/core/ops/functional_grad.cc
    GLOBAL tensorflow/core/ops/functional_ops.cc
    GLOBAL tensorflow/core/ops/image_ops.cc
    GLOBAL tensorflow/core/ops/io_ops.cc
    GLOBAL tensorflow/core/ops/linalg_ops.cc
    GLOBAL tensorflow/core/ops/list_ops.cc
    GLOBAL tensorflow/core/ops/logging_ops.cc
    GLOBAL tensorflow/core/ops/lookup_ops.cc
    GLOBAL tensorflow/core/ops/manip_ops.cc
    GLOBAL tensorflow/core/ops/math_grad.cc
    GLOBAL tensorflow/core/ops/math_ops.cc
    GLOBAL tensorflow/core/ops/nn_grad.cc
    GLOBAL tensorflow/core/ops/nn_ops.cc
    GLOBAL tensorflow/core/ops/no_op.cc
    GLOBAL tensorflow/core/ops/parsing_ops.cc
    GLOBAL tensorflow/core/ops/random_grad.cc
    GLOBAL tensorflow/core/ops/random_ops.cc
    GLOBAL tensorflow/core/ops/remote_fused_graph_ops.cc
    GLOBAL tensorflow/core/ops/resource_variable_ops.cc
    GLOBAL tensorflow/core/ops/rpc_ops.cc
    GLOBAL tensorflow/core/ops/scoped_allocator_ops.cc
    GLOBAL tensorflow/core/ops/sdca_ops.cc
    GLOBAL tensorflow/core/ops/sendrecv_ops.cc
    GLOBAL tensorflow/core/ops/set_ops.cc
    GLOBAL tensorflow/core/ops/sparse_ops.cc
    GLOBAL tensorflow/core/ops/spectral_ops.cc
    GLOBAL tensorflow/core/ops/state_ops.cc
    GLOBAL tensorflow/core/ops/stateless_random_ops.cc
    GLOBAL tensorflow/core/ops/string_ops.cc
    GLOBAL tensorflow/core/ops/summary_ops.cc
    GLOBAL tensorflow/core/ops/training_ops.cc
    GLOBAL tensorflow/core/ops/word2vec_ops.cc
    GLOBAL tensorflow/core/user_ops/fact.cc
    GLOBAL tensorflow/core/util/proto/local_descriptor_pool_registration.cc
    GLOBAL tensorflow/core/util/tensor_bundle/tensor_bundle.cc
    GLOBAL tensorflow/stream_executor/host/host_platform.cc
    GLOBAL tensorflow/stream_executor/multi_platform_manager.cc
    GLOBAL tensorflow/contrib/rnn/kernels/blas_gemm.cc
    GLOBAL tensorflow/contrib/rnn/kernels/gru_ops.cc
    GLOBAL tensorflow/contrib/rnn/kernels/lstm_ops.cc
    GLOBAL tensorflow/contrib/rnn/ops/gru_ops.cc
    GLOBAL tensorflow/contrib/rnn/ops/lstm_ops.cc
)

IF (TENSORFLOW_WITH_CUDA)
    SRCS(
        GLOBAL tensorflow/contrib/image/kernels/adjust_hsv_in_yiq_op_gpu.cu
        GLOBAL tensorflow/contrib/image/kernels/image_ops_gpu.cu
        GLOBAL tensorflow/contrib/rnn/kernels/gru_ops_gpu.cu
        GLOBAL tensorflow/contrib/seq2seq/kernels/beam_search_ops_gpu.cu
        GLOBAL tensorflow/core/kernels/aggregate_ops_gpu.cu
        GLOBAL tensorflow/core/kernels/bincount_op_gpu.cu
        GLOBAL tensorflow/core/kernels/bucketize_op_gpu.cu
        GLOBAL tensorflow/core/kernels/concat_lib_gpu_impl.cu
        GLOBAL tensorflow/core/kernels/dynamic_partition_op_gpu.cu
        GLOBAL tensorflow/core/kernels/dynamic_stitch_op_gpu.cu
        GLOBAL tensorflow/core/kernels/extract_image_patches_op_gpu.cu
        GLOBAL tensorflow/core/kernels/histogram_op_gpu.cu
        GLOBAL tensorflow/core/kernels/l2loss_op_gpu.cu
        GLOBAL tensorflow/core/kernels/list_kernels.cu
        GLOBAL tensorflow/core/kernels/softmax_op_gpu.cu
        GLOBAL tensorflow/core/kernels/sparse_xent_op_gpu.cu
        GLOBAL tensorflow/core/kernels/split_lib_gpu.cu
        GLOBAL tensorflow/core/kernels/svd_op_gpu.cu
        GLOBAL tensorflow/stream_executor/cuda/cuda_blas.cc
        GLOBAL tensorflow/stream_executor/cuda/cuda_dnn.cc
        GLOBAL tensorflow/stream_executor/cuda/cuda_fft.cc
        GLOBAL tensorflow/stream_executor/cuda/cuda_gpu_executor.cc
        GLOBAL tensorflow/stream_executor/cuda/cuda_platform.cc
    )
    PEERDIR(
        contrib/libs/nvidia/cublas
    )
ENDIF()

END()
