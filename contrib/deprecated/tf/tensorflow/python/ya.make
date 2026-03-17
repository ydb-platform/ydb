PY23_LIBRARY()

VERSION(1.10.1)

LICENSE(
    Apache-2.0 AND
    CC-BY-SA-3.0 AND
    MIT
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PROTO_NAMESPACE(contrib/deprecated/tf)

PY_NAMESPACE(.)

PEERDIR(
    contrib/deprecated/jsoncpp
    contrib/deprecated/tf
    contrib/deprecated/tf/proto
    contrib/deprecated/tf/grpc_proto
    contrib/deprecated/tf/tensorflow/python/framework/python_op_gen
    contrib/libs/zlib
    contrib/python/absl-py
    contrib/python/mock
    contrib/python/numpy
)

ADDINCL(
    contrib/deprecated/jsoncpp
    contrib/libs/farmhash
    contrib/restricted/google/gemmlowp
    FOR
    swig
    contrib/tools/swig/Lib/python
    FOR
    swig
    contrib/tools/swig/Lib
)

NO_COMPILER_WARNINGS()

NO_LINT()

CFLAGS(
    -DHAS_GLOBAL_STRING
    -DDISABLE_PYDEBUG
)

PY_REGISTER(
    tensorflow.python._pywrap_tensorflow_internal
)

IF (TENSORFLOW_WITH_CUDA)
    COPY_FILE(platform/build_info_cuda.py platform/build_info.py)
ELSE()
    COPY_FILE(platform/build_info_cpu.py platform/build_info.py)
ENDIF()

SRCDIR(
    ${ARCADIA_BUILD_ROOT}/contrib/deprecated/tf
    contrib/deprecated/tf
)

PY_SRCS(
    TOP_LEVEL
    tensorflow/compiler/xla/python_api/types.py
    tensorflow/compiler/xla/python_api/xla_shape.py
    tensorflow/contrib/__init__.py
    tensorflow/contrib/checkpoint/__init__.py
    tensorflow/contrib/checkpoint/python/containers.py
    tensorflow/contrib/checkpoint/python/split_dependency.py
    tensorflow/contrib/checkpoint/python/visualize.py
    tensorflow/contrib/compiler/__init__.py
    tensorflow/contrib/compiler/jit.py
    tensorflow/contrib/cudnn_rnn/__init__.py
    tensorflow/contrib/cudnn_rnn/python/layers/__init__.py
    tensorflow/contrib/cudnn_rnn/python/layers/cudnn_rnn.py
    tensorflow/contrib/cudnn_rnn/python/ops/cudnn_rnn_ops.py
    tensorflow/contrib/data/__init__.py
    tensorflow/contrib/data/python/ops/batching.py
    tensorflow/contrib/data/python/ops/contrib_op_loader.py
    tensorflow/contrib/data/python/ops/counter.py
    tensorflow/contrib/data/python/ops/enumerate_ops.py
    tensorflow/contrib/data/python/ops/error_ops.py
    tensorflow/contrib/data/python/ops/get_single_element.py
    tensorflow/contrib/data/python/ops/grouping.py
    tensorflow/contrib/data/python/ops/interleave_ops.py
    tensorflow/contrib/data/python/ops/iterator_ops.py
    tensorflow/contrib/data/python/ops/optimization.py
    tensorflow/contrib/data/python/ops/prefetching_ops.py
    tensorflow/contrib/data/python/ops/random_ops.py
    tensorflow/contrib/data/python/ops/readers.py
    tensorflow/contrib/data/python/ops/resampling.py
    tensorflow/contrib/data/python/ops/scan_ops.py
    tensorflow/contrib/data/python/ops/shuffle_ops.py
    tensorflow/contrib/data/python/ops/sliding.py
    tensorflow/contrib/data/python/ops/stats_ops.py
    tensorflow/contrib/data/python/ops/threadpool.py
    tensorflow/contrib/data/python/ops/unique.py
    tensorflow/contrib/data/python/ops/writers.py
    tensorflow/contrib/framework/__init__.py
    tensorflow/contrib/framework/python/framework/__init__.py
    tensorflow/contrib/framework/python/framework/checkpoint_utils.py
    tensorflow/contrib/framework/python/framework/experimental.py
    tensorflow/contrib/framework/python/framework/graph_util.py
    tensorflow/contrib/framework/python/framework/tensor_util.py
    tensorflow/contrib/framework/python/ops/__init__.py
    tensorflow/contrib/framework/python/ops/arg_scope.py
    tensorflow/contrib/framework/python/ops/audio_ops.py
    tensorflow/contrib/framework/python/ops/checkpoint_ops.py
    tensorflow/contrib/framework/python/ops/critical_section_ops.py
    tensorflow/contrib/framework/python/ops/ops.py
    tensorflow/contrib/framework/python/ops/prettyprint_ops.py
    tensorflow/contrib/framework/python/ops/script_ops.py
    tensorflow/contrib/framework/python/ops/sort_ops.py
    tensorflow/contrib/framework/python/ops/variables.py
    tensorflow/contrib/graph_editor/__init__.py
    tensorflow/contrib/graph_editor/edit.py
    tensorflow/contrib/graph_editor/reroute.py
    tensorflow/contrib/graph_editor/select.py
    tensorflow/contrib/graph_editor/subgraph.py
    tensorflow/contrib/graph_editor/transform.py
    tensorflow/contrib/graph_editor/util.py
    tensorflow/contrib/image/__init__.py
    tensorflow/contrib/image/python/ops/dense_image_warp.py
    tensorflow/contrib/image/python/ops/distort_image_ops.py
    tensorflow/contrib/image/python/ops/image_ops.py
    tensorflow/contrib/image/python/ops/interpolate_spline.py
    tensorflow/contrib/image/python/ops/single_image_random_dot_stereograms.py
    tensorflow/contrib/image/python/ops/sparse_image_warp.py
    tensorflow/contrib/layers/__init__.py
    tensorflow/contrib/layers/python/layers/__init__.py
    tensorflow/contrib/layers/python/layers/embedding_ops.py
    tensorflow/contrib/layers/python/layers/encoders.py
    tensorflow/contrib/layers/python/layers/feature_column.py
    tensorflow/contrib/layers/python/layers/feature_column_ops.py
    tensorflow/contrib/layers/python/layers/initializers.py
    tensorflow/contrib/layers/python/layers/layers.py
    tensorflow/contrib/layers/python/layers/normalization.py
    tensorflow/contrib/layers/python/layers/optimizers.py
    tensorflow/contrib/layers/python/layers/regularizers.py
    tensorflow/contrib/layers/python/layers/rev_block_lib.py
    tensorflow/contrib/layers/python/layers/summaries.py
    tensorflow/contrib/layers/python/layers/target_column.py
    tensorflow/contrib/layers/python/layers/utils.py
    tensorflow/contrib/layers/python/ops/bucketization_op.py
    tensorflow/contrib/layers/python/ops/sparse_feature_cross_op.py
    tensorflow/contrib/layers/python/ops/sparse_ops.py
    tensorflow/contrib/lookup/__init__.py
    tensorflow/contrib/lookup/lookup_ops.py
    tensorflow/contrib/losses/__init__.py
    tensorflow/contrib/losses/python/losses/__init__.py
    tensorflow/contrib/losses/python/losses/loss_ops.py
    tensorflow/contrib/losses/python/metric_learning/__init__.py
    tensorflow/contrib/losses/python/metric_learning/metric_loss_ops.py
    tensorflow/contrib/memory_stats/__init__.py
    tensorflow/contrib/memory_stats/python/ops/memory_stats_ops.py
    tensorflow/contrib/metrics/__init__.py
    tensorflow/contrib/metrics/python/metrics/__init__.py
    tensorflow/contrib/metrics/python/metrics/classification.py
    tensorflow/contrib/metrics/python/ops/confusion_matrix_ops.py
    tensorflow/contrib/metrics/python/ops/histogram_ops.py
    tensorflow/contrib/metrics/python/ops/metric_ops.py
    tensorflow/contrib/metrics/python/ops/set_ops.py
    tensorflow/contrib/mixed_precision/__init__.py
    tensorflow/contrib/mixed_precision/python/loss_scale_manager.py
    tensorflow/contrib/mixed_precision/python/loss_scale_optimizer.py
    tensorflow/contrib/opt/__init__.py
    tensorflow/contrib/opt/python/training/adamax.py
    tensorflow/contrib/opt/python/training/addsign.py
    tensorflow/contrib/opt/python/training/drop_stale_gradient_optimizer.py
    tensorflow/contrib/opt/python/training/elastic_average_optimizer.py
    tensorflow/contrib/opt/python/training/external_optimizer.py
    tensorflow/contrib/opt/python/training/ggt.py
    tensorflow/contrib/opt/python/training/lazy_adam_optimizer.py
    tensorflow/contrib/opt/python/training/model_average_optimizer.py
    tensorflow/contrib/opt/python/training/moving_average_optimizer.py
    tensorflow/contrib/opt/python/training/multitask_optimizer_wrapper.py
    tensorflow/contrib/opt/python/training/nadam_optimizer.py
    tensorflow/contrib/opt/python/training/powersign.py
    tensorflow/contrib/opt/python/training/reg_adagrad_optimizer.py
    tensorflow/contrib/opt/python/training/sign_decay.py
    tensorflow/contrib/opt/python/training/variable_clipping_optimizer.py
    tensorflow/contrib/opt/python/training/weight_decay_optimizers.py
    tensorflow/contrib/optimizer_v2/adadelta.py
    tensorflow/contrib/optimizer_v2/adagrad.py
    tensorflow/contrib/optimizer_v2/adam.py
    tensorflow/contrib/optimizer_v2/gradient_descent.py
    tensorflow/contrib/optimizer_v2/momentum.py
    tensorflow/contrib/optimizer_v2/optimizer_v2.py
    tensorflow/contrib/optimizer_v2/optimizer_v2_symbols.py
    tensorflow/contrib/optimizer_v2/rmsprop.py
    tensorflow/contrib/rnn/__init__.py
    tensorflow/contrib/rnn/python/ops/core_rnn_cell.py
    tensorflow/contrib/rnn/python/ops/fused_rnn_cell.py
    tensorflow/contrib/rnn/python/ops/gru_ops.py
    tensorflow/contrib/rnn/python/ops/lstm_ops.py
    tensorflow/contrib/rnn/python/ops/rnn.py
    tensorflow/contrib/rnn/python/ops/rnn_cell.py
    tensorflow/contrib/saved_model/python/__init__.py
    tensorflow/contrib/saved_model/python/saved_model/__init__.py
    tensorflow/contrib/saved_model/python/saved_model/reader.py
    tensorflow/contrib/saved_model/python/saved_model/signature_def_utils.py
    tensorflow/contrib/seq2seq/__init__.py
    tensorflow/contrib/seq2seq/python/ops/__init__.py
    tensorflow/contrib/seq2seq/python/ops/attention_wrapper.py
    tensorflow/contrib/seq2seq/python/ops/basic_decoder.py
    tensorflow/contrib/seq2seq/python/ops/beam_search_decoder.py
    tensorflow/contrib/seq2seq/python/ops/beam_search_ops.py
    tensorflow/contrib/seq2seq/python/ops/decoder.py
    tensorflow/contrib/seq2seq/python/ops/helper.py
    tensorflow/contrib/seq2seq/python/ops/loss.py
    tensorflow/contrib/slim/__init__.py
    tensorflow/contrib/slim/nets.py
    tensorflow/contrib/slim/python/slim/data/data_decoder.py
    tensorflow/contrib/slim/python/slim/data/data_provider.py
    tensorflow/contrib/slim/python/slim/data/dataset.py
    tensorflow/contrib/slim/python/slim/data/dataset_data_provider.py
    tensorflow/contrib/slim/python/slim/data/parallel_reader.py
    tensorflow/contrib/slim/python/slim/data/prefetch_queue.py
    tensorflow/contrib/slim/python/slim/data/test_utils.py
    tensorflow/contrib/slim/python/slim/data/tfexample_decoder.py
    tensorflow/contrib/slim/python/slim/evaluation.py
    tensorflow/contrib/slim/python/slim/learning.py
    tensorflow/contrib/slim/python/slim/model_analyzer.py
    tensorflow/contrib/slim/python/slim/nets/alexnet.py
    tensorflow/contrib/slim/python/slim/nets/inception.py
    tensorflow/contrib/slim/python/slim/nets/inception_v1.py
    tensorflow/contrib/slim/python/slim/nets/inception_v2.py
    tensorflow/contrib/slim/python/slim/nets/inception_v3.py
    tensorflow/contrib/slim/python/slim/nets/overfeat.py
    tensorflow/contrib/slim/python/slim/nets/resnet_utils.py
    tensorflow/contrib/slim/python/slim/nets/resnet_v1.py
    tensorflow/contrib/slim/python/slim/nets/resnet_v2.py
    tensorflow/contrib/slim/python/slim/nets/vgg.py
    tensorflow/contrib/slim/python/slim/queues.py
    tensorflow/contrib/slim/python/slim/summaries.py
    tensorflow/contrib/tensorboard/__init__.py
    tensorflow/contrib/tensorboard/plugins/__init__.py
    tensorflow/contrib/tensorboard/plugins/projector/__init__.py
    tensorflow/contrib/tensorboard/plugins/projector/projector_config.proto
    tensorflow/contrib/tensorboard/plugins/trace/__init__.py
    tensorflow/contrib/tensorboard/plugins/trace/trace.py
    tensorflow/contrib/tensorboard/plugins/trace/trace_info.proto
    tensorflow/contrib/testing/__init__.py
    tensorflow/contrib/testing/python/framework/fake_summary_writer.py
    tensorflow/contrib/testing/python/framework/util_test.py
    tensorflow/contrib/tfprof/__init__.py
    tensorflow/contrib/tfprof/model_analyzer.py
    tensorflow/contrib/tfprof/tfprof_logger.py
    tensorflow/contrib/training/__init__.py
    tensorflow/contrib/training/python/__init__.py
    tensorflow/contrib/training/python/training/__init__.py
    tensorflow/contrib/training/python/training/bucket_ops.py
    tensorflow/contrib/training/python/training/device_setter.py
    tensorflow/contrib/training/python/training/evaluation.py
    tensorflow/contrib/training/python/training/feeding_queue_runner.py
    tensorflow/contrib/training/python/training/hparam.proto
    tensorflow/contrib/training/python/training/hparam.py
    tensorflow/contrib/training/python/training/resample.py
    tensorflow/contrib/training/python/training/sampling_ops.py
    tensorflow/contrib/training/python/training/sequence_queueing_state_saver.py
    tensorflow/contrib/training/python/training/tensor_queue_dataset.py
    tensorflow/contrib/training/python/training/training.py
    tensorflow/contrib/training/python/training/tuner.py
    tensorflow/contrib/util/__init__.py
    tensorflow/contrib/util/loader.py
    tensorflow/tools/graph_transforms/__init__.py

    NAMESPACE tensorflow.python
    __init__.py
    client/__init__.py
    client/client_lib.py
    client/device_lib.py
    client/notebook.py
    client/session.py
    client/session_benchmark.py
    client/timeline.py
    compat/compat.py
    data/__init__.py
    data/ops/dataset_ops.py
    data/ops/iterator_ops.py
    data/ops/readers.py
    data/util/convert.py
    data/util/nest.py
    data/util/random_seed.py
    data/util/sparse.py
    debug/__init__.py
    debug/cli/__init__.py
    debug/cli/analyzer_cli.py
    debug/cli/base_ui.py
    debug/cli/cli_config.py
    debug/cli/cli_shared.py
    debug/cli/cli_test_utils.py
    debug/cli/command_parser.py
    #debug/cli/curses_ui.py
    debug/cli/curses_widgets.py
    debug/cli/debugger_cli_common.py
    debug/cli/evaluator.py
    debug/cli/offline_analyzer.py
    debug/cli/profile_analyzer_cli.py
    #debug/cli/readline_ui.py
    debug/cli/stepper_cli.py
    debug/cli/tensor_format.py
    debug/cli/ui_factory.py
    debug/examples/debug_errors.py
    debug/examples/debug_fibonacci.py
    debug/examples/debug_keras.py
    #debug/examples/debug_mnist.py
    #debug/examples/debug_tflearn_iris.py
    debug/lib/__init__.py
    debug/lib/common.py
    debug/lib/debug_data.py
    debug/lib/debug_gradients.py
    debug/lib/debug_graphs.py
    debug/lib/debug_service_pb2_grpc.py
    debug/lib/debug_utils.py
    debug/lib/grpc_debug_server.py
    #debug/lib/grpc_debug_test_server.py
    debug/lib/profiling.py
    debug/lib/session_debug_testlib.py
    debug/lib/source_remote.py
    debug/lib/source_utils.py
    debug/lib/stepper.py
    debug/wrappers/dumping_wrapper.py
    debug/wrappers/framework.py
    debug/wrappers/grpc_wrapper.py
    debug/wrappers/hooks.py
    debug/wrappers/local_cli_wrapper.py
    eager/backprop.py
    eager/context.py
    eager/core.py
    eager/execute.py
    eager/execution_callbacks.py
    eager/function.py
    eager/graph_callable.py
    eager/graph_only_ops.py
    eager/imperative_grad.py
    eager/tape.py
    estimator/__init__.py
    estimator/api/__init__.py
    estimator/api/estimator/__init__.py
    estimator/api/estimator/export/__init__.py
    estimator/api/estimator/inputs/__init__.py
    estimator/canned/__init__.py
    estimator/canned/baseline.py
    estimator/canned/boosted_trees.py
    estimator/canned/dnn.py
    estimator/canned/dnn_linear_combined.py
    estimator/canned/dnn_testing_utils.py
    estimator/canned/head.py
    estimator/canned/linear.py
    estimator/canned/linear_testing_utils.py
    estimator/canned/metric_keys.py
    estimator/canned/optimizers.py
    estimator/canned/parsing_utils.py
    estimator/canned/prediction_keys.py
    estimator/estimator.py
    estimator/estimator_lib.py
    estimator/export/__init__.py
    estimator/export/export.py
    estimator/export/export_lib.py
    estimator/export/export_output.py
    estimator/exporter.py
    estimator/gc.py
    estimator/inputs/__init__.py
    estimator/inputs/inputs.py
    estimator/inputs/numpy_io.py
    estimator/inputs/pandas_io.py
    estimator/inputs/queues/__init__.py
    estimator/inputs/queues/feeding_functions.py
    estimator/inputs/queues/feeding_queue_runner.py
    estimator/keras.py
    estimator/model_fn.py
    estimator/run_config.py
    estimator/training.py
    estimator/util.py
    feature_column/__init__.py
    feature_column/feature_column.py
    feature_column/feature_column_lib.py
    feature_column/feature_column_v2.py
    framework/__init__.py
    framework/c_api_util.py
    framework/common_shapes.py
    framework/constant_op.py
    framework/device.py
    framework/dtypes.py
    framework/error_interpolation.py
    framework/errors.py
    framework/errors_impl.py
    framework/fast_tensor_util.pyx
    framework/framework_lib.py
    framework/function.py
    framework/function_def_to_graph.py
    framework/graph_io.py
    framework/graph_to_function_def.py
    framework/graph_util.py
    framework/graph_util_impl.py
    framework/importer.py
    framework/load_library.py
    framework/meta_graph.py
    framework/op_def_library.py
    framework/op_def_registry.py
    framework/ops.py
    framework/random_seed.py
    framework/registry.py
    framework/smart_cond.py
    framework/sparse_tensor.py
    framework/subscribe.py
    framework/tensor_shape.py
    framework/tensor_spec.py
    framework/tensor_util.py
    framework/test_ops.py
    framework/test_util.py
    framework/traceable_stack.py
    framework/versions.py
    grappler/cluster.py
    grappler/controller.py
    grappler/cost_analyzer.py
    grappler/cost_analyzer_tool.py
    grappler/graph_placer.py
    grappler/hierarchical_controller.py
    grappler/item.py
    grappler/model_analyzer.py
    grappler/tf_optimizer.py
    keras/__init__.py
    keras/activations.py
    keras/applications/__init__.py
    keras/applications/densenet.py
    keras/applications/imagenet_utils.py
    keras/applications/inception_resnet_v2.py
    keras/applications/inception_v3.py
    keras/applications/mobilenet.py
    keras/applications/nasnet.py
    keras/applications/resnet50.py
    keras/applications/vgg16.py
    keras/applications/vgg19.py
    keras/applications/xception.py
    keras/backend.py
    keras/callbacks.py
    keras/constraints.py
    keras/datasets/__init__.py
    keras/datasets/boston_housing.py
    keras/datasets/cifar.py
    keras/datasets/cifar10.py
    keras/datasets/cifar100.py
    keras/datasets/fashion_mnist.py
    keras/datasets/imdb.py
    keras/datasets/mnist.py
    keras/datasets/reuters.py
    keras/engine/__init__.py
    keras/engine/base_layer.py
    keras/engine/input_layer.py
    keras/engine/network.py
    keras/engine/saving.py
    keras/engine/sequential.py
    keras/engine/training.py
    keras/engine/training_arrays.py
    keras/engine/training_eager.py
    keras/engine/training_generator.py
    keras/engine/training_utils.py
    keras/estimator/__init__.py
    keras/initializers.py
    keras/layers/__init__.py
    keras/layers/advanced_activations.py
    keras/layers/convolutional.py
    keras/layers/convolutional_recurrent.py
    keras/layers/core.py
    keras/layers/cudnn_recurrent.py
    keras/layers/embeddings.py
    keras/layers/local.py
    keras/layers/merge.py
    keras/layers/noise.py
    keras/layers/normalization.py
    keras/layers/pooling.py
    keras/layers/recurrent.py
    keras/layers/serialization.py
    keras/layers/wrappers.py
    keras/losses.py
    keras/metrics.py
    keras/models.py
    keras/optimizers.py
    keras/preprocessing/__init__.py
    keras/preprocessing/image.py
    keras/preprocessing/sequence.py
    keras/preprocessing/text.py
    keras/regularizers.py
    keras/testing_utils.py
    keras/utils/__init__.py
    keras/utils/conv_utils.py
    keras/utils/data_utils.py
    keras/utils/generic_utils.py
    keras/utils/io_utils.py
    keras/utils/layer_utils.py
    keras/utils/multi_gpu_utils.py
    keras/utils/np_utils.py
    keras/utils/tf_utils.py
    keras/utils/vis_utils.py
    keras/wrappers/__init__.py
    keras/wrappers/scikit_learn.py
    layers/__init__.py
    layers/base.py
    layers/convolutional.py
    layers/core.py
    layers/layers.py
    layers/normalization.py
    layers/pooling.py
    layers/utils.py
    lib/core/__init__.py
    lib/io/__init__.py
    lib/io/file_io.py
    lib/io/python_io.py
    lib/io/tf_record.py
    ops/accumulate_n_benchmark.py
    ops/array_grad.py
    ops/array_ops.py
    ops/batch_norm_benchmark.py
    ops/bitwise_ops.py
    ops/boosted_trees_ops.py
    ops/candidate_sampling_ops.py
    ops/check_ops.py
    ops/clip_ops.py
    ops/collective_ops.py
    ops/concat_benchmark.py
    ops/cond_v2.py
    ops/cond_v2_impl.py
    ops/confusion_matrix.py
    ops/control_flow_grad.py
    ops/control_flow_ops.py
    ops/control_flow_util.py
    ops/conv2d_benchmark.py
    ops/ctc_ops.py
    ops/cudnn_rnn_grad.py
    ops/custom_gradient.py
    ops/data_flow_grad.py
    ops/data_flow_ops.py
    ops/distributions/__init__.py
    ops/distributions/bernoulli.py
    ops/distributions/beta.py
    ops/distributions/bijector.py
    ops/distributions/bijector_impl.py
    ops/distributions/bijector_test_util.py
    ops/distributions/categorical.py
    ops/distributions/dirichlet.py
    ops/distributions/dirichlet_multinomial.py
    ops/distributions/distribution.py
    ops/distributions/distributions.py
    ops/distributions/exponential.py
    ops/distributions/gamma.py
    ops/distributions/identity_bijector.py
    ops/distributions/kullback_leibler.py
    ops/distributions/laplace.py
    ops/distributions/multinomial.py
    ops/distributions/normal.py
    ops/distributions/special_math.py
    ops/distributions/student_t.py
    ops/distributions/transformed_distribution.py
    ops/distributions/uniform.py
    ops/distributions/util.py
    ops/embedding_ops.py
    ops/functional_ops.py
    ops/gen_array_ops.py
    ops/gen_audio_ops.py
    ops/gen_batch_ops.py
    ops/gen_bitwise_ops.py
    ops/gen_boosted_trees_ops.py
    ops/gen_candidate_sampling_ops.py
    ops/gen_checkpoint_ops.py
    ops/gen_collective_ops.py
    ops/gen_control_flow_ops.py
    ops/gen_ctc_ops.py
    ops/gen_cudnn_rnn_ops.py
    ops/gen_data_flow_ops.py
    ops/gen_dataset_ops.py
    ops/gen_functional_ops.py
    ops/gen_image_ops.py
    ops/gen_io_ops.py
    ops/gen_linalg_ops.py
    ops/gen_list_ops.py
    ops/gen_logging_ops.py
    ops/gen_lookup_ops.py
    ops/gen_manip_ops.py
    ops/gen_math_ops.py
    ops/gen_nn_ops.py
    ops/gen_parsing_ops.py
    ops/gen_random_ops.py
    ops/gen_resource_variable_ops.py
    ops/gen_script_ops.py
    ops/gen_sdca_ops.py
    ops/gen_set_ops.py
    ops/gen_sparse_ops.py
    ops/gen_spectral_ops.py
    ops/gen_state_ops.py
    ops/gen_string_ops.py
    ops/gen_summary_ops.py
    ops/gen_user_ops.py
    ops/gradient_checker.py
    ops/gradients.py
    ops/gradients_impl.py
    ops/histogram_ops.py
    ops/image_grad.py
    ops/image_ops.py
    ops/image_ops_impl.py
    ops/init_ops.py
    ops/initializers_ns.py
    ops/inplace_ops.py
    ops/io_ops.py
    ops/linalg/__init__.py
    ops/linalg/linalg.py
    ops/linalg/linalg_impl.py
    ops/linalg/linear_operator.py
    ops/linalg/linear_operator_block_diag.py
    ops/linalg/linear_operator_circulant.py
    ops/linalg/linear_operator_composition.py
    ops/linalg/linear_operator_diag.py
    ops/linalg/linear_operator_full_matrix.py
    ops/linalg/linear_operator_identity.py
    ops/linalg/linear_operator_kronecker.py
    ops/linalg/linear_operator_low_rank_update.py
    ops/linalg/linear_operator_lower_triangular.py
    ops/linalg/linear_operator_test_util.py
    ops/linalg/linear_operator_util.py
    ops/linalg_grad.py
    ops/linalg_ops.py
    ops/linalg_ops_impl.py
    ops/list_ops.py
    ops/logging_ops.py
    ops/lookup_ops.py
    ops/losses/__init__.py
    ops/losses/losses.py
    ops/losses/losses_impl.py
    ops/losses/util.py
    ops/manip_grad.py
    ops/manip_ops.py
    ops/math_grad.py
    ops/math_ops.py
    ops/matmul_benchmark.py
    ops/metrics.py
    ops/metrics_impl.py
    ops/nn.py
    ops/nn_grad.py
    ops/nn_impl.py
    ops/nn_ops.py
    ops/numerics.py
    ops/parallel_for/__init__.py
    ops/parallel_for/control_flow_ops.py
    ops/parallel_for/gradients.py
    ops/parallel_for/pfor.py
    ops/parsing_ops.py
    ops/partitioned_variables.py
    ops/random_grad.py
    ops/random_ops.py
    ops/resource_variable_ops.py
    ops/resources.py
    ops/rnn.py
    ops/rnn_cell.py
    ops/rnn_cell_impl.py
    ops/script_ops.py
    ops/sdca_ops.py
    ops/session_ops.py
    ops/sets.py
    ops/sets_impl.py
    ops/sparse_grad.py
    ops/sparse_ops.py
    ops/special_math_ops.py
    ops/spectral_grad.py
    ops/spectral_ops.py
    ops/spectral_ops_test_util.py
    ops/split_benchmark.py
    ops/standard_ops.py
    ops/state_grad.py
    ops/state_ops.py
    ops/string_ops.py
    ops/summary_op_util.py
    ops/summary_ops.py
    ops/summary_ops_v2.py
    ops/template.py
    ops/tensor_array_grad.py
    ops/tensor_array_ops.py
    ops/transpose_benchmark.py
    ops/variable_scope.py
    ops/variables.py
    ops/weights_broadcast_ops.py
    platform/app.py
    platform/benchmark.py
    platform/build_info.py
    platform/control_imports.py
    platform/flags.py
    platform/gfile.py
    platform/googletest.py
    #platform/parameterized.py
    platform/resource_loader.py
    platform/self_check.py
    platform/status_bar.py
    platform/sysconfig.py
    platform/test.py
    platform/tf_logging.py
    profiler/__init__.py
    profiler/internal/flops_registry.py
    profiler/internal/model_analyzer_testlib.py
    profiler/model_analyzer.py
    profiler/option_builder.py
    #profiler/pprof_profiler.py
    profiler/profile_context.py
    profiler/profiler.py
    profiler/tfprof_logger.py
    pywrap_dlopen_global_flags.py
    pywrap_tensorflow.py
    pywrap_tensorflow_internal.py
    saved_model/builder.py
    saved_model/builder_impl.py
    saved_model/constants.py
    saved_model/loader.py
    saved_model/loader_impl.py
    saved_model/main_op.py
    saved_model/main_op_impl.py
    saved_model/saved_model.py
    saved_model/signature_constants.py
    saved_model/signature_def_utils.py
    saved_model/signature_def_utils_impl.py
    saved_model/simple_save.py
    saved_model/tag_constants.py
    saved_model/utils.py
    saved_model/utils_impl.py
    summary/__init__.py
    summary/plugin_asset.py
    summary/summary.py
    summary/summary_iterator.py
    summary/text_summary.py
    summary/writer/event_file_writer.py
    summary/writer/event_file_writer_v2.py
    summary/writer/writer.py
    summary/writer/writer_cache.py
    tools/api/generator/create_python_api.py
    tools/api/generator/doc_srcs.py
    tools/freeze_graph.py
    tools/import_pb_to_tensorboard.py
    tools/inspect_checkpoint.py
    tools/optimize_for_inference.py
    tools/optimize_for_inference_lib.py
    tools/print_selective_registration_header.py
    #tools/saved_model_cli.py
    tools/saved_model_utils.py
    tools/selective_registration_header_lib.py
    tools/strip_unused.py
    tools/strip_unused_lib.py
    training/__init__.py
    training/adadelta.py
    training/adagrad.py
    training/adagrad_da.py
    training/adam.py
    training/basic_loops.py
    training/basic_session_run_hooks.py
    training/checkpoint_ops.py
    training/checkpoint_state.proto
    training/checkpoint_utils.py
    training/checkpointable/base.py
    training/checkpointable/data_structures.py
    training/checkpointable/layer_utils.py
    training/checkpointable/tracking.py
    training/checkpointable/util.py
    training/coordinator.py
    training/device_setter.py
    training/device_util.py
    training/distribute.py
    training/evaluation.py
    training/ftrl.py
    training/gen_training_ops.py
    training/gradient_descent.py
    training/input.py
    training/learning_rate_decay.py
    training/momentum.py
    training/monitored_session.py
    training/moving_averages.py
    training/optimizer.py
    training/proximal_adagrad.py
    training/proximal_gradient_descent.py
    training/queue_runner.py
    training/queue_runner_impl.py
    training/rmsprop.py
    training/saveable_object.py
    training/saver.py
    training/saver_test_utils.py
    training/server_lib.py
    training/session_manager.py
    training/session_run_hook.py
    training/slot_creator.py
    training/summary_io.py
    training/supervisor.py
    training/sync_replicas_optimizer.py
    training/tensorboard_logging.py
    training/training.py
    training/training_ops.py
    training/training_util.py
    training/warm_starting_util.py
    user_ops/__init__.py
    user_ops/user_ops.py
    util/__init__.py
    util/all_util.py
    util/compat.py
    util/compat_internal.py
    util/decorator_utils.py
    util/deprecation.py
    util/example_parser_configuration.py
    util/function_utils.py
    util/future_api.py
    util/is_in_graph_mode.py
    util/keyword_args.py
    util/lazy_loader.py
    util/lock_util.py
    util/nest.py
    util/protobuf/__init__.py
    util/protobuf/compare.py
    util/serialization.py
    util/tf_contextlib.py
    util/tf_decorator.py
    util/tf_export.py
    util/tf_inspect.py
    util/tf_should_use.py
    util/tf_stack.py
)

SRCS(
    tensorflow/c/checkpoint_reader.cc
    tensorflow/c/python_api.cc
    tensorflow/cc/framework/cc_op_gen.cc
    tensorflow/cc/training/coordinator.cc
    tensorflow/cc/training/queue_runner.cc
    tensorflow/core/distributed_runtime/rpc/grpc_remote_master.cc
    tensorflow/core/distributed_runtime/rpc/grpc_rpc_factory.cc
    GLOBAL tensorflow/core/distributed_runtime/rpc/grpc_rpc_factory_registration.cc
    tensorflow/core/distributed_runtime/rpc/grpc_session.cc
    tensorflow/core/grappler/clusters/single_machine.cc
    tensorflow/core/grappler/costs/analytical_cost_estimator.cc
    tensorflow/core/grappler/costs/measuring_cost_estimator.cc
    tensorflow/core/grappler/costs/robust_stats.cc
    tensorflow/core/kernels/hexagon/graph_transfer_utils.cc
    tensorflow/core/kernels/hexagon/graph_transferer.cc
    tensorflow/core/kernels/hexagon/hexagon_control_wrapper.cc
    tensorflow/core/kernels/hexagon/hexagon_ops_definitions.cc
    GLOBAL tensorflow/core/kernels/hexagon/hexagon_rewriter_transform.cc
    tensorflow/core/kernels/hexagon/soc_interface.cc
    GLOBAL tensorflow/core/kernels/remote_fused_graph_rewriter_transform.cc
    GLOBAL tensorflow/core/ops/script_ops.cc
    tensorflow/core/profiler/internal/advisor/internal_checker_runner_dummy.cc
    tensorflow/core/profiler/internal/print_model_analysis.cc
    tensorflow/core/profiler/internal/tfprof_code.cc
    tensorflow/core/profiler/internal/tfprof_graph.cc
    tensorflow/core/profiler/internal/tfprof_node.cc
    tensorflow/core/profiler/internal/tfprof_node_show.cc
    tensorflow/core/profiler/internal/tfprof_op.cc
    tensorflow/core/profiler/internal/tfprof_scope.cc
    tensorflow/core/profiler/internal/tfprof_show.cc
    tensorflow/core/profiler/internal/tfprof_show_multi.cc
    tensorflow/core/profiler/internal/tfprof_stats.cc
    tensorflow/core/profiler/internal/tfprof_tensor.cc
    tensorflow/core/profiler/internal/tfprof_timeline.cc
    tensorflow/core/profiler/internal/tfprof_utils.cc
    tensorflow/core/profiler/tfprof_options.cc
    GLOBAL tensorflow/python/client/test_construction_fails_op.cc
    tensorflow/python/client/tf_session_helper.cc
    tensorflow/python/eager/pywrap_tensor.cc
    tensorflow/python/eager/pywrap_tfe_src.cc
    tensorflow/python/framework/cpp_shape_inference.cc
    GLOBAL tensorflow/python/framework/test_ops.cc
    tensorflow/python/grappler/cost_analyzer.cc
    tensorflow/python/grappler/model_analyzer.cc
    tensorflow/python/lib/core/bfloat16.cc
    tensorflow/python/lib/core/ndarray_tensor.cc
    tensorflow/python/lib/core/ndarray_tensor_bridge.cc
    tensorflow/python/lib/core/numpy.cc
    tensorflow/python/lib/core/py_exception_registry.cc
    GLOBAL tensorflow/python/lib/core/py_func.cc
    tensorflow/python/lib/core/py_seq_tensor.cc
    tensorflow/python/lib/core/py_util.cc
    tensorflow/python/lib/core/safe_ptr.cc
    tensorflow/python/lib/io/py_record_reader.cc
    tensorflow/python/lib/io/py_record_writer.cc
    tensorflow/python/pywrap_tensorflow_internal.swg.cc
    tensorflow/python/util/kernel_registry.cc
    tensorflow/python/util/util.cc
    GLOBAL tensorflow/tools/graph_transforms/add_default_attributes.cc
    GLOBAL tensorflow/tools/graph_transforms/backports.cc
    tensorflow/tools/graph_transforms/file_utils.cc
    GLOBAL tensorflow/tools/graph_transforms/flatten_atrous.cc
    GLOBAL tensorflow/tools/graph_transforms/fold_batch_norms.cc
    GLOBAL tensorflow/tools/graph_transforms/fold_constants_lib.cc
    GLOBAL tensorflow/tools/graph_transforms/fold_old_batch_norms.cc
    GLOBAL tensorflow/tools/graph_transforms/freeze_requantization_ranges.cc
    GLOBAL tensorflow/tools/graph_transforms/fuse_convolutions.cc
    GLOBAL tensorflow/tools/graph_transforms/insert_logging.cc
    GLOBAL tensorflow/tools/graph_transforms/obfuscate_names.cc
    GLOBAL tensorflow/tools/graph_transforms/quantize_nodes.cc
    GLOBAL tensorflow/tools/graph_transforms/quantize_weights.cc
    GLOBAL tensorflow/tools/graph_transforms/remove_attribute.cc
    GLOBAL tensorflow/tools/graph_transforms/remove_control_dependencies.cc
    GLOBAL tensorflow/tools/graph_transforms/remove_device.cc
    GLOBAL tensorflow/tools/graph_transforms/remove_nodes.cc
    GLOBAL tensorflow/tools/graph_transforms/rename_attribute.cc
    GLOBAL tensorflow/tools/graph_transforms/rename_op.cc
    GLOBAL tensorflow/tools/graph_transforms/round_weights.cc
    GLOBAL tensorflow/tools/graph_transforms/set_device.cc
    GLOBAL tensorflow/tools/graph_transforms/sort_by_execution_order.cc
    GLOBAL tensorflow/tools/graph_transforms/sparsify_gather.cc
    GLOBAL tensorflow/tools/graph_transforms/strip_unused_nodes.cc
    tensorflow/tools/graph_transforms/transform_graph.cc
    tensorflow/tools/graph_transforms/transform_utils.cc
)

RUN_PROGRAM(
    contrib/tools/swig -c++ -python -module pywrap_tensorflow_internal -o pywrap_tensorflow_internal.swg.cc
        -outdir ${ARCADIA_BUILD_ROOT}/contrib/deprecated/tf/tensorflow/python
        -ltensorflow/python/client/device_lib.i -ltensorflow/python/client/events_writer.i
        -ltensorflow/python/client/tf_session.i -ltensorflow/python/client/tf_sessionrun_wrapper.i
        -ltensorflow/python/framework/cpp_shape_inference.i -ltensorflow/python/framework/python_op_gen.i
        -ltensorflow/python/grappler/cluster.i -ltensorflow/python/grappler/cost_analyzer.i
        -ltensorflow/python/grappler/item.i -ltensorflow/python/grappler/model_analyzer.i
        -ltensorflow/python/grappler/tf_optimizer.i -ltensorflow/python/lib/core/bfloat16.i
        -ltensorflow/python/lib/core/py_exception_registry.i -ltensorflow/python/lib/core/py_func.i
        -ltensorflow/python/lib/core/strings.i -ltensorflow/python/lib/io/file_io.i
        -ltensorflow/python/lib/io/py_record_reader.i -ltensorflow/python/lib/io/py_record_writer.i
        -ltensorflow/python/platform/base.i -ltensorflow/python/platform/stacktrace_handler.i
        -ltensorflow/python/pywrap_tfe.i -ltensorflow/python/training/quantize_training.i
        -ltensorflow/python/training/server_lib.i -ltensorflow/python/util/kernel_registry.i
        -ltensorflow/python/util/port.i -ltensorflow/python/util/py_checkpoint_reader.i
        -ltensorflow/python/util/stat_summarizer.i -ltensorflow/python/util/tfprof.i
        -ltensorflow/python/util/transform_graph.i -ltensorflow/python/util/util.i
        tensorflow/python/tensorflow.i
    CWD ${ARCADIA_ROOT}/contrib/deprecated/tf
    IN tensorflow.i
    OUTPUT_INCLUDES Python.h tensorflow/c/checkpoint_reader.h tensorflow/core/common_runtime/device_factory.h
        tensorflow/core/common_runtime/device.h tensorflow/core/distributed_runtime/server_lib.h
        tensorflow/core/framework/device_attributes.pb.h tensorflow/core/framework/device_base.h
        tensorflow/core/framework/graph.pb.h tensorflow/core/framework/kernel_def.pb.h
        tensorflow/core/framework/memory_types.h tensorflow/core/framework/node_def_util.h
        tensorflow/core/framework/session_state.h tensorflow/core/framework/step_stats.pb.h
        tensorflow/core/framework/types.h tensorflow/core/graph/quantize_training.h
        tensorflow/core/grappler/clusters/cluster.h tensorflow/core/grappler/clusters/single_machine.h
        tensorflow/core/grappler/clusters/utils.h tensorflow/core/grappler/clusters/virtual_cluster.h
        tensorflow/core/grappler/costs/graph_memory.h tensorflow/core/grappler/costs/graph_properties.h
        tensorflow/core/grappler/costs/measuring_cost_estimator.h
        tensorflow/core/grappler/costs/op_performance_data.pb.h tensorflow/core/grappler/costs/utils.h
        tensorflow/core/grappler/devices.h tensorflow/core/grappler/grappler_item_builder.h
        tensorflow/core/grappler/grappler_item.h tensorflow/core/grappler/optimizers/meta_optimizer.h
        tensorflow/core/grappler/utils.h tensorflow/core/grappler/utils/topological_sort.h
        tensorflow/core/lib/core/error_codes.pb.h tensorflow/core/lib/core/errors.h
        tensorflow/core/lib/core/status.h tensorflow/core/lib/core/stringpiece.h
        tensorflow/core/lib/io/buffered_inputstream.h tensorflow/core/lib/io/inputstream_interface.h
        tensorflow/core/lib/io/path.h tensorflow/core/lib/io/random_inputstream.h
        tensorflow/core/lib/strings/strcat.h tensorflow/core/lib/strings/stringprintf.h
        tensorflow/core/platform/env.h tensorflow/core/platform/file_statistics.h
        tensorflow/core/platform/file_system.h tensorflow/core/platform/stacktrace_handler.h
        tensorflow/core/platform/types.h tensorflow/core/profiler/internal/print_model_analysis.h
        tensorflow/core/protobuf/config.pb.h tensorflow/core/protobuf/device_properties.pb.h
        tensorflow/core/protobuf/meta_graph.pb.h tensorflow/core/protobuf/rewriter_config.pb.h
        tensorflow/core/public/session_options.h tensorflow/core/public/version.h
        tensorflow/core/util/event.pb.h tensorflow/core/util/events_writer.h tensorflow/core/util/port.h
        tensorflow/core/util/stat_summarizer.h tensorflow/c/python_api.h tensorflow/c/tf_status_helper.h
        tensorflow/python/client/tf_session_helper.h tensorflow/python/eager/pywrap_tfe.h
        tensorflow/python/framework/cpp_shape_inference.h tensorflow/python/framework/python_op_gen.h
        tensorflow/python/grappler/cost_analyzer.h tensorflow/python/grappler/model_analyzer.h
        tensorflow/python/lib/core/bfloat16.h tensorflow/python/lib/core/ndarray_tensor.h
        tensorflow/python/lib/core/py_exception_registry.h tensorflow/python/lib/core/py_func.h
        tensorflow/python/lib/core/safe_ptr.h tensorflow/python/lib/io/py_record_reader.h
        tensorflow/python/lib/io/py_record_writer.h tensorflow/python/util/kernel_registry.h
        tensorflow/python/util/util.h tensorflow/tools/graph_transforms/transform_graph.h
    OUT_NOAUTO pywrap_tensorflow_internal.swg.cc pywrap_tensorflow_internal.py
)

END()
