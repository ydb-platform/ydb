PROTO_LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(1.10.1)

LICENSE(Apache-2.0)

EXCLUDE_TAGS(
    GO_PROTO
    JAVA_PROTO
)

PROTO_NAMESPACE(
    GLOBAL
    contrib/deprecated/tf
)

PY_NAMESPACE(.)

SRCDIR(contrib/deprecated/tf)

SRCS(
    tensorflow/compiler/tf2xla/host_compute_metadata.proto
    tensorflow/compiler/tf2xla/tf2xla.proto
    tensorflow/compiler/xla/service/hlo.proto
    tensorflow/compiler/xla/service/hlo_profile_printer_data.proto
    tensorflow/compiler/xla/xla.proto
    tensorflow/compiler/xla/xla_data.proto
    tensorflow/contrib/tpu/proto/tpu_embedding_config.proto
    tensorflow/core/debug/debugger_event_metadata.proto
    tensorflow/core/example/example.proto
    tensorflow/core/example/example_parser_configuration.proto
    tensorflow/core/example/feature.proto
    tensorflow/core/framework/allocation_description.proto
    tensorflow/core/framework/api_def.proto
    tensorflow/core/framework/attr_value.proto
    tensorflow/core/framework/cost_graph.proto
    tensorflow/core/framework/device_attributes.proto
    tensorflow/core/framework/function.proto
    tensorflow/core/framework/graph.proto
    tensorflow/core/framework/graph_transfer_info.proto
    tensorflow/core/framework/iterator.proto
    tensorflow/core/framework/kernel_def.proto
    tensorflow/core/framework/log_memory.proto
    tensorflow/core/framework/node_def.proto
    tensorflow/core/framework/op_def.proto
    tensorflow/core/framework/reader_base.proto
    tensorflow/core/framework/remote_fused_graph_execute_info.proto
    tensorflow/core/framework/resource_handle.proto
    tensorflow/core/framework/step_stats.proto
    tensorflow/core/framework/summary.proto
    tensorflow/core/framework/tensor.proto
    tensorflow/core/framework/tensor_description.proto
    tensorflow/core/framework/tensor_shape.proto
    tensorflow/core/framework/tensor_slice.proto
    tensorflow/core/framework/types.proto
    tensorflow/core/framework/variable.proto
    tensorflow/core/framework/versions.proto
    tensorflow/core/grappler/costs/op_performance_data.proto
    tensorflow/core/kernels/boosted_trees/boosted_trees.proto
    tensorflow/core/lib/core/error_codes.proto
    tensorflow/core/profiler/profile.proto
    tensorflow/core/profiler/tfprof_log.proto
    tensorflow/core/profiler/tfprof_options.proto
    tensorflow/core/profiler/tfprof_output.proto
    tensorflow/core/protobuf/arcadia.proto
    tensorflow/core/protobuf/checkpointable_object_graph.proto
    tensorflow/core/protobuf/cluster.proto
    tensorflow/core/protobuf/config.proto
    tensorflow/core/protobuf/control_flow.proto
    tensorflow/core/protobuf/debug.proto
    tensorflow/core/protobuf/device_properties.proto
    tensorflow/core/protobuf/eager_service.proto
    tensorflow/core/protobuf/master.proto
    tensorflow/core/protobuf/meta_graph.proto
    tensorflow/core/protobuf/named_tensor.proto
    tensorflow/core/protobuf/queue_runner.proto
    tensorflow/core/protobuf/rewriter_config.proto
    tensorflow/core/protobuf/saved_model.proto
    tensorflow/core/protobuf/saver.proto
    tensorflow/core/protobuf/tensor_bundle.proto
    tensorflow/core/protobuf/tensorflow_server.proto
    tensorflow/core/protobuf/transport_options.proto
    tensorflow/core/protobuf/worker.proto
    tensorflow/core/util/event.proto
    tensorflow/core/util/memmapped_file_system.proto
    tensorflow/core/util/saved_tensor_slice.proto
    tensorflow/core/util/test_log.proto
    tensorflow/python/framework/cpp_shape_inference.proto
)

IF (TENSORFLOW_WITH_CUDA)
    SRCS(
        tensorflow/compiler/xla/service/gpu/backend_configs.proto
    )
ENDIF()

END()
