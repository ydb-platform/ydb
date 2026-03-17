LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

SUBSCRIBER(g:cpp-contrib)

NO_COMPILER_WARNINGS()

SRCDIR(contrib/deprecated/tf)

ADDINCL(
    contrib/deprecated/tf
)

PEERDIR(
    contrib/deprecated/tf/proto
    contrib/libs/nvidia/tensor_rt
)

CFLAGS(
    -DGOOGLE_TENSORRT=1
    -DGOOGLE_CUDA=1
)

SRCS(
    GLOBAL kernels/trt_engine_op.cc
    GLOBAL ops/trt_engine_op.cc
    convert/convert_graph.cc
    convert/convert_nodes.cc
    convert/trt_optimization_pass.cc
    convert/utils.cc
    log/trt_logger.cc
    plugin/trt_plugin.cc
    plugin/trt_plugin_factory.cc
    plugin/trt_plugin_utils.cc
    resources/trt_allocator.cc
    resources/trt_int8_calibrator.cc
    resources/trt_resource_manager.cc
    segment/segment.cc
    shape_fn/trt_shfn.cc
)

END()
