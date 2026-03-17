PY3TEST()

PEERDIR(
    contrib/python/pathy
    contrib/python/hypothesis
    contrib/python/mock
    contrib/python/thinc
)

SRCDIR(contrib/python/thinc)

TEST_SRCS(
    thinc/extra/tests/__init__.py
    thinc/tests/__init__.py
    thinc/tests/backends/__init__.py
    thinc/tests/backends/test_mem.py
    thinc/tests/backends/test_ops.py
    thinc/tests/conftest.py
    thinc/tests/extra/__init__.py
    thinc/tests/extra/test_beam_search.py
    thinc/tests/layers/__init__.py
    thinc/tests/layers/test_basic_tagger.py
    thinc/tests/layers/test_combinators.py
    thinc/tests/layers/test_feed_forward.py
    thinc/tests/layers/test_hash_embed.py
    thinc/tests/layers/test_layers_api.py
    thinc/tests/layers/test_linear.py
    thinc/tests/layers/test_lstm.py
    thinc/tests/layers/test_mnist.py
    thinc/tests/layers/test_mxnet_wrapper.py
    thinc/tests/layers/test_pytorch_wrapper.py
    thinc/tests/layers/test_reduce.py
    thinc/tests/layers/test_shim.py
    thinc/tests/layers/test_softmax.py
    thinc/tests/layers/test_sparse_linear.py
    thinc/tests/layers/test_tensorflow_wrapper.py
    thinc/tests/layers/test_transforms.py
    thinc/tests/layers/test_uniqued.py
    thinc/tests/layers/test_with_debug.py
    thinc/tests/layers/test_with_transforms.py
    thinc/tests/model/__init__.py
    thinc/tests/model/test_model.py
    thinc/tests/model/test_validation.py
    # thinc/tests/mypy/__init__.py
    # thinc/tests/mypy/modules/__init__.py
    # thinc/tests/mypy/modules/fail_no_plugin.py
    # thinc/tests/mypy/modules/fail_plugin.py
    # thinc/tests/mypy/modules/success_no_plugin.py
    # thinc/tests/mypy/modules/success_plugin.py
    # thinc/tests/mypy/test_mypy.py
    thinc/tests/regression/__init__.py
    thinc/tests/regression/issue519/__init__.py
    thinc/tests/regression/issue519/program.py
    thinc/tests/regression/issue519/test_issue519.py
    thinc/tests/regression/test_issue208.py
    thinc/tests/shims/__init__.py
    thinc/tests/shims/test_pytorch_grad_scaler.py
    thinc/tests/strategies.py
    thinc/tests/test_config.py
    thinc/tests/test_examples.py
    thinc/tests/test_indexing.py
    thinc/tests/test_initializers.py
    thinc/tests/test_loss.py
    thinc/tests/test_optimizers.py
    thinc/tests/test_schedules.py
    thinc/tests/test_serialize.py
    thinc/tests/test_types.py
    thinc/tests/test_util.py
    thinc/tests/util.py
)

NO_LINT()

END()
