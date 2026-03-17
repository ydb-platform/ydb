

def exception_test(assert_check_fn):
    try:
        def exception_wrapper(*arg, **kwarg):
            assert_check_fn(*arg, **kwarg)
    except Exception as e:
        pytest.failed(e)