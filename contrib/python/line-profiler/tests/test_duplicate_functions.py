def test_duplicate_function():
    """
    Test from https://github.com/pyutils/line_profiler/issues/232
    """
    import line_profiler

    class C:
        def f1(self):
            pass

        def f2(self):
            pass

        def f3(self):
            pass

    profile = line_profiler.LineProfiler()
    profile(C.f1)
    profile(C.f2)
    profile(C.f3)
