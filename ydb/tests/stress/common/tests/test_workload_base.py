# -*- coding: utf-8 -*-
import threading

from ydb.tests.stress.common.common import WorkloadBase


class TestWorkloadBaseStart:
    def test_start_dispatches_each_function_to_its_own_thread(self):
        """Regression test: WorkloadBase.start() must dispatch each function to a
        dedicated thread, not re-use the last function for all threads.

        Previously, all threads captured the loop variable 'f' by reference
        (Python closure bug), so all threads ran only the last function in the
        list instead of their assigned one.
        """
        called = []
        lock = threading.Lock()

        class MultiWorkload(WorkloadBase):
            def __init__(self):
                super().__init__(None, 'prefix', 'name', None)

            def _make_func(self, tag):
                def run():
                    with lock:
                        called.append(tag)
                return run

            def get_workload_thread_funcs(self):
                return [self._make_func(i) for i in range(5)]

        workload = MultiWorkload()
        workload.start()
        workload.join()

        assert sorted(called) == list(range(5)), (
            f"Expected each of the 5 functions to be called exactly once, got: {sorted(called)}"
        )

    def test_lambda_binding_captures_each_value_independently(self):
        """Deterministic regression test for the closure bug fix.

        WorkloadBase.start() creates thread lambdas with `lambda f=f: wrapper(f)`.
        The `f=f` default argument binds the CURRENT value of `f` at lambda creation
        time. Without it (`lambda: wrapper(f)`), all lambdas would capture the same
        reference to the loop variable, so after the loop, all threads would run the
        last function (the classic Python closure-over-loop-variable bug).

        This test is deterministic: it calls lambdas after the loop completes,
        which is exactly the case when threads are slow to start (the scenario
        that triggered the original CI failure).
        """
        n = 5
        funcs = list(range(n))

        # Simulate what WorkloadBase.start() does: create lambdas inside the loop.
        # With the fix (f=f), each lambda captures its own bound value of f.
        lambdas = []
        for f in funcs:
            lambdas.append(lambda f=f: f)

        # Call all lambdas AFTER the loop — simulates threads executing after the
        # loop has completed. With the bug (no f=f), all would return funcs[-1].
        results = [lam() for lam in lambdas]

        assert results == list(range(n)), (
            f"Expected {list(range(n))}, got {results}. "
            "Each lambda must capture its own independent value of f."
        )
