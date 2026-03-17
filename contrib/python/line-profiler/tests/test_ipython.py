import unittest


class TestIPython(unittest.TestCase):
    def test_init(self):
        """
        CommandLine:
            pytest -k test_init -s -v
        """
        try:
            from IPython.testing.globalipapp import get_ipython
        except ImportError:
            import pytest
            pytest.skip()

        ip = get_ipython()
        ip.run_line_magic('load_ext', 'line_profiler')
        ip.run_cell(raw_cell='def func():\n    return 2**20')
        lprof = ip.run_line_magic('lprun', '-r -f func func()')

        timings = lprof.get_stats().timings
        self.assertEqual(len(timings), 1)  # 1 function

        func_data, lines_data = next(iter(timings.items()))
        print(f'func_data={func_data}')
        print(f'lines_data={lines_data}')
        self.assertEqual(func_data[1], 1)  # lineno of the function
        self.assertEqual(func_data[2], "func")  # function name
        self.assertEqual(len(lines_data), 1)  # 1 line of code
        self.assertEqual(lines_data[0][0], 2)  # lineno
        self.assertEqual(lines_data[0][1], 1)  # hits
