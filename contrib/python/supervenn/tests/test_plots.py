import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import unittest

from supervenn._plots import supervenn


def rectangle_present(ax, expected_bbox_bounds):
    """
    Check whether Axes ax contains a matplotlib.patches.Rectangle with given bbox bounds
    :param ax: matplolib Axes
    :param expected_bbox_bounds: expected bbox bounds [min_x, min_y, max_x, max_y]
    :return: True / False
    """
    found = False
    for child in ax.get_children():
        if isinstance(child, Rectangle) and child.get_bbox().bounds == expected_bbox_bounds:
            found = True
            break
    return found


class TestSupervenn(unittest.TestCase):

    def test_no_sets(self):
        sets = []
        with self.assertRaises(ValueError):
            supervenn(sets)
        plt.close()

    def test_empty_sets(self):
        sets = [set(), set()]
        with self.assertRaises(ValueError):
            supervenn(sets)
        plt.close()

    def test_no_side_plots(self):
        """
        Test that the supervenn() runs without side plots, produces only one axes, and if given a list of one set
        produces a rectangle of correct dimensions.
        """
        set_size = 3
        sets = [set(range(set_size))]
        supervenn_plot = supervenn(sets, side_plots=False)
        self.assertEqual(list(supervenn_plot.axes), ['main'])
        self.assertEqual(len(supervenn_plot.figure.axes), 1)
        expected_rectangle_bounds = (0.0, 0.0, float(set_size), 1.0)
        self.assertTrue(rectangle_present(supervenn_plot.axes['main'], expected_rectangle_bounds))
        plt.close()

    def test_with_side_plots(self):
        """
        Test that supervenn() runs with side plots, produces four axes, and if given a list of one set
        produces a rectangle of correct dimensions.
        """
        set_size = 3
        sets = [set(range(set_size))]
        supervenn_plot = supervenn(sets, side_plots=True)
        self.assertEqual(set(supervenn_plot.axes), {'main', 'right_side_plot', 'top_side_plot', 'unused'})
        expected_rectangle_bounds = (0.0, 0.0, float(set_size), 1.0)  # same for all the three axes actually!
        for ax_name, ax in supervenn_plot.axes.items():
            if ax_name == 'unused':
                continue
            self.assertTrue(rectangle_present(ax, expected_rectangle_bounds))
        plt.close()

    def test_with_one_side_plot(self):
        set_size = 3
        sets = [set(range(set_size))]
        for side_plots in ('top', 'right'):
            supervenn_plot = supervenn(sets, side_plots=side_plots)
            self.assertEqual(set(supervenn_plot.axes), {'main', '{}_side_plot'.format(side_plots)})
            expected_rectangle_bounds = (0.0, 0.0, float(set_size), 1.0)  # same for both axes
            for ax_name, ax in supervenn_plot.axes.items():
                self.assertTrue(rectangle_present(ax, expected_rectangle_bounds))
            plt.close()


if __name__ == '__main__':
    unittest.main()
