from unittest import TestCase
from os.path import dirname, realpath, join
from pickle import load

# These are simple tests to check for basic compatibility between versions
# of this library.
# - Compatibility between C-ext and pure-python is required
# - Old versions of the pickle file should be readable
# - Old versions of the library need not be able to read new pickle files
#   (bonus points if they are though!)
#
# For ease, creating a pickle file only needs three nodes; the last with
# data:
#
# >>> import radix
# >>> import pickle
# >>> tree = radix.Radix()
# >>> tree.add('10.0.1.0/24')
# >>> tree.add('10.0.2.0/24')
# >>> node = tree.add('10.0.3.0/24')
# >>> node.data['one'] = 1
# >>> with open('radix-ver-mode.pkl', 'wb') as f:
# ...     pickle.dump(tree, f)


class LoadRadixPickle(TestCase):
    def setUp(self):
        import yatest.common as yc
        self.data_dir = join(dirname(realpath(yc.source_path(__file__))), 'data')

    def _check_file(self, file_name):
        with open(join(self.data_dir, file_name), 'rb') as f:
            tree = load(f)
        self.assertEqual(len(tree.nodes()), 3)
        nodes = ['10.0.1.0/24', '10.0.2.0/24', '10.0.3.0/24']
        for actual, expected in zip(tree.nodes(), nodes):
            self.assertEqual(actual.prefix, expected)
        self.assertTrue('one' in actual.data)
        self.assertEqual(actual.data['one'], 1)

    def test_radix_0_5_c_ext(self):
        self._check_file('radix-0.5-c_ext.pkl')

    def test_radix_0_6_c_ext(self):
        self._check_file('radix-0.6-c_ext.pkl')

    def test_radix_0_6_no_ext(self):
        self._check_file('radix-0.6-no_ext.pkl')
