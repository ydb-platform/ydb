from collections import OrderedDict, namedtuple
from functools import cmp_to_key, reduce
from itertools import combinations

from .pairs_storage import PairsStorage, key


class Item:
    @property
    def id(self):
        return self.__item_id

    @property
    def value(self):
        return self.__value

    @property
    def weights(self):
        return self.__weights

    def __init__(self, item_id, value):
        self.__item_id = item_id
        self.__value = value
        self.set_weights([])

    def __str__(self):
        return str(self.__dict__)

    def set_weights(self, weights):
        self.__weights = weights


def get_max_combination_number(prameter_matrix, n):
    param_len_list = [len(value_list) for value_list in prameter_matrix]

    return sum([reduce(lambda x, y: x * y, z) for z in combinations(param_len_list, n)])


def cmp_item(lhs, rhs):
    if lhs.weights == rhs.weights:
        return 0

    return -1 if lhs.weights < rhs.weights else 1


class AllPairs:
    def __init__(self, parameters, filter_func=lambda x: True, previously_tested=None, n=2):
        """
        TODO: check that input arrays are:
            - (optional) has no duplicated values inside single array / or compress such values
        """

        if not previously_tested:
            previously_tested = [[]]

        self.__validate_parameter(parameters)

        self.__is_ordered_dict_param = isinstance(parameters, OrderedDict)
        self.__param_name_list = self.__extract_param_name_list(parameters)
        self.__pairs_class = namedtuple("Pairs", self.__param_name_list)

        self.__filter_func = filter_func
        self.__n = n
        self.__pairs = PairsStorage(n)

        value_matrix = self.__extract_value_matrix(parameters)
        self.__max_unique_pairs_expected = get_max_combination_number(value_matrix, n)
        self.__working_item_matrix = self.__get_working_item_matrix(value_matrix)

        for arr in previously_tested:
            if not arr:
                continue

            if len(arr) != len(self.__working_item_matrix):
                raise RuntimeError("previously tested combination is not complete")

            if not self.__filter_func(arr):
                raise ValueError("invalid tested combination is provided")

            tested = []
            for i, val in enumerate(arr):
                idxs = [
                    Item(item.id, 0) for item in self.__working_item_matrix[i] if item.value == val
                ]

                if len(idxs) != 1:
                    raise ValueError(
                        "value from previously tested combination is not "
                        "found in the parameters or found more than "
                        "once"
                    )

                tested.append(idxs[0])

            self.__pairs.add_sequence(tested)

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        assert len(self.__pairs) <= self.__max_unique_pairs_expected

        if len(self.__pairs) == self.__max_unique_pairs_expected:
            # no reasons to search further - all pairs are found
            raise StopIteration()

        previous_unique_pairs_count = len(self.__pairs)
        chosen_item_list = [None] * len(self.__working_item_matrix)
        indexes = [None] * len(self.__working_item_matrix)

        direction = 1
        i = 0

        while -1 < i < len(self.__working_item_matrix):
            if direction == 1:
                # move forward
                self.__resort_working_array(chosen_item_list[:i], i)
                indexes[i] = 0
            elif direction == 0 or direction == -1:
                # scan current array or go back
                indexes[i] += 1
                if indexes[i] >= len(self.__working_item_matrix[i]):
                    direction = -1
                    if i == 0:
                        raise StopIteration()
                    i += direction
                    continue
                direction = 0
            else:
                raise ValueError(f"next(): unknown 'direction' code '{direction}'")

            chosen_item_list[i] = self.__working_item_matrix[i][indexes[i]]

            if self.__filter_func(self.__get_values(chosen_item_list[: i + 1])):
                assert direction > -1
                direction = 1
            else:
                direction = 0
            i += direction

        if len(self.__working_item_matrix) != len(chosen_item_list):
            raise StopIteration()

        self.__pairs.add_sequence(chosen_item_list)

        if len(self.__pairs) == previous_unique_pairs_count:
            # could not find new unique pairs - stop
            raise StopIteration()

        # replace returned array elements with real values and return it
        return self.__get_iteration_value(chosen_item_list)

    def __validate_parameter(self, value):
        if isinstance(value, OrderedDict):
            for parameter_list in value.values():
                if not parameter_list:
                    raise ValueError("each parameter arrays must have at least one item")

            return

        if len(value) < 2:
            raise ValueError("must provide more than one option")

        for parameter_list in value:
            if not parameter_list:
                raise ValueError("each parameter arrays must have at least one item")

    def __resort_working_array(self, chosen_item_list, num):
        for item in self.__working_item_matrix[num]:
            data_node = self.__pairs.get_node_info(item)

            new_combs = [
                # numbers of new combinations to be created if this item is
                # appended to array
                {key(z) for z in combinations(chosen_item_list + [item], i + 1)}
                - self.__pairs.get_combs()[i]
                for i in range(0, self.__n)
            ]

            # weighting the node node that creates most of new pairs is the best
            weights = [-len(new_combs[-1])]

            # less used outbound connections most likely to produce more new
            # pairs while search continues
            weights.extend(
                [len(data_node.out)]
                + [len(x) for x in reversed(new_combs[:-1])]
                + [-data_node.counter]  # less used node is better
            )

            # otherwise we will prefer node with most of free inbound
            # connections; somehow it works out better ;)
            weights.append(-len(data_node.in_))

            item.set_weights(weights)

        self.__working_item_matrix[num].sort(key=cmp_to_key(cmp_item))

    def __get_working_item_matrix(self, parameter_matrix):
        return [
            [
                Item(f"a{param_idx:d}v{value_idx:d}", value)
                for value_idx, value in enumerate(value_list)
            ]
            for param_idx, value_list in enumerate(parameter_matrix)
        ]

    @staticmethod
    def __get_values(item_list):
        return [item.value for item in item_list]

    def __get_iteration_value(self, item_list):
        if not self.__param_name_list:
            return [item.value for item in item_list]

        return self.__pairs_class(*[item.value for item in item_list])

    def __extract_param_name_list(self, parameters):
        if not self.__is_ordered_dict_param:
            return []

        return list(parameters)

    def __extract_value_matrix(self, parameters):
        if not self.__is_ordered_dict_param:
            return parameters

        return [v for v in parameters.values()]
