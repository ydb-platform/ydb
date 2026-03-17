from itertools import combinations


class Node:
    @property
    def id(self):
        return self.__node_id

    @property
    def counter(self):
        return self.__counter

    def __init__(self, node_id):
        self.__node_id = node_id
        self.__counter = 0
        self.in_ = set()
        self.out = set()

    def __str__(self):
        return str(self.__dict__)

    def inc_counter(self):
        self.__counter += 1


key_cache = {}


def key(items):
    if items in key_cache:
        return key_cache[items]

    key_value = tuple([x.id for x in items])
    key_cache[items] = key_value

    return key_value


class PairsStorage:
    def __init__(self, n):
        self.__n = n
        self.__nodes = {}
        self.__combs_arr = [set() for _i in range(n)]

    def __len__(self):
        return len(self.__combs_arr[-1])

    def add_sequence(self, sequence):
        for i in range(1, self.__n + 1):
            for combination in combinations(sequence, i):
                self.__add_combination(combination)

    def get_node_info(self, item):
        return self.__nodes.get(item.id, Node(item.id))

    def get_combs(self):
        return self.__combs_arr

    def __add_combination(self, combination):
        n = len(combination)
        assert n > 0

        self.__combs_arr[n - 1].add(key(combination))
        if n == 1 and combination[0].id not in self.__nodes:
            self.__nodes[combination[0].id] = Node(combination[0].id)
            return

        ids = [x.id for x in combination]
        for i, id in enumerate(ids):
            curr = self.__nodes[id]
            curr.inc_counter()
            curr.in_.update(ids[:i])
            curr.out.update(ids[i + 1 :])
