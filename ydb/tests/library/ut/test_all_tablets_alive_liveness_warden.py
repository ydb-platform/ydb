from ydb.tests.library.wardens.hive import AllTabletsAliveLivenessWarden


_ALIVE = "SUM(Hive/TabletsAlive)"
_TOTAL = "SUM(Hive/TabletsTotal)"
_DONE = "SUM(Hive/StateDone)"


class _Monitor(object):
    def __init__(self, values):
        self._values = values

    def sensor(self, counters, sensor, _default=None, **kwargs):
        return self._values.get(sensor, _default)


class _Node(object):
    def __init__(self, values):
        self.monitor = _Monitor(values)


class _Cluster(object):
    def __init__(self, node_values):
        self.nodes = {
            node_id: _Node(values) for node_id, values in node_values.items()
        }


def _violations(node_values):
    return AllTabletsAliveLivenessWarden(_Cluster(node_values)).list_of_liveness_violations


def test_two_nodes_with_state_done_pass_when_all_tablets_are_alive():
    violations = _violations({
        1: {_ALIVE: 1, _TOTAL: 1, _DONE: 1},
        2: {_ALIVE: 1, _TOTAL: 1, _DONE: 1},
    })

    assert violations == []


def test_alive_below_total_fails():
    violations = _violations({
        1: {_ALIVE: 1, _TOTAL: 1, _DONE: 1},
        2: {_ALIVE: 0, _TOTAL: 1, _DONE: 1},
    })

    assert len(violations) == 1
    assert "1 non-active tablet(s)" in violations[0]
    assert "tablets alive - 1, tablets total - 2, state done - 2" in violations[0]


def test_state_done_zero_fails_when_all_tablets_are_alive():
    violations = _violations({
        1: {_ALIVE: 1, _TOTAL: 1, _DONE: 0},
        2: {_ALIVE: 1, _TOTAL: 1, _DONE: 0},
    })

    assert len(violations) == 1
    assert "0 non-active" not in violations[0]
    assert "Hive StateDone is 0" in violations[0]
    assert "tablets alive - 2, tablets total - 2, state done - 0" in violations[0]
