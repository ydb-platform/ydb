import sys

from ydb.tests.olap.lib.workload_result import ErrorArea, ErrorPriority, Iteration, WorkloadError, WorkloadRunResult


def _make_result() -> WorkloadRunResult:
    result = WorkloadRunResult()
    result.iterations[0] = Iteration()
    return result


def _error_with_tb(message: str) -> WorkloadError:
    try:
        raise ValueError(message)
    except ValueError:
        return WorkloadError(message, tb=sys.exc_info()[2])


class TestWorkloadError:
    def test_serialize_defaults(self):
        e = WorkloadError('boom')
        assert e.priority == ErrorPriority.ERROR
        assert e.area == ErrorArea.OTHER
        assert e.serialize() == {
            'priority': 'ERROR',
            'area': 'OTHER',
            'message': 'boom',
        }

    def test_serialize_with_traceback(self):
        e = _error_with_tb('boom')
        serialized = e.serialize()
        assert serialized['priority'] == 'ERROR'
        assert serialized['message'] == 'boom'
        assert 'traceback' in serialized
        assert all(isinstance(t, str) for t in serialized['traceback'])
        # Строки трейсбека не должны кончаться на \n
        assert all(not t.endswith('\n') for t in serialized['traceback'])
        assert any('ValueError' in t for t in serialized['traceback'])

    def test_none_area_coerced_to_other(self):
        assert WorkloadError('boom', area=None).area == ErrorArea.OTHER


class TestWorkloadRunResult:
    def test_empty(self):
        result = _make_result()
        assert result.get_errors() == []
        assert result.get_integrated_error() is None
        assert result.success is True
        assert result.get_error_stats() == {}

    def test_add_error_and_warning(self):
        result = _make_result()
        assert result.add_error('e1', area=ErrorArea.REQUEST) is True
        assert result.add_error('', area=ErrorArea.DIFF) is False
        result.add_warning('w1', area=ErrorArea.TIMEOUT)
        assert len(result.get_errors()) == 2
        assert len(result.get_errors(ErrorPriority.ERROR)) == 1
        assert len(result.get_errors(ErrorPriority.WARNING)) == 1
        assert len(result.get_errors(area=ErrorArea.REQUEST)) == 1
        assert len(result.get_errors(area=ErrorArea.OTHER)) == 0

    def test_success(self):
        result = _make_result()
        result.add_error('e1', area=ErrorArea.DIFF)
        assert result.success is False
        result2 = _make_result()
        result2.add_warning('w1', area=ErrorArea.TIMEOUT)
        assert result2.success is True

    def test_get_error_stats(self):
        result = _make_result()
        result.iterations[0].error_message = 'Deadline Exceeded'
        result.add_error('e2', area=ErrorArea.NODE_FAIL)
        result.add_warning('w1', area=ErrorArea.OTHER)
        stats = result.get_error_stats()
        assert stats == {'timeout': True, 'node_fail': True, 'warning': True}

    def test_get_error_stats_other_area(self):
        # ErrorArea.OTHER == 0, не должен потеряться как falsy
        result = _make_result()
        result.add_error('e1', area=ErrorArea.OTHER)
        assert result.get_error_stats() == {'other': True}

    def test_integrated_error_merges_all(self):
        result = _make_result()
        result.add_error('e1', area=ErrorArea.REQUEST)
        result.add_error('e2', area=ErrorArea.NODE_FAIL)
        result.add_warning('w1', area=ErrorArea.DIFF)
        ie = result.get_integrated_error()
        assert ie is not None
        assert ie.priority == ErrorPriority.ERROR
        # При равных приоритетах берется первая добавленная ошибка
        assert ie.area == ErrorArea.REQUEST
        assert str(ie) == 'REQUEST: e1\nNODE_FAIL: e2\nDIFF: w1'
        # Только WARNING-приоритет: ошибки ERROR не включаются
        wm = result.get_integrated_error(ErrorPriority.WARNING)
        assert wm is not None
        assert str(wm) == 'DIFF: w1'
        assert wm.priority == ErrorPriority.WARNING
        assert wm.area == ErrorArea.DIFF

    def test_integrated_error_traceback(self):
        result = _make_result()
        result.add_error('e1', area=ErrorArea.REQUEST)
        result.add_custom_error(_error_with_tb('e2'))
        ie = result.get_integrated_error()
        assert ie.__traceback__ is not None
        assert any('ValueError' in t for t in ie.serialize()['traceback'])

    def test_merge(self):
        r1 = _make_result()
        r1.add_error('e1', area=ErrorArea.REQUEST)
        r2 = _make_result()
        r2.add_warning('w1', area=ErrorArea.DIFF)
        merged = _make_result().merge(r1, r2)
        assert len(merged.get_errors()) == 2
        assert str(merged.get_integrated_error()) == 'REQUEST: e1\nDIFF: w1'
