# cython: profile=False
from cymem.cymem cimport Pool

from thinc.extra.search cimport Beam
from thinc.typedefs cimport class_t, weight_t


cdef struct TestState:
    int length
    int x
    Py_UNICODE* string


cdef int transition(void* dest, void* src, class_t clas, void* extra_args) except -1:
    dest_state = <TestState*>dest
    src_state = <TestState*>src
    dest_state.length = src_state.length
    dest_state.x = src_state.x
    dest_state.x += clas
    if extra_args != NULL:
        dest_state.string = <Py_UNICODE*>extra_args
    else:
        dest_state.string = src_state.string


cdef void* initialize(Pool mem, int n, void* extra_args) except NULL:
    state = <TestState*>mem.alloc(1, sizeof(TestState))
    state.length = n
    state.x = 1
    if extra_args == NULL:
        state.string = 'default'
    else:
        state.string = <Py_UNICODE*>extra_args
    return state


cdef int destroy(Pool mem, void* state, void* extra_args) except -1:
    state = <TestState*>state
    mem.free(state)


def test_init(nr_class, beam_width):
    b = Beam(nr_class, beam_width)
    assert b.size == 1
    assert b.width == beam_width
    assert b.nr_class == nr_class


def test_initialize(nr_class, beam_width, length):
    b = Beam(nr_class, beam_width)
    b.initialize(initialize, destroy, length, NULL)
    for i in range(b.width):
        s = <TestState*>b.at(i)
        assert s.length == length, s.length
        assert s.string == 'default'


def test_initialize_extra(nr_class, beam_width, length, unicode extra):
    b = Beam(nr_class, beam_width)
    b.initialize(initialize, destroy, length, <void*><Py_UNICODE*>extra)
    for i in range(b.width):
        s = <TestState*>b.at(i)
        assert s.length == length


def test_transition(nr_class=3, beam_width=6, length=3):
    b = Beam(nr_class, beam_width)
    b.initialize(initialize, destroy, length, NULL)
    b.set_cell(0, 2, 30, True, 0)
    b.set_cell(0, 1, 42, False, 0)
    b.advance(transition, NULL, NULL)
    assert b.size == 1, b.size
    assert b.score == 30, b.score
    s = <TestState*>b.at(0)
    assert s.x == 3
    assert b._states[0].score == 30, b._states[0].score
    b.set_cell(0, 1, 10, True, 0)
    b.set_cell(0, 2, 20, True, 0)
    b.advance(transition, NULL, NULL)
    assert b._states[0].score == 50, b._states[0].score
    assert b._states[1].score == 40
    s = <TestState*>b.at(0)
    assert s.x == 5
