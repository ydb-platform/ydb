#cython: freethreading_compatible = True

from cpp_common cimport EditType, RfEditOp, RfOpcode, convert_string, is_valid_string
from cpython.list cimport PyList_New, PyList_SET_ITEM
from cpython.pycapsule cimport PyCapsule_GetPointer, PyCapsule_IsValid, PyCapsule_New
from cpython.ref cimport Py_INCREF
from libc.stdint cimport int64_t, uint32_t
from libc.stdlib cimport free, malloc
from libcpp cimport bool
from libcpp.utility cimport move
from libcpp.vector cimport vector

from rapidfuzz cimport RF_String


cdef extern from "rapidfuzz/details/types.hpp" namespace "rapidfuzz" nogil:
    cdef struct LevenshteinWeightTable:
        int64_t insert_cost
        int64_t delete_cost
        int64_t replace_cost

cdef extern from "cpp_common.hpp":
    object opcodes_apply(const RfOpcodes& ops, const RF_String& str1, const RF_String& str2) except + nogil
    object editops_apply(const RfEditops& ops, const RF_String& str1, const RF_String& str2) except + nogil

cdef str edit_type_to_str(EditType edit_type):
    if edit_type == EditType.Insert:
        return "insert"
    elif edit_type == EditType.Delete:
        return "delete"
    elif edit_type == EditType.Replace:
        return "replace"
    else:
        return "equal"

cdef EditType str_to_edit_type(edit_type) except *:
    # todo fix compiler warning about potentially uninitialized return value
    if edit_type == "insert":
        return EditType.Insert
    elif edit_type == "delete":
        return EditType.Delete
    elif edit_type == "replace":
        return EditType.Replace
    elif edit_type == "equal":
        return EditType.None
    else:
        raise ValueError("Invalid Edit Type")

cdef RfEditops list_to_editops(ops, size_t src_len, size_t dest_len) except *:
    cdef RfEditops result
    cdef size_t i
    cdef EditType edit_type
    cdef size_t src_pos, dest_pos
    cdef size_t ops_len = len(ops)
    result.set_src_len(src_len)
    result.set_dest_len(dest_len)

    if not ops_len:
        return result

    if len(ops[0]) == 5:
        return RfEditops(list_to_opcodes(ops, src_len, dest_len))

    result.reserve(ops_len)
    for op in ops:
        if len(op) != 3:
            raise TypeError("Expected list of 3-tuples, or a list of 5-tuples")

        edit_type = str_to_edit_type(op[0])
        src_pos = op[1]
        dest_pos = op[2]

        if src_pos > src_len or dest_pos > dest_len:
            raise ValueError("List of edit operations invalid")

        if src_pos == src_len and edit_type != EditType.Insert:
            raise ValueError("List of edit operations invalid")
        elif dest_pos == dest_len and edit_type != EditType.Delete:
            raise ValueError("List of edit operations invalid")

        # keep operations are not relevant in editops
        if edit_type == EditType.None:
            continue

        result.emplace_back(edit_type, src_pos, dest_pos)

    # validate order of editops
    if (result.size()):
        for i in range(0, result.size() - 1):
            if result[i + 1].src_pos < result[i].src_pos or result[i + 1].dest_pos < result[i].dest_pos:
                raise ValueError("List of edit operations out of order")
            if result[i + 1].src_pos == result[i].src_pos and result[i + 1].dest_pos == result[i].dest_pos:
                raise ValueError("Duplicated edit operation")

    result.shrink_to_fit()
    return result

cdef RfOpcodes list_to_opcodes(ops, size_t src_len, size_t dest_len) except *:
    cdef RfOpcodes result
    cdef size_t i
    cdef EditType edit_type
    cdef size_t src_start, src_end, dest_start, dest_end
    cdef size_t ops_len = len(ops)
    if not ops_len or len(ops[0]) == 3:
        return RfOpcodes(list_to_editops(ops, src_len, dest_len))

    result.set_src_len(src_len)
    result.set_dest_len(dest_len)
    result.reserve(ops_len)
    for op in ops:
        if len(op) != 5:
            raise TypeError("Expected list of 3-tuples, or a list of 5-tuples")

        edit_type = str_to_edit_type(op[0])
        src_start = op[1]
        src_end = op[2]
        dest_start = op[3]
        dest_end = op[4]

        if src_end > src_len or dest_end > dest_len:
            raise ValueError("List of edit operations invalid")
        elif src_end < src_start or dest_end < dest_start:
            raise ValueError("List of edit operations invalid")

        if edit_type == EditType.None or edit_type == EditType.Replace:
            if src_end - src_start != dest_end - dest_start or src_start == src_end:
                raise ValueError("List of edit operations invalid")
        if edit_type == EditType.Insert:
            if src_start != src_end or dest_start == dest_end:
                raise ValueError("List of edit operations invalid")
        elif edit_type == EditType.Delete:
            if src_start == src_end or dest_start != dest_end:
                raise ValueError("List of edit operations invalid")

        # merge similar adjacent blocks
        if not result.empty():
            if result.back().type == edit_type and result.back().src_end == src_start and result.back().dest_end == dest_start:
                result.back().src_end = src_end
                result.back().dest_end = dest_end
                continue

        result.emplace_back(edit_type, src_start, src_end, dest_start, dest_end)

    # check if edit operations span the complete string
    if result[0].src_begin != 0 or result[0].dest_begin != 0:
        raise ValueError("List of edit operations does not start at position 0")
    if result.back().src_end != src_len or result.back().dest_end != dest_len:
        raise ValueError("List of edit operations does not end at the string ends")
    for i in range(0, result.size() - 1):
        if result[i + 1].src_begin != result[i].src_end or result[i + 1].dest_begin != result[i].dest_end:
            raise ValueError("List of edit operations is not continuous")

    result.shrink_to_fit()
    return result

cdef list editops_to_list(const RfEditops& ops):
    cdef size_t op_count = ops.size()
    cdef list result_list = PyList_New(<Py_ssize_t>op_count)
    for i in range(op_count):
        result_item = (edit_type_to_str(ops[i].type), ops[i].src_pos, ops[i].dest_pos)
        Py_INCREF(result_item)
        PyList_SET_ITEM(result_list, <Py_ssize_t>i, result_item)

    return result_list

cdef list opcodes_to_list(const RfOpcodes& ops):
    cdef size_t op_count = ops.size()
    cdef list result_list = PyList_New(<Py_ssize_t>op_count)
    for i in range(op_count):
        result_item = (
            edit_type_to_str(ops[i].type),
            ops[i].src_begin, ops[i].src_end,
            ops[i].dest_begin, ops[i].dest_end)
        Py_INCREF(result_item)
        PyList_SET_ITEM(result_list, <Py_ssize_t>i, result_item)

    return result_list

cdef class MatchingBlock:
    """
    Triple describing matching subsequences
    """

    cdef public size_t a
    cdef public size_t b
    cdef public size_t size

    def __cinit__(self, size_t a, size_t b, size_t size):
        self.a = a
        self.b = b
        self.size = size

    def __len__(self):
        return 3

    def __eq__(self, other):
        try:
            if len(other) != 3:
                return False

            return (other[0] == self.a
                and other[1] == self.b
                and other[2] == self.size)
        except:
            return False

    def __getitem__(self, Py_ssize_t i):
        if i==0 or i==-3: return self.a
        if i==1 or i==-2: return self.b
        if i==2 or i==-1: return self.size

        raise IndexError('MatchingBlock index out of range')

    def __iter__(self):
        yield self.a
        yield self.b
        yield self.size

    def __repr__(self):
        return f"MatchingBlock(a={self.a}, b={self.b}, size={self.size})"

cdef list editops_to_matching_blocks(const RfEditops& ops):
    cdef MatchingBlock result_item
    cdef Py_ssize_t block_num = 0
    cdef size_t i = 0
    cdef size_t j = 0
    cdef size_t src_pos = 0
    cdef size_t dest_pos = 0
    for i in range(ops.size()):
        if src_pos < ops[i].src_pos or dest_pos < ops[i].dest_pos:
            if min(ops[i].src_pos - src_pos, ops[i].dest_pos - dest_pos) > 0:
                block_num += 1
            src_pos = ops[i].src_pos
            dest_pos = ops[i].dest_pos

        if ops[i].type == EditType.Replace:
            src_pos += 1
            dest_pos += 1
        elif ops[i].type == EditType.Delete:
            src_pos += 1
        elif ops[i].type == EditType.Insert:
            dest_pos += 1

    if src_pos < ops.get_src_len() or dest_pos < ops.get_dest_len():
        if min(ops.get_src_len() - src_pos, ops.get_dest_len() - dest_pos) > 0:
            block_num += 1

    cdef list result_list = PyList_New(block_num + 1)
    src_pos = 0
    dest_pos = 0
    for i in range(ops.size()):
        if src_pos < ops[i].src_pos or dest_pos < ops[i].dest_pos:
            length = min(ops[i].src_pos - src_pos, ops[i].dest_pos - dest_pos)
            if length > 0:
                result_item = MatchingBlock(src_pos, dest_pos, length)
                Py_INCREF(result_item)
                PyList_SET_ITEM(result_list, <Py_ssize_t>j, result_item)
                j += 1
            src_pos = ops[i].src_pos
            dest_pos = ops[i].dest_pos

        if ops[i].type == EditType.Replace:
            src_pos += 1
            dest_pos += 1
        elif ops[i].type == EditType.Delete:
            src_pos += 1
        elif ops[i].type == EditType.Insert:
            dest_pos += 1

    if src_pos < ops.get_src_len() or dest_pos < ops.get_dest_len():
        length = min(ops.get_src_len() - src_pos, ops.get_dest_len() - dest_pos)
        if length > 0:
            result_item = MatchingBlock(src_pos, dest_pos, length)
            Py_INCREF(result_item)
            PyList_SET_ITEM(result_list, <Py_ssize_t>j, result_item)
            j += 1

    result_item = MatchingBlock(ops.get_src_len(), ops.get_dest_len(), 0)
    Py_INCREF(result_item)
    PyList_SET_ITEM(result_list, <Py_ssize_t>block_num, result_item)
    return result_list

cdef list opcodes_to_matching_blocks(const RfOpcodes& ops):
    cdef MatchingBlock result_item
    cdef Py_ssize_t block_num = 0
    cdef size_t i = 0
    cdef size_t j = 0
    for i in range(ops.size()):
        if ops[i].type == EditType.None:
            if min(ops[i].src_end - ops[i].src_begin, ops[i].dest_end - ops[i].dest_begin) > 0:
                block_num += 1

    cdef list result_list = PyList_New(block_num + 1)
    for i in range(ops.size()):
        if ops[i].type == EditType.None:
            length = min(ops[i].src_end - ops[i].src_begin, ops[i].dest_end - ops[i].dest_begin)
            if length > 0:
                result_item = MatchingBlock(ops[i].src_begin, ops[i].dest_begin, length)
                Py_INCREF(result_item)
                PyList_SET_ITEM(result_list, <Py_ssize_t>j, result_item)
                j += 1

    result_item = MatchingBlock(ops.get_src_len(), ops.get_dest_len(), 0)
    Py_INCREF(result_item)
    PyList_SET_ITEM(result_list, <Py_ssize_t>block_num, result_item)
    return result_list

cdef class Editop:
    """
    Tuple like object describing an edit operation.
    It is in the form (tag, src_pos, dest_pos)

    The tags are strings, with these meanings:

    +-----------+---------------------------------------------------+
    | tag       | explanation                                       |
    +===========+===================================================+
    | 'replace' | src[src_pos] should be replaced by dest[dest_pos] |
    +-----------+---------------------------------------------------+
    | 'delete'  | src[src_pos] should be deleted                    |
    +-----------+---------------------------------------------------+
    | 'insert'  | dest[dest_pos] should be inserted at src[src_pos] |
    +-----------+---------------------------------------------------+
    """
    cdef public str tag
    cdef public Py_ssize_t src_pos
    cdef public Py_ssize_t dest_pos

    def __init__(self, tag, src_pos, dest_pos):
        self.tag = tag
        self.src_pos = src_pos
        self.dest_pos = dest_pos

    def __len__(self):
        return 3

    def __eq__(self, other):
        try:
            if len(other) != 3:
                return False

            return (other[0] == self.tag
                and other[1] == self.src_pos
                and other[2] == self.dest_pos)
        except:
            return False

    def __getitem__(self, Py_ssize_t i):
        if i==0 or i==-3: return self.tag
        if i==1 or i==-2: return self.src_pos
        if i==2 or i==-1: return self.dest_pos

        raise IndexError('Editop index out of range')

    def __iter__(self):
        yield self.tag
        yield self.src_pos
        yield self.dest_pos

    def __repr__(self):
        return f"Editop(tag='{self.tag}', src_pos={self.src_pos}, dest_pos={self.dest_pos})"

cdef class Editops:
    """
    List like object of Editops describing how to turn s1 into s2.
    """

    def __init__(self, editops=None, src_len=0, dest_len=0):
        if editops is not None:
            self.editops = list_to_editops(editops, src_len, dest_len)

    @classmethod
    def from_opcodes(cls, Opcodes opcodes):
        """
        Create Editops from Opcodes

        Parameters
        ----------
        opcodes : Opcodes
            opcodes to convert to editops

        Returns
        -------
        editops : Editops
            Opcodes converted to Editops
        """
        cdef Editops self = cls.__new__(cls)
        self.editops = RfEditops(opcodes.opcodes)
        return self

    def as_opcodes(self):
        """
        Convert to Opcodes

        Returns
        -------
        opcodes : Opcodes
            Editops converted to Opcodes
        """
        cdef Opcodes opcodes = Opcodes.__new__(Opcodes)
        opcodes.opcodes = RfOpcodes(self.editops)
        return opcodes

    def as_matching_blocks(self):
        """
        Convert to matching blocks

        Returns
        -------
        matching blocks : list[MatchingBlock]
            Editops converted to matching blocks
        """
        return editops_to_matching_blocks(self.editops)

    def as_list(self):
        """
        Convert Editops to a list of tuples.

        This is the equivalent of ``[x for x in editops]``
        """
        return editops_to_list(self.editops)

    def copy(self):
        """
        performs copy of Editops
        """
        cdef Editops x = Editops.__new__(Editops)
        x.editops = self.editops
        return x

    def inverse(self):
        """
        Invert Editops, so it describes how to transform the destination string to
        the source string.

        Returns
        -------
        editops : Editops
            inverted Editops

        Examples
        --------
        >>> from rapidfuzz.distance import Levenshtein
        >>> Levenshtein.editops('spam', 'park')
        [Editop(tag=delete, src_pos=0, dest_pos=0),
         Editop(tag=replace, src_pos=3, dest_pos=2),
         Editop(tag=insert, src_pos=4, dest_pos=3)]

        >>> Levenshtein.editops('spam', 'park').inverse()
        [Editop(tag=insert, src_pos=0, dest_pos=0),
         Editop(tag=replace, src_pos=2, dest_pos=3),
         Editop(tag=delete, src_pos=3, dest_pos=4)]
        """
        cdef Editops x = Editops.__new__(Editops)
        x.editops = self.editops.inverse()
        return x

    def apply(self, source_string, destination_string):
        """
        apply editops to source_string

        Parameters
        ----------
        source_string : str | bytes
            string to apply editops to
        destination_string : str | bytes
            string to use for replacements / insertions into source_string

        Returns
        -------
        mod_string : str
            modified source_string

        """
        if not (is_valid_string(source_string) and is_valid_string(destination_string)):
            raise TypeError("expected strings or bytes object")

        return editops_apply(self.editops, convert_string(source_string), convert_string(destination_string))

    def remove_subsequence(self, Editops subsequence):
        """
        remove a subsequence

        Parameters
        ----------
        subsequence : Editops
            subsequence to remove (has to be a subset of editops)

        Returns
        -------
        sequence : Editops
            a copy of the editops without the subsequence
        """
        cdef Editops x = Editops.__new__(Editops)
        x.editops = self.editops.remove_subsequence(subsequence.editops)
        return x

    @property
    def src_len(self):
        return self.editops.get_src_len()

    @src_len.setter
    def src_len(self, value):
        self.editops.set_src_len(value)

    @property
    def dest_len(self):
        return self.editops.get_dest_len()

    @dest_len.setter
    def dest_len(self, value):
        self.editops.set_dest_len(value)

    def __eq__(self, other):
        if isinstance(other, Editops):
            return self.editops == (<Editops>other).editops

        return False

    def __len__(self):
        return self.editops.size()

    def __delitem__(self, key):
        cdef Py_ssize_t index
        cdef Py_ssize_t start, stop, step

        if isinstance(key, int):
            index = key
            if index < 0:
                index += <Py_ssize_t>self.editops.size()

            if index < 0 or index >= <Py_ssize_t>self.editops.size():
                raise IndexError("Editops index out of range")

            self.editops.erase(self.editops.begin() + index)
        elif isinstance(key, slice):
            start, stop, step = key.indices(<Py_ssize_t>self.editops.size())
            if step < 0:
                raise ValueError("step sizes below 0 lead to an invalid order of editops")

            self.editops.remove_slice(start, stop, step)
        else:
            raise TypeError("Expected index or slice")

    def __getitem__(self, key):
        cdef Py_ssize_t index
        cdef Py_ssize_t start, stop, step

        if isinstance(key, int):
            index = key
            if index < 0:
                index += <Py_ssize_t>self.editops.size()

            if index < 0 or index >= <Py_ssize_t>self.editops.size():
                raise IndexError("Editops index out of range")

            return Editop(
                edit_type_to_str(self.editops[index].type),
                self.editops[index].src_pos,
                self.editops[index].dest_pos
            )
        elif isinstance(key, slice):
            start, stop, step = key.indices(<Py_ssize_t>self.editops.size())
            if step < 0:
                raise ValueError("step sizes below 0 lead to an invalid order of editops")
            x = Editops.__new__(Editops)
            (<Editops>x).editops = self.editops.slice(start, stop, step)
            return x
        else:
            raise TypeError("Expected index or slice")

    def __iter__(self):
        cdef size_t i
        for i in range(self.editops.size()):
            yield Editop(
                edit_type_to_str(self.editops[i].type),
                self.editops[i].src_pos,
                self.editops[i].dest_pos
            )

    def __repr__(self):
        return "Editops([" + ", ".join(repr(op) for op in self) + f"], src_len={self.editops.get_src_len()}, dest_len={self.editops.get_dest_len()})"

cdef class Opcode:
    """
    Tuple like object describing an edit operation.
    It is in the form (tag, src_start, src_end, dest_start, dest_end)

    The tags are strings, with these meanings:

    +-----------+-----------------------------------------------------+
    | tag       | explanation                                         |
    +===========+=====================================================+
    | 'replace' | src[src_start:src_end] should be                    |
    |           | replaced by dest[dest_start:dest_end]               |
    +-----------+-----------------------------------------------------+
    | 'delete'  | src[src_start:src_end] should be deleted.           |
    |           | Note that dest_start==dest_end in this case.        |
    +-----------+-----------------------------------------------------+
    | 'insert'  | dest[dest_start:dest_end] should be inserted        |
    |           | at src[src_start:src_start].                        |
    |           | Note that src_start==src_end in this case.          |
    +-----------+-----------------------------------------------------+
    | 'equal'   | src[src_start:src_end] == dest[dest_start:dest_end] |
    +-----------+-----------------------------------------------------+

    Note
    ----
    Opcode is compatible with the tuples returned by difflib's SequenceMatcher to make them
    interoperable
    """
    cdef public str tag
    cdef public Py_ssize_t src_start
    cdef public Py_ssize_t src_end
    cdef public Py_ssize_t dest_start
    cdef public Py_ssize_t dest_end

    def __init__(self, tag, src_start, src_end, dest_start, dest_end):
        self.tag = tag
        self.src_start = src_start
        self.src_end = src_end
        self.dest_start = dest_start
        self.dest_end = dest_end

    def __len__(self):
        return 5

    def __eq__(self, other):
        try:
            if len(other) != 5:
                return False

            return (other[0] == self.tag
                and other[1] == self.src_start
                and other[2] == self.src_end
                and other[3] == self.dest_start
                and other[4] == self.dest_end)
        except:
            return False

    def __getitem__(self, Py_ssize_t i):
        if i==0 or i==-5: return self.tag
        if i==1 or i==-4: return self.src_start
        if i==2 or i==-3: return self.src_end
        if i==3 or i==-2: return self.dest_start
        if i==4 or i==-1: return self.dest_end

        raise IndexError('Opcode index out of range')

    def __iter__(self):
        yield self.tag
        yield self.src_start
        yield self.src_end
        yield self.dest_start
        yield self.dest_end

    def __repr__(self):
        return f"Opcode(tag='{self.tag}', src_start={self.src_start}, src_end={self.src_end}, dest_start={self.dest_start}, dest_end={self.dest_end})"

cdef class Opcodes:
    """
    List like object of Opcodes describing how to turn s1 into s2.
    The first Opcode has src_start == dest_start == 0, and remaining tuples
    have src_start == the src_end from the tuple preceding it,
    and likewise for dest_start == the previous dest_end.
    """

    def __init__(self, opcodes=None, src_len=0, dest_len=0):
        if opcodes is not None:
            self.opcodes = list_to_opcodes(opcodes, src_len, dest_len)

    @classmethod
    def from_editops(cls, Editops editops):
        """
        Create Opcodes from Editops

        Parameters
        ----------
        editops : Editops
            editops to convert to opcodes

        Returns
        -------
        opcodes : Opcodes
            Editops converted to Opcodes
        """
        cdef Opcodes self = cls.__new__(cls)
        self.opcodes = RfOpcodes(editops.editops)
        return self

    def as_editops(self):
        """
        Convert to Editops

        Returns
        -------
        editops : Editops
            Opcodes converted to Editops
        """
        cdef Editops editops = Editops.__new__(Editops)
        editops.editops = RfEditops(self.opcodes)
        return editops

    def as_matching_blocks(self):
        """
        Convert to matching blocks

        Returns
        -------
        matching blocks : list[MatchingBlock]
            Opcodes converted to matching blocks
        """
        return opcodes_to_matching_blocks(self.opcodes)

    def as_list(self):
        """
        Convert Opcodes to a list of tuples, which is compatible
        with the opcodes of difflibs SequenceMatcher.

        This is the equivalent of ``[x for x in opcodes]``
        """
        return opcodes_to_list(self.opcodes)

    def copy(self):
        """
        performs copy of Opcodes
        """
        cdef Opcodes x = Opcodes.__new__(Opcodes)
        x.opcodes = self.opcodes
        return x

    def inverse(self):
        """
        Invert Opcodes, so it describes how to transform the destination string to
        the source string.

        Returns
        -------
        opcodes : Opcodes
            inverted Opcodes

        Examples
        --------
        >>> from rapidfuzz.distance import Levenshtein
        >>> Levenshtein.opcodes('spam', 'park')
        [Opcode(tag=delete, src_start=0, src_end=1, dest_start=0, dest_end=0),
         Opcode(tag=equal, src_start=1, src_end=3, dest_start=0, dest_end=2),
         Opcode(tag=replace, src_start=3, src_end=4, dest_start=2, dest_end=3),
         Opcode(tag=insert, src_start=4, src_end=4, dest_start=3, dest_end=4)]

        >>> Levenshtein.opcodes('spam', 'park').inverse()
        [Opcode(tag=insert, src_start=0, src_end=0, dest_start=0, dest_end=1),
         Opcode(tag=equal, src_start=0, src_end=2, dest_start=1, dest_end=3),
         Opcode(tag=replace, src_start=2, src_end=3, dest_start=3, dest_end=4),
         Opcode(tag=delete, src_start=3, src_end=4, dest_start=4, dest_end=4)]
        """
        cdef Opcodes x = Opcodes.__new__(Opcodes)
        x.opcodes = self.opcodes.inverse()
        return x

    def apply(self, source_string, destination_string):
        """
        apply opcodes to source_string

        Parameters
        ----------
        source_string : str | bytes
            string to apply opcodes to
        destination_string : str | bytes
            string to use for replacements / insertions into source_string

        Returns
        -------
        mod_string : str
            modified source_string

        """
        if not (is_valid_string(source_string) and is_valid_string(destination_string)):
            raise TypeError("expected strings or bytes object")

        return opcodes_apply(self.opcodes, convert_string(source_string), convert_string(destination_string))

    @property
    def src_len(self):
        return self.opcodes.get_src_len()

    @src_len.setter
    def src_len(self, value):
        self.opcodes.set_src_len(value)

    @property
    def dest_len(self):
        return self.opcodes.get_dest_len()

    @dest_len.setter
    def dest_len(self, value):
        self.opcodes.set_dest_len(value)

    def __eq__(self, other):
        if isinstance(other, Opcodes):
            return self.opcodes == (<Opcodes>other).opcodes

        return False

    def __len__(self):
        return self.opcodes.size()

    def __getitem__(self, key):
        cdef Py_ssize_t index

        if isinstance(key, int):
            index = key
            if index < 0:
                index += <Py_ssize_t>self.opcodes.size()

            if index < 0 or index >= <Py_ssize_t>self.opcodes.size():
                raise IndexError("Opcodes index out of range")

            return Opcode(
                edit_type_to_str(self.opcodes[index].type),
                self.opcodes[index].src_begin,
                self.opcodes[index].src_end,
                self.opcodes[index].dest_begin,
                self.opcodes[index].dest_end
            )
        else:
            raise TypeError("Expected index")

    def __iter__(self):
        cdef size_t i
        for i in range(self.opcodes.size()):
            yield Opcode(
                edit_type_to_str(self.opcodes[i].type),
                self.opcodes[i].src_begin,
                self.opcodes[i].src_end,
                self.opcodes[i].dest_begin,
                self.opcodes[i].dest_end
            )

    def __repr__(self):
        return "Opcodes([" + ", ".join(repr(op) for op in self) + f"], src_len={self.opcodes.get_src_len()}, dest_len={self.opcodes.get_dest_len()})"


cdef class ScoreAlignment:
    """
    Tuple like object describing the position of the compared strings in
    src and dest.

    It indicates that the score has been calculated between
    src[src_start:src_end] and dest[dest_start:dest_end]
    """
    def __init__(self, score, src_start, src_end, dest_start, dest_end):
        self.score = score
        self.src_start = src_start
        self.src_end = src_end
        self.dest_start = dest_start
        self.dest_end = dest_end

    def __len__(self):
        return 5

    def __eq__(self, other):
        try:
            if len(other) != 5:
                return False

            return (other[0] == self.score
                and other[1] == self.src_start
                and other[2] == self.src_end
                and other[3] == self.dest_start
                and other[4] == self.dest_end)
        except:
            return False

    def __getitem__(self, Py_ssize_t i):
        if i==0 or i==-5: return self.score
        if i==1 or i==-4: return self.src_start
        if i==2 or i==-3: return self.src_end
        if i==3 or i==-2: return self.dest_start
        if i==4 or i==-1: return self.dest_end

        raise IndexError('Opcode index out of range')

    def __iter__(self):
        yield self.score
        yield self.src_start
        yield self.src_end
        yield self.dest_start
        yield self.dest_end

    def __repr__(self):
        return f"ScoreAlignment(score={self.score}, src_start={self.src_start}, src_end={self.src_end}, dest_start={self.dest_start}, dest_end={self.dest_end})"
