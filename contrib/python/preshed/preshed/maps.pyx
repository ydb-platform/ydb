# cython: infer_types=True
# cython: cdivision=True
#
cimport cython


DEF EMPTY_KEY = 0
DEF DELETED_KEY = 1


cdef class PreshMap:
    """Hash map that assumes keys come pre-hashed. Maps uint64_t --> uint64_t.
    Uses open addressing with linear probing.

    Usage
        map = PreshMap() # Create a table
        map = PreshMap(initial_size=1024) # Create with initial size (efficiency)
        map[key] = value # Set a value to a key
        value = map[key] # Get a value given a key
        for key, value in map.items(): # Iterate over items
        len(map) # Get number of inserted keys
    """
    def __init__(self, size_t initial_size=8):
        # Size must be power of two
        if initial_size == 0:
            initial_size = 8
        if initial_size & (initial_size - 1) != 0:
            power = 1
            while power < initial_size:
                power *= 2
            initial_size = power
        self.mem = Pool()
        self.c_map = <MapStruct*>self.mem.alloc(1, sizeof(MapStruct))
        map_init(self.mem, self.c_map, initial_size)

    property capacity:
        def __get__(self):
            return self.c_map.length

    def items(self):
        cdef key_t key
        cdef void* value
        cdef int i = 0
        while map_iter(self.c_map, &i, &key, &value):
            yield key, <size_t>value

    def keys(self):
        for key, _ in self.items():
            yield key

    def values(self):
        for _, value in self.items():
            yield value

    def pop(self, key_t key, default=None):
        cdef Result result = map_get_unless_missing(self.c_map, key)
        map_clear(self.c_map, key)
        if result.found:
            return <size_t>result.value
        else:
            return default

    def __getitem__(self, key_t key):
        cdef Result result = map_get_unless_missing(self.c_map, key)
        if result.found:
            return <size_t>result.value
        else:
            return None

    def __setitem__(self, key_t key, size_t value):
        map_set(self.mem, self.c_map, key, <void*>value)

    def __delitem__(self, key_t key):
        map_clear(self.c_map, key)

    def __len__(self):
        return self.c_map.filled

    def __contains__(self, key_t key):
        cdef Result result = map_get_unless_missing(self.c_map, key)
        return True if result.found else False

    def __iter__(self):
        for key in self.keys():
            yield key

    cdef inline void* get(self, key_t key) nogil:
        return map_get(self.c_map, key)

    cdef void set(self, key_t key, void* value) except *:
        map_set(self.mem, self.c_map, key, <void*>value)


cdef class PreshMapArray:
    """An array of hash tables that assume keys come pre-hashed.  Each table
    uses open addressing with linear probing.
    """
    def __init__(self, size_t length, size_t initial_size=8):
        self.mem = Pool()
        self.length = length
        self.maps = <MapStruct*>self.mem.alloc(length, sizeof(MapStruct))
        for i in range(length):
            map_init(self.mem, &self.maps[i], initial_size)

    cdef inline void* get(self, size_t i, key_t key) nogil:
        return map_get(&self.maps[i], key)

    cdef void set(self, size_t i, key_t key, void* value) except *:
        map_set(self.mem, &self.maps[i], key, <void*>value)


cdef void map_init(Pool mem, MapStruct* map_, size_t length) except *:
    map_.length = length
    map_.filled = 0
    map_.cells = <Cell*>mem.alloc(length, sizeof(Cell))


cdef void map_set(Pool mem, MapStruct* map_, key_t key, void* value) except *:
    cdef Cell* cell
    if key == EMPTY_KEY:
        map_.value_for_empty_key = value
        map_.is_empty_key_set = True
    elif key == DELETED_KEY:
        map_.value_for_del_key = value
        map_.is_del_key_set = True
    else:
        cell = _find_cell_for_insertion(map_.cells, map_.length, key)
        if cell.key == EMPTY_KEY:
            map_.filled += 1
        cell.key = key
        cell.value = value
        if (map_.filled + 1) * 5 >= (map_.length * 3):
            _resize(mem, map_)


cdef void* map_get(const MapStruct* map_, const key_t key) nogil:
    if key == EMPTY_KEY:
        return map_.value_for_empty_key
    elif key == DELETED_KEY:
        return map_.value_for_del_key
    cdef Cell* cell = _find_cell(map_.cells, map_.length, key)
    return cell.value


cdef Result map_get_unless_missing(const MapStruct* map_, const key_t key) nogil:
    cdef Result result
    cdef Cell* cell
    result.found = 0
    result.value = NULL
    if key == EMPTY_KEY:
        if map_.is_empty_key_set:
            result.found = 1
            result.value = map_.value_for_empty_key
    elif key == DELETED_KEY:
        if map_.is_del_key_set:
            result.found = 1
            result.value = map_.value_for_del_key
    else:
        cell = _find_cell(map_.cells, map_.length, key)
        if cell.key == key:
            result.found = 1
            result.value = cell.value
    return result


cdef void* map_clear(MapStruct* map_, const key_t key) nogil:
    if key == EMPTY_KEY:
        value = map_.value_for_empty_key if map_.is_empty_key_set else NULL
        map_.is_empty_key_set = False
        return value
    elif key == DELETED_KEY:
        value = map_.value_for_del_key if map_.is_del_key_set else NULL
        map_.is_del_key_set = False
        return value
    else:
        cell = _find_cell(map_.cells, map_.length, key)
        cell.key = DELETED_KEY
        # We shouldn't decrement the "filled" value here, as we're not actually
        # making "empty" values -- deleted values aren't quite the same.
        # Instead if we manage to insert into a deleted slot, we don't increment
        # the fill rate.
        return cell.value


cdef void* map_bulk_get(const MapStruct* map_, const key_t* keys, void** values,
                        int n) nogil:
    cdef int i
    for i in range(n):
        values[i] = map_get(map_, keys[i])


cdef bint map_iter(const MapStruct* map_, int* i, key_t* key, void** value) nogil:
    '''Iterate over the filled items, setting the current place in i, and the
    key and value.  Return False when iteration finishes.
    '''
    cdef const Cell* cell
    while i[0] < map_.length:
        cell = &map_.cells[i[0]]
        i[0] += 1
        if cell[0].key != EMPTY_KEY and cell[0].key != DELETED_KEY:
            key[0] = cell[0].key
            value[0] = cell[0].value
            return True
    # Remember to check for cells keyed by the special empty and deleted keys
    if i[0] == map_.length:
        i[0] += 1
        if map_.is_empty_key_set:
            key[0] = EMPTY_KEY
            value[0] = map_.value_for_empty_key
            return True
    if i[0] == map_.length + 1:
        i[0] += 1
        if map_.is_del_key_set:
            key[0] = DELETED_KEY
            value[0] = map_.value_for_del_key
            return True
    return False


@cython.cdivision
cdef inline Cell* _find_cell(Cell* cells, const key_t size, const key_t key) nogil:
    # Modulo for powers-of-two via bitwise &
    cdef key_t i = (key & (size - 1))
    while cells[i].key != EMPTY_KEY and cells[i].key != key:
        i = (i + 1) & (size - 1)
    return &cells[i]


@cython.cdivision
cdef inline Cell* _find_cell_for_insertion(Cell* cells, const key_t size, const key_t key) nogil:
    """Find the correct cell to insert a value, which could be a previously
    deleted cell. If we cross a deleted cell and the key is in the table, we
    mark the later cell as deleted, and return the earlier one."""
    cdef Cell* deleted = NULL
    # Modulo for powers-of-two via bitwise &
    cdef key_t i = (key & (size - 1))
    while cells[i].key != EMPTY_KEY and cells[i].key != key:
        if cells[i].key == DELETED_KEY:
            deleted = &cells[i]
        i = (i + 1) & (size - 1)
    if deleted is not NULL:
        if cells[i].key == key:
            # We need to ensure we don't end up with the key in the table twice.
            # If we're using a deleted cell and we also have the key, we mark
            # the later cell as deleted.
            cells[i].key = DELETED_KEY
        return deleted
    return &cells[i]


cdef void _resize(Pool mem, MapStruct* map_) except *:
    cdef size_t new_size = map_.length * 2
    cdef Cell* old_cells = map_.cells
    cdef size_t old_size = map_.length

    map_.length = new_size
    map_.filled = 0
    map_.cells = <Cell*>mem.alloc(new_size, sizeof(Cell))
    
    cdef size_t i
    cdef size_t slot
    for i in range(old_size):
        if old_cells[i].key != EMPTY_KEY and old_cells[i].key != DELETED_KEY:
            map_set(mem, map_, old_cells[i].key, old_cells[i].value)
    mem.free(old_cells)
