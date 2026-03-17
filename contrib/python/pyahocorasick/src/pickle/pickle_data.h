#pragma once

typedef struct PickleData {
	PyObject*	bytes_list;	///< PyList of PyBytes
	bool		chunked;	///< bytes_list has more than one element
	size_t		size;		///< size of single array
	uint8_t*	data;		///< current array
	Py_ssize_t*	count;		///< ptr to number of nodes stored in the current array
	size_t		top;		///< first free address in the current array

	PyObject* 	values;		///< a list (if store == STORE_ANY)
	bool		error;		///< error occurred during pickling
} PickleData;


static void
pickle_data__init_default(PickleData* data);

static void
pickle_data__cleanup(PickleData* data);

static bool
pickle_data__add_next_buffer(PickleData* data);

static bool
pickle_data__shrink_last_buffer(PickleData* data);

static int
pickle_data__init(PickleData* data, KeysStore store, size_t total_size, size_t max_array_size);
