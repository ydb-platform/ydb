#include "pickle.h"
#include "pickle_data.h"


static void
pickle_data__init_default(PickleData* data) {
	ASSERT(data != NULL);

	data->bytes_list	= NULL;
	data->chunked		= false;
	data->size			= 0;
	data->data			= NULL;
	data->count			= NULL;
	data->top			= 0;
	data->values		= 0;
	data->error			= false;
}


static void
pickle_data__cleanup(PickleData* data) {
	ASSERT(data != NULL);

	Py_XDECREF(data->bytes_list);
	Py_XDECREF(data->values);
}


static bool
pickle_data__add_next_buffer(PickleData* data) {

	PyObject* bytes;
	void* raw;

	ASSERT(data != NULL);

	bytes = F(PyBytes_FromStringAndSize)(NULL, data->size);
	if (UNLIKELY(bytes == NULL)) {
		return false;
	}

	if (UNLIKELY(F(PyList_Append)(data->bytes_list, bytes) < 0)) {
		Py_DECREF(bytes);
		return false;
	}

	raw = PyBytes_AS_STRING(bytes);

	data->count 	= (Py_ssize_t*)raw;
	(*data->count)	= 0;

	data->data  	= (uint8_t*)raw;
	data->top   	= PICKLE_CHUNK_COUNTER_SIZE;

	return true;
}


static bool
pickle_data__shrink_last_buffer(PickleData* data) {

	PyObject* bytes;
	PyObject* new;
	Py_ssize_t last_idx;

	ASSERT(data != NULL);

	if (data->top >= data->size) {
		return true;
	}

	ASSERT(data->bytes_list);

	last_idx = PyList_GET_SIZE(data->bytes_list) - 1;

	bytes = F(PyList_GetItem)(data->bytes_list, last_idx);
	if (UNLIKELY(bytes == NULL)) {
		return false;
	}

	new = F(PyBytes_FromStringAndSize)(PyBytes_AS_STRING(bytes), data->top);
	if (UNLIKELY(new == NULL)) {
		return false;
	}

	if (F(PyList_SetItem)(data->bytes_list, last_idx, new) < 0) {
		return false;
	}

	return true;
}


static int
pickle_data__init(PickleData* data, KeysStore store, size_t total_size, size_t max_array_size) {

	pickle_data__init_default(data);

	ASSERT(total_size > 0);
	ASSERT(max_array_size > PICKLE_TRIENODE_SIZE * 1024);

	data->bytes_list = F(PyList_New)(0);
	if (UNLIKELY(data->bytes_list == NULL)) {
		return false;
	}

	if (store == STORE_ANY) {
		data->values = F(PyList_New)(0);
		if (UNLIKELY(data->values == NULL)) {
			Py_DECREF(data->bytes_list);
			return false;
		}
	}

	if (total_size <= max_array_size) {
		data->size = total_size + PICKLE_CHUNK_COUNTER_SIZE;
		data->chunked = false;
	} else {
		// TODO: more heuristic here: what if total_size > 100MB? what if > 1GB, > 10GB?
		data->size = max_array_size;
		data->chunked = true;
	}

	return pickle_data__add_next_buffer(data);
}

