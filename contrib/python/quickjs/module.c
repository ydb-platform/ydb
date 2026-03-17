#include <Python.h>
#include <time.h>

#include "upstream-quickjs/quickjs.h"

// Node of Python callable that the context needs to keep available.
typedef struct PythonCallableNode PythonCallableNode;
struct PythonCallableNode {
	PyObject *obj;
	PythonCallableNode *prev;
	PythonCallableNode *next;
};

// Keeps track of the time if we are using a time limit.
typedef struct {
	clock_t start;
	clock_t limit;
} InterruptData;

// The data of the type _quickjs.Context.
typedef struct {
	PyObject_HEAD JSRuntime *runtime;
	JSContext *context;
	int has_time_limit;
	clock_t time_limit;
	// Used when releasing the GIL.
	PyThreadState *thread_state;
	InterruptData interrupt_data;
	// NULL-terminated doubly linked list of callable Python objects that we need to keep track of.
	// We need to store references to callables in a place where we can access them when running
	// Python's GC. Having them stored only in QuickJS' function opaques would create a dependency
	// cycle across Python and QuickJS that neither GC can notice.
	PythonCallableNode *python_callables;
} RuntimeData;

// The data of the type _quickjs.Object.
typedef struct {
	PyObject_HEAD;
	RuntimeData *runtime_data;
	JSValue object;
} ObjectData;

// The exception raised by this module.
static PyObject *JSException = NULL;
static PyObject *StackOverflow = NULL;
// Converts the current Javascript exception to a Python exception via a C string.
static void quickjs_exception_to_python(JSContext *context);
// Converts a JSValue to a Python object.
//
// Takes ownership of the JSValue and will deallocate it (refcount reduced by 1).
static PyObject *quickjs_to_python(RuntimeData *runtime_data, JSValue value);

// Returns nonzero if we should stop due to a time limit.
static int js_interrupt_handler(JSRuntime *rt, void *opaque) {
	InterruptData *data = opaque;
	if (clock() - data->start >= data->limit) {
		return 1;
	} else {
		return 0;
	}
}

// Sets up a context and an InterruptData struct if the context has a time limit.
static void setup_time_limit(RuntimeData *runtime_data, InterruptData *interrupt_data) {
	if (runtime_data->has_time_limit) {
		JS_SetInterruptHandler(runtime_data->runtime, js_interrupt_handler, interrupt_data);
		interrupt_data->limit = runtime_data->time_limit;
		interrupt_data->start = clock();
	}
}

// Restores the context if the context has a time limit.
static void teardown_time_limit(RuntimeData *runtime_data) {
	if (runtime_data->has_time_limit) {
		JS_SetInterruptHandler(runtime_data->runtime, NULL, NULL);
	}
}

// This method is always called in a context before running JS code in QuickJS. It sets up time
// limites, releases the GIL etc.
static void prepare_call_js(RuntimeData *runtime_data) {
	// We release the GIL in order to speed things up for certain use cases.
	assert(!runtime_data->thread_state);
	runtime_data->thread_state = PyEval_SaveThread();
	JS_UpdateStackTop(runtime_data->runtime);
	setup_time_limit(runtime_data, &runtime_data->interrupt_data);
}

// This method is called right after returning from running JS code. Aquires the GIL etc.
static void end_call_js(RuntimeData *runtime_data) {
	teardown_time_limit(runtime_data);
	assert(runtime_data->thread_state);
	PyEval_RestoreThread(runtime_data->thread_state);
	runtime_data->thread_state = NULL;
}

// Called when Python is called again from inside QuickJS.
static void prepare_call_python(RuntimeData *runtime_data) {
	assert(runtime_data->thread_state);
	PyEval_RestoreThread(runtime_data->thread_state);
	runtime_data->thread_state = NULL;
}

// Called when the operation started by prepare_call_python is done.
static void end_call_python(RuntimeData *runtime_data) {
	assert(!runtime_data->thread_state);
	runtime_data->thread_state = PyEval_SaveThread();
}

// GC traversal.
static int object_traverse(ObjectData *self, visitproc visit, void *arg) {
	Py_VISIT(self->runtime_data);
	return 0;
}

// Creates an instance of the Object class.
static PyObject *object_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
	ObjectData *self = PyObject_GC_New(ObjectData, type);
	if (self != NULL) {
		self->runtime_data = NULL;
	}
	return (PyObject *)self;
}

// Deallocates an instance of the Object class.
static void object_dealloc(ObjectData *self) {
	if (self->runtime_data) {
		PyObject_GC_UnTrack(self);
		JS_FreeValue(self->runtime_data->context, self->object);
		// We incremented the refcount of the runtime data when we created this object, so we should
		// decrease it now so we don't leak memory.
		Py_CLEAR(self->runtime_data);
	}
	PyObject_GC_Del(self);
}

// _quickjs.Object.__call__
static PyObject *object_call(ObjectData *self, PyObject *args, PyObject *kwds);

// _quickjs.Object.json
//
// Returns the JSON representation of the object as a Python string.
static PyObject *object_json(ObjectData *self) {
	JSContext *context = self->runtime_data->context;
	JSValue json_string = JS_JSONStringify(context, self->object, JS_UNDEFINED, JS_UNDEFINED);
	return quickjs_to_python(self->runtime_data, json_string);
}

// All methods of the _quickjs.Object class.
static PyMethodDef object_methods[] = {
    {"json", (PyCFunction)object_json, METH_NOARGS, "Converts to a JSON string."},
    {NULL} /* Sentinel */
};

// Define the quickjs.Object type.
static PyTypeObject Object = {PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_quickjs.Object",
                              .tp_doc = "Quickjs object",
                              .tp_basicsize = sizeof(ObjectData),
                              .tp_itemsize = 0,
                              .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
                              .tp_traverse = (traverseproc)object_traverse,
                              .tp_new = object_new,
                              .tp_dealloc = (destructor)object_dealloc,
                              .tp_call = (ternaryfunc)object_call,
                              .tp_methods = object_methods};

// Whether converting item to QuickJS would be possible.
static int python_to_quickjs_possible(RuntimeData *runtime_data, PyObject *item) {
	if (PyBool_Check(item)) {
		return 1;
	} else if (PyLong_Check(item)) {
		return 1;
	} else if (PyFloat_Check(item)) {
		return 1;
	} else if (item == Py_None) {
		return 1;
	} else if (PyUnicode_Check(item)) {
		return 1;
	} else if (PyObject_IsInstance(item, (PyObject *)&Object)) {
		ObjectData *object = (ObjectData *)item;
		if (object->runtime_data != runtime_data) {
			PyErr_Format(PyExc_ValueError, "Can not mix JS objects from different contexts.");
			return 0;
		}
		return 1;
	} else {
		PyErr_Format(PyExc_TypeError,
		             "Unsupported type when converting a Python object to quickjs: %s.",
		             Py_TYPE(item)->tp_name);
		return 0;
	}
}

// Converts item to QuickJS.
//
// If the Python object is not possible to convert to JS, undefined will be returned. This fallback
// will not be used if python_to_quickjs_possible returns 1.
static JSValueConst python_to_quickjs(RuntimeData *runtime_data, PyObject *item) {
	if (PyBool_Check(item)) {
		return JS_MKVAL(JS_TAG_BOOL, item == Py_True ? 1 : 0);
	} else if (PyLong_Check(item)) {
		int overflow;
		long value = PyLong_AsLongAndOverflow(item, &overflow);
		if (overflow) {
			PyObject *float_value = PyNumber_Float(item);
			double double_value = PyFloat_AsDouble(float_value);
			Py_DECREF(float_value);
			return JS_NewFloat64(runtime_data->context, double_value);
		} else {
			return JS_MKVAL(JS_TAG_INT, value);
		}
	} else if (PyFloat_Check(item)) {
		return JS_NewFloat64(runtime_data->context, PyFloat_AsDouble(item));
	} else if (item == Py_None) {
		return JS_NULL;
	} else if (PyUnicode_Check(item)) {
		return JS_NewString(runtime_data->context, PyUnicode_AsUTF8(item));
	} else if (PyObject_IsInstance(item, (PyObject *)&Object)) {
		return JS_DupValue(runtime_data->context, ((ObjectData *)item)->object);
	} else {
		// Can not happen if python_to_quickjs_possible passes.
		return JS_UNDEFINED;
	}
}

// _quickjs.Object.__call__
static PyObject *object_call(ObjectData *self, PyObject *args, PyObject *kwds) {
	if (self->runtime_data == NULL) {
		// This object does not have a context and has not been created by this module.
		Py_RETURN_NONE;
	}

	// We first loop through all arguments and check that they are supported without doing anything.
	// This makes the cleanup code simpler for the case where we have to raise an error.
	const int nargs = PyTuple_Size(args);
	for (int i = 0; i < nargs; ++i) {
		PyObject *item = PyTuple_GetItem(args, i);
		if (!python_to_quickjs_possible(self->runtime_data, item)) {
			return NULL;
		}
	}

	// Now we know that all arguments are supported and we can convert them.
	JSValueConst *jsargs;
	if (nargs) {
		jsargs = js_malloc(self->runtime_data->context, nargs * sizeof(JSValueConst));
		if (jsargs == NULL) {
			quickjs_exception_to_python(self->runtime_data->context);
			return NULL;
		}
	}
	for (int i = 0; i < nargs; ++i) {
		PyObject *item = PyTuple_GetItem(args, i);
		jsargs[i] = python_to_quickjs(self->runtime_data, item);
	}

	prepare_call_js(self->runtime_data);
	JSValue value;
	value = JS_Call(self->runtime_data->context, self->object, JS_NULL, nargs, jsargs);
	for (int i = 0; i < nargs; ++i) {
		JS_FreeValue(self->runtime_data->context, jsargs[i]);
	}
	if (nargs) {
		js_free(self->runtime_data->context, jsargs);
	}
	end_call_js(self->runtime_data);
	return quickjs_to_python(self->runtime_data, value);
}

// Converts the current Javascript exception to a Python exception via a C string.
static void quickjs_exception_to_python(JSContext *context) {
	JSValue exception = JS_GetException(context);
	const char *cstring = JS_ToCString(context, exception);
	const char *stack_cstring = NULL;
	if (!JS_IsNull(exception) && !JS_IsUndefined(exception)) {
		JSValue stack = JS_GetPropertyStr(context, exception, "stack");
		if (!JS_IsException(stack)) {
			stack_cstring = JS_ToCString(context, stack);
			JS_FreeValue(context, stack);
		}
	}
	if (cstring != NULL) {
		const char *safe_stack_cstring = stack_cstring ? stack_cstring : "";
		if (strstr(cstring, "stack overflow") != NULL) {
			PyErr_Format(StackOverflow, "%s\n%s", cstring, safe_stack_cstring);
		} else {
			PyErr_Format(JSException, "%s\n%s", cstring, safe_stack_cstring);
		}
	} else {
		// This has been observed to happen when different threads have used the same QuickJS
		// runtime, but not at the same time.
		// Could potentially be another problem though, since JS_ToCString may return NULL.
		PyErr_Format(JSException,
					 "(Failed obtaining QuickJS error string. Concurrency issue?)");
	}
	JS_FreeCString(context, cstring);
	JS_FreeCString(context, stack_cstring);
	JS_FreeValue(context, exception);
}

// Converts a JSValue to a Python object.
//
// Takes ownership of the JSValue and will deallocate it (refcount reduced by 1).
static PyObject *quickjs_to_python(RuntimeData *runtime_data, JSValue value) {
	JSContext *context = runtime_data->context;
	int tag = JS_VALUE_GET_TAG(value);
	// A return value of NULL means an exception.
	PyObject *return_value = NULL;

	if (tag == JS_TAG_INT) {
		return_value = Py_BuildValue("i", JS_VALUE_GET_INT(value));
	} else if (tag == JS_TAG_BIG_INT) {
		const char *cstring = JS_ToCString(context, value);
		return_value = PyLong_FromString(cstring, NULL, 10);
		JS_FreeCString(context, cstring);
	} else if (tag == JS_TAG_BOOL) {
		return_value = Py_BuildValue("O", JS_VALUE_GET_BOOL(value) ? Py_True : Py_False);
	} else if (tag == JS_TAG_NULL) {
		return_value = Py_None;
	} else if (tag == JS_TAG_UNDEFINED) {
		return_value = Py_None;
	} else if (tag == JS_TAG_EXCEPTION) {
		quickjs_exception_to_python(context);
	} else if (tag == JS_TAG_FLOAT64) {
		return_value = Py_BuildValue("d", JS_VALUE_GET_FLOAT64(value));
	} else if (tag == JS_TAG_STRING) {
		const char *cstring = JS_ToCString(context, value);
		return_value = Py_BuildValue("s", cstring);
		JS_FreeCString(context, cstring);
	} else if (tag == JS_TAG_OBJECT || tag == JS_TAG_MODULE || tag == JS_TAG_SYMBOL) {
		// This is a Javascript object or function. We wrap it in a _quickjs.Object.
		return_value = PyObject_CallObject((PyObject *)&Object, NULL);
		ObjectData *object = (ObjectData *)return_value;
		// This is important. Otherwise, the context may be deallocated before the object, which
		// will result in a segfault with high probability.
		Py_INCREF(runtime_data);
		object->runtime_data = runtime_data;
		PyObject_GC_Track(object);
		object->object = JS_DupValue(context, value);
	} else {
		PyErr_Format(PyExc_TypeError, "Unknown quickjs tag: %d", tag);
	}

	JS_FreeValue(context, value);
	if (return_value == Py_None) {
		// Can not simply return PyNone for refcounting reasons.
		Py_RETURN_NONE;
	}
	return return_value;
}

static PyObject *test(PyObject *self, PyObject *args) {
	return Py_BuildValue("i", 42);
}

// Global state of the module. Currently none.
struct module_state {};

// GC traversal.
static int runtime_traverse(RuntimeData *self, visitproc visit, void *arg) {
	PythonCallableNode *node = self->python_callables;
	while (node) {
		Py_VISIT(node->obj);
		node = node->next;
	}
	return 0;
}

// GC clearing. Object does not have a clearing method, therefore dependency cycles
// between Context and Object will always be cleared starting here.
static int runtime_clear(RuntimeData *self) {
	PythonCallableNode *node = self->python_callables;
	while (node) {
		Py_CLEAR(node->obj);
		node = node->next;
	}
	return 0;
}

static JSClassID js_python_function_class_id;

static void js_python_function_finalizer(JSRuntime *rt, JSValue val) {
	PythonCallableNode *node = JS_GetOpaque(val, js_python_function_class_id);
	RuntimeData *runtime_data = JS_GetRuntimeOpaque(rt);
	if (node) {
		// fail safe
		JS_SetOpaque(val, NULL);
		// NOTE: This may be called from e.g. runtime_dealloc, but also from
		// e.g. JS_Eval, so we need to ensure that we are in the correct state.
		// TODO: integrate better with (prepare|end)_call_(python|js).
		if (runtime_data->thread_state) {
			PyEval_RestoreThread(runtime_data->thread_state);
		}
		if (node->prev) {
			node->prev->next = node->next;
		} else {
			runtime_data->python_callables = node->next;
		}
		if (node->next) {
			node->next->prev = node->prev;
		}
		// May have just been cleared in runtime_clear.
		Py_XDECREF(node->obj);
		PyMem_Free(node);
		if (runtime_data->thread_state) {
			runtime_data->thread_state = PyEval_SaveThread();
		}
	};
}

static JSValue js_python_function_call(JSContext *ctx, JSValueConst func_obj,
                                       JSValueConst this_val, int argc, JSValueConst *argv,
                                       int flags) {
	RuntimeData *runtime_data = (RuntimeData *)JS_GetRuntimeOpaque(JS_GetRuntime(ctx));
	PythonCallableNode *node = JS_GetOpaque(func_obj, js_python_function_class_id);
	if (runtime_data->has_time_limit) {
		return JS_ThrowInternalError(ctx, "Can not call into Python with a time limit set.");
	}
	prepare_call_python(runtime_data);

	PyObject *args = PyTuple_New(argc);
	if (!args) {
		end_call_python(runtime_data);
		return JS_ThrowOutOfMemory(ctx);
	}
	int tuple_success = 1;
	for (int i = 0; i < argc; ++i) {
		PyObject *arg = quickjs_to_python(runtime_data, JS_DupValue(ctx, argv[i]));
		if (!arg) {
			tuple_success = 0;
			break;
		}
		PyTuple_SET_ITEM(args, i, arg);
	}
	if (!tuple_success) {
		Py_DECREF(args);
		end_call_python(runtime_data);
		return JS_ThrowInternalError(ctx, "Internal error: could not convert args.");
	}

	PyObject *result = PyObject_CallObject(node->obj, args);
	Py_DECREF(args);
	if (!result) {
		end_call_python(runtime_data);
		return JS_ThrowInternalError(ctx, "Python call failed.");
	}
	JSValue js_result = JS_NULL;
	if (python_to_quickjs_possible(runtime_data, result)) {
		js_result = python_to_quickjs(runtime_data, result);
	} else {
		PyErr_Clear();
		js_result = JS_ThrowInternalError(ctx, "Can not convert Python result to JS.");
	}
	Py_DECREF(result);

	end_call_python(runtime_data);
	return js_result;
}

static JSClassDef js_python_function_class = {
	"PythonFunction",
	.finalizer = js_python_function_finalizer,
	.call = js_python_function_call,
};

// Creates an instance of the _quickjs.Context class.
static PyObject *runtime_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
	RuntimeData *self = PyObject_GC_New(RuntimeData, type);
	if (self != NULL) {
		// We never have different contexts for the same runtime. This way, different
		// _quickjs.Context can be used concurrently.
		self->runtime = JS_NewRuntime();
		self->context = JS_NewContext(self->runtime);
		JS_NewClass(self->runtime, js_python_function_class_id,
		            &js_python_function_class);
		JSValue global = JS_GetGlobalObject(self->context);
		JSValue fct_cls = JS_GetPropertyStr(self->context, global, "Function");
		JSValue fct_proto = JS_GetPropertyStr(self->context, fct_cls, "prototype");
		JS_FreeValue(self->context, fct_cls);
		JS_SetClassProto(self->context, js_python_function_class_id, fct_proto);
		JS_FreeValue(self->context, global);
		self->has_time_limit = 0;
		self->time_limit = 0;
		self->thread_state = NULL;
		self->python_callables = NULL;
		JS_SetRuntimeOpaque(self->runtime, self);
		PyObject_GC_Track(self);
	}
	return (PyObject *)self;
}

// Deallocates an instance of the _quickjs.Context class.
static void runtime_dealloc(RuntimeData *self) {
	JS_FreeContext(self->context);
	JS_FreeRuntime(self->runtime);
	PyObject_GC_UnTrack(self);
	PyObject_GC_Del(self);
}

// Evaluates a Python string as JS and returns the result as a Python object. Will return
// _quickjs.Object for complex types (other than e.g. str, int).
static PyObject *runtime_eval_internal(RuntimeData *self, PyObject *args, int eval_type) {
	const char *code;
	if (!PyArg_ParseTuple(args, "s", &code)) {
		return NULL;
	}
	prepare_call_js(self);
	JSValue value;
	value = JS_Eval(self->context, code, strlen(code), "<input>", eval_type);
	end_call_js(self);
	return quickjs_to_python(self, value);
}

// _quickjs.Context.eval
//
// Evaluates a Python string as JS and returns the result as a Python object. Will return
// _quickjs.Object for complex types (other than e.g. str, int).
static PyObject *runtime_eval(RuntimeData *self, PyObject *args) {
	return runtime_eval_internal(self, args, JS_EVAL_TYPE_GLOBAL);
}

// _quickjs.Context.module
//
// Evaluates a Python string as JS module. Otherwise identical to eval.
static PyObject *runtime_module(RuntimeData *self, PyObject *args) {
	return runtime_eval_internal(self, args, JS_EVAL_TYPE_MODULE);
}

// _quickjs.Context.execute_pending_job
//
// If there are pending jobs, executes one and returns True. Else returns False.
static PyObject *runtime_execute_pending_job(RuntimeData *self) {
	prepare_call_js(self);
	JSContext *ctx;
	int ret = JS_ExecutePendingJob(self->runtime, &ctx);
	end_call_js(self);
	if (ret > 0) {
		Py_RETURN_TRUE;
	} else if (ret == 0) {
		Py_RETURN_FALSE;
	} else {
		quickjs_exception_to_python(ctx);
		return NULL;
	}
}

// _quickjs.Context.parse_json
//
// Evaluates a Python string as JSON and returns the result as a Python object. Will
// return _quickjs.Object for complex types (other than e.g. str, int).
static PyObject *runtime_parse_json(RuntimeData *self, PyObject *args) {
	const char *data;
	if (!PyArg_ParseTuple(args, "s", &data)) {
		return NULL;
	}
	JSValue value;
	Py_BEGIN_ALLOW_THREADS;
	value = JS_ParseJSON(self->context, data, strlen(data), "runtime_parse_json.json");
	Py_END_ALLOW_THREADS;
	return quickjs_to_python(self, value);
}

// _quickjs.Context.get
//
// Retrieves a global variable from the JS context.
static PyObject *runtime_get(RuntimeData *self, PyObject *args) {
	const char *name;
	if (!PyArg_ParseTuple(args, "s", &name)) {
		return NULL;
	}
	JSValue global = JS_GetGlobalObject(self->context);
	JSValue value = JS_GetPropertyStr(self->context, global, name);
	JS_FreeValue(self->context, global);
	return quickjs_to_python(self, value);
}

// _quickjs.Context.set
//
// Sets a global variable to the JS context.
static PyObject *runtime_set(RuntimeData *self, PyObject *args) {
	const char *name;
	PyObject *item;
	if (!PyArg_ParseTuple(args, "sO", &name, &item)) {
		return NULL;
	}
	JSValue global = JS_GetGlobalObject(self->context);
	int ret = 0;
	if (python_to_quickjs_possible(self, item)) {
		ret = JS_SetPropertyStr(self->context, global, name, python_to_quickjs(self, item));
		if (ret != 1) {
			PyErr_SetString(PyExc_TypeError, "Failed setting the variable.");
		}
	}
	JS_FreeValue(self->context, global);
	if (ret == 1) {
		Py_RETURN_NONE;
	} else {
		return NULL;
	}
}

// _quickjs.Context.set_memory_limit
//
// Sets the memory limit of the context.
static PyObject *runtime_set_memory_limit(RuntimeData *self, PyObject *args) {
	Py_ssize_t limit;
	if (!PyArg_ParseTuple(args, "n", &limit)) {
		return NULL;
	}
	JS_SetMemoryLimit(self->runtime, limit);
	Py_RETURN_NONE;
}

// _quickjs.Context.set_time_limit
//
// Sets the CPU time limit of the context. This will be used in an interrupt handler.
static PyObject *runtime_set_time_limit(RuntimeData *self, PyObject *args) {
	double limit;
	if (!PyArg_ParseTuple(args, "d", &limit)) {
		return NULL;
	}
	if (limit < 0) {
		self->has_time_limit = 0;
	} else {
		self->has_time_limit = 1;
		self->time_limit = (clock_t)(limit * CLOCKS_PER_SEC);
	}
	Py_RETURN_NONE;
}

// _quickjs.Context.set_max_stack_size
//
// Sets the max stack size in bytes.
static PyObject *runtime_set_max_stack_size(RuntimeData *self, PyObject *args) {
	Py_ssize_t limit;
	if (!PyArg_ParseTuple(args, "n", &limit)) {
		return NULL;
	}
	JS_SetMaxStackSize(self->runtime, limit);
	Py_RETURN_NONE;
}

// _quickjs.Context.memory
//
// Sets the CPU time limit of the context. This will be used in an interrupt handler.
static PyObject *runtime_memory(RuntimeData *self) {
	PyObject *dict = PyDict_New();
	if (dict == NULL) {
		return NULL;
	}
	JSMemoryUsage usage;
	JS_ComputeMemoryUsage(self->runtime, &usage);
#define MEM_USAGE_ADD_TO_DICT(key)                          \
	{                                                       \
		PyObject *value = PyLong_FromLongLong(usage.key);   \
		if (PyDict_SetItemString(dict, #key, value) != 0) { \
			return NULL;                                    \
		}                                                   \
		Py_DECREF(value);                                   \
	}
	MEM_USAGE_ADD_TO_DICT(malloc_size);
	MEM_USAGE_ADD_TO_DICT(malloc_limit);
	MEM_USAGE_ADD_TO_DICT(memory_used_size);
	MEM_USAGE_ADD_TO_DICT(malloc_count);
	MEM_USAGE_ADD_TO_DICT(memory_used_count);
	MEM_USAGE_ADD_TO_DICT(atom_count);
	MEM_USAGE_ADD_TO_DICT(atom_size);
	MEM_USAGE_ADD_TO_DICT(str_count);
	MEM_USAGE_ADD_TO_DICT(str_size);
	MEM_USAGE_ADD_TO_DICT(obj_count);
	MEM_USAGE_ADD_TO_DICT(obj_size);
	MEM_USAGE_ADD_TO_DICT(prop_count);
	MEM_USAGE_ADD_TO_DICT(prop_size);
	MEM_USAGE_ADD_TO_DICT(shape_count);
	MEM_USAGE_ADD_TO_DICT(shape_size);
	MEM_USAGE_ADD_TO_DICT(js_func_count);
	MEM_USAGE_ADD_TO_DICT(js_func_size);
	MEM_USAGE_ADD_TO_DICT(js_func_code_size);
	MEM_USAGE_ADD_TO_DICT(js_func_pc2line_count);
	MEM_USAGE_ADD_TO_DICT(js_func_pc2line_size);
	MEM_USAGE_ADD_TO_DICT(c_func_count);
	MEM_USAGE_ADD_TO_DICT(array_count);
	MEM_USAGE_ADD_TO_DICT(fast_array_count);
	MEM_USAGE_ADD_TO_DICT(fast_array_elements);
	MEM_USAGE_ADD_TO_DICT(binary_object_count);
	MEM_USAGE_ADD_TO_DICT(binary_object_size);
	return dict;
}

// _quickjs.Context.gc
//
// Runs garbage collection.
static PyObject *runtime_gc(RuntimeData *self) {
	JS_RunGC(self->runtime);
	Py_RETURN_NONE;
}


static PyObject *runtime_add_callable(RuntimeData *self, PyObject *args) {
	const char *name;
	PyObject *callable;
	if (!PyArg_ParseTuple(args, "sO", &name, &callable)) {
		return NULL;
	}
	if (!PyCallable_Check(callable)) {
		PyErr_SetString(PyExc_TypeError, "Argument must be callable.");
		return NULL;
	}

	JSValue function = JS_NewObjectClass(self->context, js_python_function_class_id);
	if (JS_IsException(function)) {
		quickjs_exception_to_python(self->context);
		return NULL;
	}
	// TODO: Should we allow setting the .length of the function to something other than 0?
	JS_DefinePropertyValueStr(self->context, function, "name", JS_NewString(self->context, name), JS_PROP_CONFIGURABLE);
	PythonCallableNode *node = PyMem_Malloc(sizeof(PythonCallableNode));
	if (!node) {
		JS_FreeValue(self->context, function);
		return NULL;
	}
	Py_INCREF(callable);
	node->obj = callable;
	node->prev = NULL;
	node->next = self->python_callables;
	if (self->python_callables) {
		self->python_callables->prev = node;
	}
	self->python_callables = node;
	JS_SetOpaque(function, node);

	JSValue global = JS_GetGlobalObject(self->context);
	if (JS_IsException(global)) {
		JS_FreeValue(self->context, function);
		quickjs_exception_to_python(self->context);
		return NULL;
	}
	// If this fails we don't notify the caller of this function.
	int ret = JS_SetPropertyStr(self->context, global, name, function);
	JS_FreeValue(self->context, global);
	if (ret != 1) {
		PyErr_SetString(PyExc_TypeError, "Failed adding the callable.");
		return NULL;
	} else {
		Py_RETURN_NONE;
	}
}


// _quickjs.Context.globalThis
//
// Global object of the JS context.
static PyObject *runtime_global_this(RuntimeData *self, void *closure) {
	return quickjs_to_python(self, JS_GetGlobalObject(self->context));
}


// All methods of the _quickjs.Context class.
static PyMethodDef runtime_methods[] = {
    {"eval", (PyCFunction)runtime_eval, METH_VARARGS, "Evaluates a Javascript string."},
    {"module",
     (PyCFunction)runtime_module,
     METH_VARARGS,
     "Evaluates a Javascript string as a module."},
    {"execute_pending_job", (PyCFunction)runtime_execute_pending_job, METH_NOARGS, "Executes a pending job."},
    {"parse_json", (PyCFunction)runtime_parse_json, METH_VARARGS, "Parses a JSON string."},
    {"get", (PyCFunction)runtime_get, METH_VARARGS, "Gets a Javascript global variable."},
    {"set", (PyCFunction)runtime_set, METH_VARARGS, "Sets a Javascript global variable."},
    {"set_memory_limit",
     (PyCFunction)runtime_set_memory_limit,
     METH_VARARGS,
     "Sets the memory limit in bytes."},
    {"set_time_limit",
     (PyCFunction)runtime_set_time_limit,
     METH_VARARGS,
     "Sets the CPU time limit in seconds (C function clock() is used)."},
    {"set_max_stack_size",
     (PyCFunction)runtime_set_max_stack_size,
     METH_VARARGS,
     "Sets the maximum stack size in bytes. Default is 256kB."},
    {"memory", (PyCFunction)runtime_memory, METH_NOARGS, "Returns the memory usage as a dict."},
    {"gc", (PyCFunction)runtime_gc, METH_NOARGS, "Runs garbage collection."},
    {"add_callable", (PyCFunction)runtime_add_callable, METH_VARARGS, "Wraps a Python callable."},
    {NULL} /* Sentinel */
};

// All getsetters (properties) of the _quickjs.Context class.
static PyGetSetDef runtime_getsetters[] = {
    {"globalThis", (getter)runtime_global_this, NULL, "Global object of the context.", NULL},
    {NULL} /* Sentinel */
};

// Define the _quickjs.Context type.
static PyTypeObject Context = {PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_quickjs.Context",
                               .tp_doc = "Quickjs context",
                               .tp_basicsize = sizeof(RuntimeData),
                               .tp_itemsize = 0,
                               .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
                               .tp_traverse = (traverseproc)runtime_traverse,
                               .tp_clear = (inquiry)runtime_clear,
                               .tp_new = runtime_new,
                               .tp_dealloc = (destructor)runtime_dealloc,
                               .tp_methods = runtime_methods,
                               .tp_getset = runtime_getsetters};

// All global methods in _quickjs.
static PyMethodDef myextension_methods[] = {{"test", (PyCFunction)test, METH_NOARGS, NULL},
                                            {NULL, NULL}};

// Define the _quickjs module.
static struct PyModuleDef moduledef = {PyModuleDef_HEAD_INIT,
                                       "quickjs",
                                       NULL,
                                       sizeof(struct module_state),
                                       myextension_methods,
                                       NULL,
                                       NULL,
                                       NULL,
                                       NULL};

// This function runs when the module is first imported.
PyMODINIT_FUNC PyInit__quickjs(void) {
	if (PyType_Ready(&Context) < 0) {
		return NULL;
	}
	if (PyType_Ready(&Object) < 0) {
		return NULL;
	}

	PyObject *module = PyModule_Create(&moduledef);
	if (module == NULL) {
		return NULL;
	}

	JS_NewClassID(&js_python_function_class_id);

	JSException = PyErr_NewException("_quickjs.JSException", NULL, NULL);
	if (JSException == NULL) {
		return NULL;
	}
	StackOverflow = PyErr_NewException("_quickjs.StackOverflow", JSException, NULL);
	if (StackOverflow == NULL) {
		return NULL;
	}

	Py_INCREF(&Context);
	PyModule_AddObject(module, "Context", (PyObject *)&Context);
	Py_INCREF(&Object);
	PyModule_AddObject(module, "Object", (PyObject *)&Object);
	PyModule_AddObject(module, "JSException", JSException);
	PyModule_AddObject(module, "StackOverflow", StackOverflow);
	return module;
}
