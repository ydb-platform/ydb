#include <contrib/python/uWSGI/py2/config.h>
#include "uwsgi_python.h"

extern struct uwsgi_server uwsgi;
extern struct uwsgi_python up;


struct _symimporter {
	PyObject_HEAD;
} uwsgi_symbol_importer_object;

struct _symzipimporter {
	PyObject_HEAD;
	char *prefix;
	PyObject *zip;
        PyObject *items;
} uwsgi_symbol_zip_importer_object;


static char *symbolize(char *name) {

	char *base = uwsgi_concat2(name, "");
	char *ptr = base;
	while(*ptr != 0) {
		if (*ptr == '.') {
			*ptr = '_';
		}
		ptr++;
	}

	return base;
}

static char *name_to_py(char *prefix, char *name) {

	char *base;
	char *ptr;

	if (prefix) {
		if (prefix[strlen(prefix)-1] == '/') { 
        		base = uwsgi_concat3(prefix, name, ".py");
			ptr = base + strlen(prefix);
		}
		else {
        		base = uwsgi_concat4(prefix, "/", name, ".py");
			ptr = base + strlen(prefix) + 1;
		}
	}
	else {
        	base = uwsgi_concat2(name, ".py");
		ptr = base;
	}
        while(*ptr != 0) {
                if (*ptr == '.') {
                        *ptr = '/';
                }
                ptr++;
        }

	// fix .py
	ptr-=3;
	*ptr = '.';
        return base;
}

static char *name_to_init_py(char *prefix, char *name) {

	char *base;
	char *ptr;

	if (prefix) {
		if (prefix[strlen(prefix)-1] == '/') { 
        		base = uwsgi_concat3(prefix, name, "/__init__.py");
			ptr = base + strlen(prefix);
		}
		else {
        		base = uwsgi_concat4(prefix, "/", name, "/__init__.py");
			ptr = base + strlen(prefix) + 1;
		}
	}
	else {
        	base = uwsgi_concat2(name, "/__init__.py");
		ptr = base;
	}

        while(*ptr != 0) {
                if (*ptr == '.') {
                        *ptr = '/';
                }
                ptr++;
        }

        // fix .py
        ptr-=3;
        *ptr = '.';

        return base;
}




static char *name_to_symbol(char *name, char *what) {

        char *symbol = uwsgi_concat4("_binary_", name, "_", what);
        char *sym_ptr_start = dlsym(RTLD_DEFAULT, symbol);
        free(symbol);
        return sym_ptr_start;
}


static char *name_to_symbol_module(char *name, char *what) {

	char *symbol = uwsgi_concat4("_binary_", name, "_py_", what);
        char *sym_ptr_start = dlsym(RTLD_DEFAULT, symbol);
	free(symbol);
	return sym_ptr_start;
}

static char *name_to_symbol_pkg(char *name, char *what) {

	char *symbol = uwsgi_concat4("_binary_", name, "___init___py_", what);
        char *sym_ptr_start = dlsym(RTLD_DEFAULT, symbol);
	free(symbol);
	return sym_ptr_start;
}

int py_list_has_string(PyObject *obj, char *name) {

	Py_ssize_t i, len = PyList_Size(obj);
	int found = 0;
	for(i=0;i<len;i++) {
		PyObject *current = PyList_GetItem(obj, i);
		char *filename = PyString_AsString(current);
		if (!strcmp(filename, name)) {
			found = 1;
			break;
		}
	}

	return found;
}

static PyObject* symzipimporter_find_module(PyObject *self, PyObject *args) {

	char *fullname;
	PyObject *path = NULL;
	struct _symzipimporter *this = (struct _symzipimporter *) self;

	if (!PyArg_ParseTuple(args, "s|O:find_module", &fullname, &path)) {
		return NULL;
	}

	char *filename = name_to_py(this->prefix, fullname);

	if (py_list_has_string(this->items, filename)) {
		free(filename);
		return self;
	}

	PyErr_Clear();
	free(filename);

	filename = name_to_init_py(this->prefix, fullname);

	if (py_list_has_string(this->items, filename)) {
		free(filename);
		return self;
	}

	PyErr_Clear();
	free(filename);

	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject* symzipimporter_load_module(PyObject *self, PyObject *args) {

	char *fullname;
	char *modname;
        struct _symzipimporter *this = (struct _symzipimporter *) self;

        if (!PyArg_ParseTuple(args, "s:load_module", &fullname)) {
                return NULL;
        }

        char *filename = name_to_py(this->prefix, fullname);

        if (py_list_has_string(this->items, filename)) {
		PyObject *mod = PyImport_AddModule(fullname);
                if (!mod) goto clear;
                PyObject *dict = PyModule_GetDict(mod);
                if (!dict) goto clear;

                PyDict_SetItemString(dict, "__loader__", self);

                modname = uwsgi_concat2("symzip://", fullname);

		PyObject *source = PyObject_CallMethod(this->zip, "read", "(s)", filename);
                free(filename);
                PyObject *code = Py_CompileString(PyString_AsString(source), modname, Py_file_input);
		if (!code) {
			PyErr_Print();
			goto shit;
		}
                mod = PyImport_ExecCodeModuleEx(fullname, code, modname);

                Py_DECREF(code);
shit:
                Py_DECREF(source);
                free(modname);
                return mod;
        }

        PyErr_Clear();
        free(filename);

        filename = name_to_init_py(this->prefix, fullname);

        if (py_list_has_string(this->items, filename)) {
		PyObject *mod = PyImport_AddModule(fullname);
                if (!mod) goto clear;
                PyObject *dict = PyModule_GetDict(mod);
                if (!dict) goto clear;

                modname = uwsgi_concat2("symzip://", fullname);

		PyObject *pkgpath = Py_BuildValue("[O]", PyString_FromString(modname));

                PyDict_SetItemString(dict, "__path__", pkgpath);
                PyDict_SetItemString(dict, "__loader__", self);


                PyObject *source = PyObject_CallMethod(this->zip, "read", "(s)", filename);
                free(filename);
                PyObject *code = Py_CompileString(PyString_AsString(source), modname, Py_file_input);
		if (!code) {
			PyErr_Print();
			goto shit2;
		}
                mod = PyImport_ExecCodeModuleEx(fullname, code, modname);

                Py_DECREF(code);
shit2:
                Py_DECREF(source);
                free(modname);
                return mod;
        }

clear:
        PyErr_Clear();
        free(filename);

	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject* symimporter_find_module(PyObject *self, PyObject *args) {

	char *fullname;
	PyObject *path = NULL;

	if (!PyArg_ParseTuple(args, "s|O:find_module", &fullname, &path)) {
		return NULL;
	}

	char *fullname2 = symbolize(fullname);

	char *code_start = name_to_symbol_module(fullname2, "start");
	if (code_start) {
		free(fullname2);
		Py_INCREF(self);
		return self;
	}
	code_start = name_to_symbol_pkg(fullname2, "start");
	if (code_start) {
		free(fullname2);
		Py_INCREF(self);
		return self;
	}
	
	
	free(fullname2);
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject* symimporter_load_module(PyObject *self, PyObject *args) {

	char *code_start;
	char *code_end;
	char *fullname;
	char *source;
	char *modname;
	PyObject *code;
	if (!PyArg_ParseTuple(args, "s:load_module", &fullname)) {
                return NULL;
        }

	char *fullname2 = symbolize(fullname);

	code_start = name_to_symbol_module(fullname2, "start");
	if (code_start) {
		code_end = name_to_symbol_module(fullname2, "end");
		if (code_end) {
			PyObject *mod = PyImport_AddModule(fullname);
			if (!mod) goto clear;
			PyObject *dict = PyModule_GetDict(mod);
			if (!dict) goto clear;

			PyDict_SetItemString(dict, "__loader__", self);

			source = uwsgi_concat2n(code_start, code_end-code_start, "", 0);
			modname = uwsgi_concat3("sym://", fullname2, "_py");

			code = Py_CompileString(source, modname, Py_file_input);
		if (!code) {
			PyErr_Print();
			goto shit;
		}
			mod = PyImport_ExecCodeModuleEx(fullname, code, modname);

			Py_DECREF(code);
shit:
			free(source);
			free(modname);
			free(fullname2);
			return mod;	
		}
	}

	code_start = name_to_symbol_pkg(fullname2, "start");
        if (code_start) {
                code_end = name_to_symbol_pkg(fullname2, "end");
                if (code_end) {
			char *symbolized;
                        PyObject *mod = PyImport_AddModule(fullname);
			if (!mod) goto clear;
                        PyObject *dict = PyModule_GetDict(mod);
			if (!dict) goto clear;

                        source = uwsgi_concat2n(code_start, code_end-code_start, "", 0);
			symbolized = symbolize(fullname);
			modname = uwsgi_concat3("sym://", symbolized, "___init___py");

			PyObject *pkgpath = Py_BuildValue("[O]", PyString_FromString(modname));

			PyDict_SetItemString(dict, "__path__", pkgpath);
			PyDict_SetItemString(dict, "__loader__", self);

			code = Py_CompileString(source, modname, Py_file_input);
			if (!code) {
				PyErr_Print();
				goto shit2;
			}
                        mod = PyImport_ExecCodeModuleEx(fullname, code, modname);

			Py_DECREF(code);
shit2:
			free(symbolized);
			free(source);
			free(modname);
			free(fullname2);
                        return mod;
                }
        }

clear:
	free(fullname2);
	Py_INCREF(Py_None);
        return Py_None;
}

static PyMethodDef symimporter_methods[] = {
	{"find_module", symimporter_find_module, METH_VARARGS},
 	{"load_module", symimporter_load_module, METH_VARARGS},
	{ NULL, NULL },
};

static PyMethodDef symzipimporter_methods[] = {
	{"find_module", symzipimporter_find_module, METH_VARARGS},
	{"load_module", symzipimporter_load_module, METH_VARARGS},
	{ NULL, NULL },
};

static void uwsgi_symimporter_free(struct _symimporter *self) {
	PyObject_Del(self);
}


static PyTypeObject SymImporter_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "uwsgi.SymbolsImporter",
    sizeof(struct _symimporter),
    0,                                          /* tp_itemsize */
    (destructor) uwsgi_symimporter_free,            /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,
    "uwsgi symbols importer",                                          /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    symimporter_methods,                        /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    0,                 /* tp_init */
    PyType_GenericAlloc,                        /* tp_alloc */
    PyType_GenericNew,                          /* tp_new */
    0,                            /* tp_free */
};

static int
zipimporter_init(struct _symzipimporter *self, PyObject *args, PyObject *kwds)
{


        char *name;
        char *prefix = NULL;
	size_t len = 0;

        if (!PyArg_ParseTuple(args, "s", &name))
                return -1;

        // avoid GC !!!
        name = uwsgi_concat2(name, "");

	if (uwsgi_check_scheme(name)) {
                prefix = uwsgi_get_last_char(name, '/');
                prefix = uwsgi_get_last_char(prefix, ':');
        }
        else {
                prefix = uwsgi_get_last_char(name, ':');
        }

	if (prefix) {
		prefix[0] = 0;
	}


	char *body = uwsgi_open_and_read(name, &len, 0, NULL);
	if (!body) {
		return -1;
	}



	PyObject *stringio = PyImport_ImportModule("StringIO");
        if (!stringio) {
		free(body);
                return -1;
        }


#ifdef PYTHREE
        PyObject *source_code = PyObject_CallMethodObjArgs(stringio, PyString_FromString("StringIO"), PyString_FromStringAndSize(body, len));
#else
	PyObject *stringio_dict = PyModule_GetDict(stringio);
        if (!stringio_dict) {
                return -1;
        }


        PyObject *stringio_stringio = PyDict_GetItemString(stringio_dict, "StringIO");
        if (!stringio_stringio) {
                return -1;
        }


        PyObject *stringio_args = PyTuple_New(1);
        PyTuple_SetItem(stringio_args, 0, PyString_FromStringAndSize(body, len));



        PyObject *source_code = PyInstance_New(stringio_stringio, stringio_args, NULL);
#endif
        if (!source_code) {
                return -1;
        }


        PyObject *zipfile = PyImport_ImportModule("zipfile");

        if (!zipfile) {
		PyErr_Print();
                return -1;
        }


#ifdef PYTHREE
	self->zip = PyObject_CallMethodObjArgs(zipfile, PyString_FromString("ZipFile"), source_code);
#else
	PyObject *zipfile_dict = PyModule_GetDict(zipfile);
        if (!zipfile_dict) {
                return -1;
        }

        PyObject *zipfile_zipfile = PyDict_GetItemString(zipfile_dict, "ZipFile");
        if (!zipfile_zipfile) {
                return -1;
        }


        PyObject *zipfile_args = PyTuple_New(1);
        PyTuple_SetItem(zipfile_args, 0, source_code);


        self->zip = PyInstance_New(zipfile_zipfile, zipfile_args, NULL);
#endif
        if (!self->zip) {
                return -1;
        }

        Py_INCREF(self->zip);

        self->items = PyObject_CallMethod(self->zip, "namelist", NULL);
        if (!self->items) {
                return -1;
        }

        Py_INCREF(self->items);

        self->prefix = NULL;
        if (prefix) {
                self->prefix = prefix+1;
                prefix[0] = ':';
        }


        return 0;
}



static int
symzipimporter_init(struct _symzipimporter *self, PyObject *args, PyObject *kwds)
{


	char *name;
	char *prefix = NULL;

	if (!PyArg_ParseTuple(args, "s", &name))
        	return -1; 

	// avoid GC !!!
	name = uwsgi_concat2(name, "");

	prefix = strchr(name, ':');
	if (prefix) {
		prefix[0] = 0;
	}

	char *code_start = name_to_symbol(name, "start");
	if (!code_start) {
		PyErr_Format(PyExc_ValueError, "unable to find symbol");
		goto error;
	}

	char *code_end = name_to_symbol(name, "end");
	if (!code_end) {
		PyErr_Format(PyExc_ValueError, "unable to find symbol");
		goto error;
	}

	PyObject *stringio = PyImport_ImportModule("StringIO");
	if (!stringio) {
		goto error;
	}

#ifdef PYTHREE
	PyObject *source_code = PyObject_CallMethodObjArgs(stringio, PyString_FromString("StringIO"), PyString_FromStringAndSize(code_start, code_end-code_start));
#else
	PyObject *stringio_dict = PyModule_GetDict(stringio);
	if (!stringio_dict) {
		goto error;
	}

	PyObject *stringio_stringio = PyDict_GetItemString(stringio_dict, "StringIO");
	if (!stringio_stringio) {
		goto error;
	}

	PyObject *stringio_args = PyTuple_New(1);
	PyTuple_SetItem(stringio_args, 0, PyString_FromStringAndSize(code_start, code_end-code_start));


	PyObject *source_code = PyInstance_New(stringio_stringio, stringio_args, NULL);
#endif
	if (!source_code) {
		goto error;
	}

	PyObject *zipfile = PyImport_ImportModule("zipfile");
	if (!zipfile) {
		goto error;
	}
	
#ifdef PYTHREE
	self->zip = PyObject_CallMethodObjArgs(zipfile, PyString_FromString("ZipFile"), source_code);
#else
	PyObject *zipfile_dict = PyModule_GetDict(zipfile);
        if (!zipfile_dict) {
		goto error;
        }

        PyObject *zipfile_zipfile = PyDict_GetItemString(zipfile_dict, "ZipFile");
        if (!zipfile_zipfile) {
		goto error;
        }

        PyObject *zipfile_args = PyTuple_New(1);
        PyTuple_SetItem(zipfile_args, 0, source_code);


	self->zip = PyInstance_New(zipfile_zipfile, zipfile_args, NULL);
#endif
        if (!self->zip) {
		goto error;
        }

	Py_INCREF(self->zip);

	self->items = PyObject_CallMethod(self->zip, "namelist", NULL);
        if (!self->items) {
		goto error;
        }

	Py_INCREF(self->items);

	self->prefix = NULL;
	if (prefix) {
		self->prefix = prefix+1;
		prefix[0] = ':';
	}


	return 0;

error:
	free(name);
	return -1;
}

static PyTypeObject SymZipImporter_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "uwsgi.SymbolsZipImporter",
    sizeof(struct _symzipimporter),
    0,                                          /* tp_itemsize */
    (destructor) uwsgi_symimporter_free,            /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,
    "uwsgi symbols zip importer",                                          /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    symzipimporter_methods,                        /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc) symzipimporter_init,                 /* tp_init */
    PyType_GenericAlloc,                        /* tp_alloc */
    PyType_GenericNew,                          /* tp_new */
    0,                            /* tp_free */
};

static PyTypeObject ZipImporter_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "uwsgi.ZipImporter",
    sizeof(struct _symzipimporter),
    0,                                          /* tp_itemsize */
    (destructor) uwsgi_symimporter_free,            /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,
    "uwsgi zip importer",                                          /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    symzipimporter_methods,                        /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc) zipimporter_init,                 /* tp_init */
    PyType_GenericAlloc,                        /* tp_alloc */
    PyType_GenericNew,                          /* tp_new */
    0,                            /* tp_free */
};



int uwsgi_init_symbol_import() {


	if (PyType_Ready(&SymImporter_Type) < 0) {
		PyErr_Print();
		uwsgi_log("unable to initialize symbols importer module\n");
		exit(1);
	}

	if (PyType_Ready(&ZipImporter_Type) < 0) {
		PyErr_Print();
		uwsgi_log("unable to initialize zip importer module\n");
		exit(1);
	}

	if (PyType_Ready(&SymZipImporter_Type) < 0) {
		PyErr_Print();
		uwsgi_log("unable to initialize symbols zip importer module\n");
		exit(1);
	}

	PyObject *uwsgi_em = PyImport_ImportModule("uwsgi");
	if (!uwsgi_em) {
		PyErr_Print();
		uwsgi_log("unable to get uwsgi module\n");
		exit(1);
	}

	Py_INCREF((PyObject *)&SymImporter_Type);
	if (PyModule_AddObject(uwsgi_em, "SymbolsImporter",
                           (PyObject *)&SymImporter_Type) < 0) {
		PyErr_Print();
		uwsgi_log("unable to initialize symbols importer object\n");
		exit(1);
	}

	Py_INCREF((PyObject *)&ZipImporter_Type);
	if (PyModule_AddObject(uwsgi_em, "ZipImporter",
                           (PyObject *)&ZipImporter_Type) < 0) {
		PyErr_Print();
		uwsgi_log("unable to initialize zip importer object\n");
		exit(1);
	}

	Py_INCREF((PyObject *)&SymZipImporter_Type);
	if (PyModule_AddObject(uwsgi_em, "SymbolsZipImporter",
                           (PyObject *)&SymZipImporter_Type) < 0) {
		PyErr_Print();
		uwsgi_log("unable to initialize symbols zip importer object\n");
		exit(1);
	}


        return 0;
	
}
