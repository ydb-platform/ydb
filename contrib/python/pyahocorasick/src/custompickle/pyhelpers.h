#pragma once

typedef struct SaveLoadParameters {
    PyObject* path;
    PyObject* callback;
} SaveLoadParameters;

bool 
automaton_save_load_parse_args(KeysStore store, PyObject* args, SaveLoadParameters* result);

