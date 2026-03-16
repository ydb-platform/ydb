#ifndef TRAINER_WRAPPER_H
#define TRAINER_WRAPPER_H 1

#include <string>
#include "crfsuite_api.hpp"

struct _object;
typedef _object PyObject;

namespace CRFSuiteWrapper
{

typedef PyObject* (*messagefunc)(PyObject *self, std::string message);

/**
* A wrapper around CRFSuite::Trainer that allows overriding
* 'message' method from Python.
*/
class Trainer : public CRFSuite::Trainer
{
protected:
    PyObject *m_obj;
    messagefunc handler;

public:
    void set_handler(PyObject *obj, messagefunc handler);
    virtual void message(const std::string& msg);
    void _init_hack();
};

}
#endif
