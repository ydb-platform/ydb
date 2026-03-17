#include <iostream>
#include "Python.h"
#include "trainer_wrapper.hpp"
#include <stdexcept>

namespace CRFSuiteWrapper
{


void Trainer::set_handler(PyObject *obj, messagefunc handler)
{
    // We are not holding a reference to obj (no PY_INCREF) here
    // because doing so prevents __del__ from being called
    this->m_obj = obj;
    this->handler = handler;
}


void Trainer::message(const std::string& msg)
{
    if (this->m_obj == NULL) {
        std::cerr << "** Trainer invalid state: obj [" << this->m_obj << "]\n";
        return;
    }
    PyObject* result = handler(this->m_obj, msg);
    if (result == NULL){
        // Python exception is raised in the handler.
        // Raise a C++ exception to stop training.
        // Cython will catch it and re-raise the previous Python exception
        // (which is the one raised in a handler).
        throw std::runtime_error("You shouldn't have seen this message!");
    }
}

void Trainer::_init_hack()
{
    Trainer::init();
}


}
