#pragma once

#include <library/cpp/tvmauth/exception.h>
#include <library/cpp/tvmauth/client/exception.h>

#include <exception>
#include <ios>
#include <new>
#include <stdexcept>
#include <typeinfo>

#include <Python.h>

extern "C" DL_EXPORT(PyObject*) TA_pyEmptyTvmKeysException;
extern "C" DL_EXPORT(PyObject*) TA_pyMalformedTvmKeysException;
extern "C" DL_EXPORT(PyObject*) TA_pyMalformedTvmSecretException;
extern "C" DL_EXPORT(PyObject*) TA_pyNotAllowedException;
extern "C" DL_EXPORT(PyObject*) TA_pyClientException;
extern "C" DL_EXPORT(PyObject*) TA_pyBrokenTvmClientSettings;
extern "C" DL_EXPORT(PyObject*) TA_pyRetriableException;
extern "C" DL_EXPORT(PyObject*) TA_pyNonRetriableException;
extern "C" DL_EXPORT(PyObject*) TA_pyPermissionDenied;
extern "C" DL_EXPORT(PyObject*) TA_pyMissingServiceTicket;

static void TA_raise_py_error() {
    // Catch a handful of different errors here and turn them into the
    // equivalent Python errors.
    try {
        if (PyErr_Occurred())
            ; // let the latest Python exn pass through and ignore the current one
        else
            throw;
    } catch (const NTvmAuth::TEmptyTvmKeysException& ex) {
        PyErr_SetString(TA_pyEmptyTvmKeysException, ex.what());
    } catch (const NTvmAuth::TMalformedTvmKeysException& ex) {
        PyErr_SetString(TA_pyMalformedTvmKeysException, ex.what());
    } catch (const NTvmAuth::TMalformedTvmSecretException& ex) {
        PyErr_SetString(TA_pyMalformedTvmSecretException, ex.what());
    } catch (const NTvmAuth::TNotAllowedException& ex) {
        PyErr_SetString(TA_pyNotAllowedException, ex.what());
    } catch (const NTvmAuth::TBrokenTvmClientSettings& ex) {
        PyErr_SetString(TA_pyBrokenTvmClientSettings, ex.what());
    } catch (const NTvmAuth::TPermissionDenied& ex) {
        PyErr_SetString(TA_pyPermissionDenied, ex.what());
    } catch (const NTvmAuth::TMissingServiceTicket& ex) {
        PyErr_SetString(TA_pyMissingServiceTicket, ex.what());
    } catch (const NTvmAuth::TNonRetriableException& ex) {
        PyErr_SetString(TA_pyNonRetriableException, ex.what());
    } catch (const NTvmAuth::TRetriableException& ex) {
        PyErr_SetString(TA_pyRetriableException, ex.what());
    } catch (const NTvmAuth::TClientException& ex) {
        PyErr_SetString(TA_pyClientException, ex.what());
    } catch (const std::bad_alloc& ex) {
        PyErr_SetString(PyExc_MemoryError, ex.what());
    } catch (const std::bad_cast& ex) {
        PyErr_SetString(PyExc_TypeError, ex.what());
    } catch (const std::domain_error& ex) {
        PyErr_SetString(PyExc_ValueError, ex.what());
    } catch (const std::invalid_argument& ex) {
        PyErr_SetString(PyExc_ValueError, ex.what());
    } catch (const std::ios_base::failure& ex) {
        // Unfortunately, in standard C++ we have no way of distinguishing EOF
        // from other errors here; be careful with the exception mask
        PyErr_SetString(PyExc_IOError, ex.what());
    } catch (const std::out_of_range& ex) {
        // Change out_of_range to IndexError
        PyErr_SetString(PyExc_IndexError, ex.what());
    } catch (const std::overflow_error& ex) {
        PyErr_SetString(PyExc_OverflowError, ex.what());
    } catch (const std::range_error& ex) {
        PyErr_SetString(PyExc_ArithmeticError, ex.what());
    } catch (const std::underflow_error& ex) {
        PyErr_SetString(PyExc_ArithmeticError, ex.what());
    } catch (const std::exception& ex) {
        PyErr_SetString(PyExc_RuntimeError, ex.what());
    } catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "Unknown exception");
    }
}
