/* py-directio file : directiomodule.c
   A Python module interface to 'open', 'read' and 'write' on a
   direct I/O context.

   This is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License 
   as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   This is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
   .- */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <string.h>
#include <sys/mman.h>

#if PY_MAJOR_VERSION >= 3
  #define PyString_FromStringAndSize PyBytes_FromStringAndSize
  #define PyInt_FromLong PyLong_FromLong
  #define MOD_ERROR_VAL NULL
  #define MOD_SUCCESS_VAL(val) val
  #define MOD_INIT(name) PyMODINIT_FUNC PyInit_##name(void)
  #define MOD_DEF(ob, name, doc, methods) \
          static struct PyModuleDef moduledef = { \
            PyModuleDef_HEAD_INIT, name, doc, -1, methods, }; \
          ob = PyModule_Create(&moduledef);
#else
  #define MOD_ERROR_VAL
  #define MOD_SUCCESS_VAL(val)
  #define MOD_INIT(name) void init##name(void)
  #define MOD_DEF(ob, name, doc, methods) \
          ob = Py_InitModule3(name, methods, doc);
#endif

static size_t alignment = 512;

static inline void * 
aligned_allocate (size_t size)
{
  return mmap(NULL, size, PROT_READ | PROT_WRITE, 
              MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
}

static inline void 
aligned_free (void *addr, size_t size)
{
  munmap(addr, size);
}

#define ALLOCATION_FAILED ((void *) -1)

static PyObject *
method_read (PyObject * self, PyObject * args)
{
  int fd;
  void *alignedbuff = NULL;
  PyObject *pyresult;
  long int count;
  ssize_t ret;

  if (!PyArg_ParseTuple (args, "il", &fd, &count))
    return NULL;
  
  if (count < 0 || count % alignment)
    {
      PyErr_SetString (PyExc_ValueError,
                       "read size must be a multiple of a 512");
      return NULL;
    }

  alignedbuff = aligned_allocate (count);
  
  if (ALLOCATION_FAILED == alignedbuff)
    {
      return PyErr_NoMemory ();
    }
    
  Py_BEGIN_ALLOW_THREADS;       
  ret = read (fd, alignedbuff, count);
  Py_END_ALLOW_THREADS;

  if (ret == -1)
    {
      PyErr_SetFromErrno (PyExc_OSError);
      aligned_free (alignedbuff, count);
      return NULL;
    }
  else
    {
      pyresult = PyString_FromStringAndSize (alignedbuff, ret);
      aligned_free (alignedbuff, count);
      return pyresult;
    }
}

static PyObject *
method_write (PyObject * self, PyObject * args)
{
  int fd;
  void *buff = NULL, *alignedbuff = NULL;
  Py_ssize_t count = 0;
  ssize_t ret = 0;

  if (!PyArg_ParseTuple (args, "is#", &fd, &buff, &count))
    return NULL;

  if (count <= 0 || count % alignment)
    {
      PyErr_SetString (PyExc_ValueError,
                       "write size must be a multiple of a 512.");
      return NULL;
    }

  alignedbuff = aligned_allocate (count);
  
  if (ALLOCATION_FAILED == alignedbuff)
    {
      return PyErr_NoMemory ();
    }

  memcpy (alignedbuff, buff, count);

  Py_BEGIN_ALLOW_THREADS;
  ret = write (fd, alignedbuff, count);
  Py_END_ALLOW_THREADS;

  aligned_free (alignedbuff, count);
      
  if (ret == -1)
    {
      PyErr_SetFromErrno (PyExc_OSError);
      return NULL;
    }
  return Py_BuildValue ("i", ret);
}

static PyMethodDef SpliceTeeMethods[] = {
  {"read", method_read, METH_VARARGS,
   "read(fd, count) = string\n"
   "\n"
   "read() attempts to read up to count bytes from file descriptor fd into a \
buffer.\n" "\n" "Upon success, 'read' returns a buffer containing the bytes read.\n"},
  {"write", method_write, METH_VARARGS,
   "write(fd,buf) = sent\n"
   "\n"
   " write() writes up to count bytes to the file referenced by the file \
descriptor fd from the buffer 'buf'.\n" "On a direct I/O context, this will result in data stored directly in hd, \
ignoring the buffer cache.\n"},
  {NULL, NULL, 0, NULL} /* Sentinel */
};

MOD_INIT(directio)
{
  PyObject *m;

  MOD_DEF (m,
           "directio",  
           "Direct interface 'read' and 'write' system calls on a direct I/O context.",
           SpliceTeeMethods
          );
  if (!m)
    return MOD_ERROR_VAL;

  PyModule_AddStringConstant (m, "__version__", "1.2");
  
  return MOD_SUCCESS_VAL(m);
}
