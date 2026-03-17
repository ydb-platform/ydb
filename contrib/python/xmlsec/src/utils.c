// Copyright (c) 2017 Ryan Leckey
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include "utils.h"

PyObject* PyXmlSec_GetFilePathOrContent(PyObject* file, int* is_content) {
    PyObject* data;
    PyObject* utf8;
    PyObject* tmp = NULL;

    if (PyObject_HasAttrString(file, "read")) {
        data = PyObject_CallMethod(file, "read", NULL);
        if (data != NULL && PyUnicode_Check(data)) {
            utf8 = PyUnicode_AsUTF8String(data);
            Py_DECREF(data);
            data = utf8;
        }
        *is_content = 1;
        return data;
    }
    *is_content = 0;
    if (!PyUnicode_FSConverter(file, &tmp)) {
        return NULL;
    }
    return tmp;
}

int PyXmlSec_SetStringAttr(PyObject* obj, const char* name, const char* value) {
    PyObject* tmp = PyUnicode_FromString(value);
    int r;

    if (tmp == NULL) {
        return -1;
    }
    r = PyObject_SetAttrString(obj, name, tmp);
    Py_DECREF(tmp);
    return r;
}

int PyXmlSec_SetLongAttr(PyObject* obj, const char* name, long value) {
    PyObject* tmp = PyLong_FromLong(value);
    int r;

    if (tmp == NULL) {
        return -1;
    }
    r = PyObject_SetAttrString(obj, name, tmp);
    Py_DECREF(tmp);
    return r;
}
