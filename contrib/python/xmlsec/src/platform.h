// Copyright (c) 2017 Ryan Leckey
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#ifndef __PYXMLSEC_PLATFORM_H__
#define __PYXMLSEC_PLATFORM_H__

#define PY_SSIZE_T_CLEAN 1

#include <xmlsec/version.h>
#include <Python.h>

#ifdef MS_WIN32
#include <windows.h>
#endif /* MS_WIN32 */

#define XMLSEC_VERSION_HEX ((XMLSEC_VERSION_MAJOR << 16) | (XMLSEC_VERSION_MINOR << 8) | (XMLSEC_VERSION_SUBMINOR))

// XKMS support was removed in version 1.2.21
// https://mail.gnome.org/archives/commits-list/2015-February/msg10555.html
#if  XMLSEC_VERSION_HEX > 0x10214
#define XMLSEC_NO_XKMS 1
#endif

#define XSTR(c) (const xmlChar*)(c)

#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif

static inline char* PyBytes_AsStringAndSize2(PyObject *obj, Py_ssize_t* length) {
    char* buffer = NULL;
    return ((PyBytes_AsStringAndSize(obj, &buffer, length) < 0) ? (char*)(0) : buffer);
}

#endif //__PYXMLSEC_PLATFORM_H__
