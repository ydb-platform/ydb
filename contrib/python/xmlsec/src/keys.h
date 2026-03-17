// Copyright (c) 2017 Ryan Leckey
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#ifndef __PYXMLSEC_KEY_H__
#define __PYXMLSEC_KEY_H__

#include "platform.h"

#include <xmlsec/xmlsec.h>

typedef struct {
    PyObject_HEAD
    xmlSecKeyPtr handle;
    int is_own;
} PyXmlSec_Key;

extern PyTypeObject* PyXmlSec_KeyType;

PyXmlSec_Key* PyXmlSec_NewKey(void);

typedef struct {
    PyObject_HEAD
    xmlSecKeysMngrPtr handle;
} PyXmlSec_KeysManager;

extern PyTypeObject* PyXmlSec_KeysManagerType;

// converts object `o` to PyXmlSec_KeysManager, None will be converted to NULL, increments ref_count
int PyXmlSec_KeysManagerConvert(PyObject* o, PyXmlSec_KeysManager** p);

#endif //__PYXMLSEC_KEY_H__
