// Copyright (c) 2017 Ryan Leckey
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#ifndef __PYXMLSEC_UTILS_H__
#define __PYXMLSEC_UTILS_H__

#include "platform.h"

int PyXmlSec_SetStringAttr(PyObject* obj, const char* name, const char* value);

int PyXmlSec_SetLongAttr(PyObject* obj, const char* name, long value);

// return content if file is fileobject, or fs encoded filepath
PyObject* PyXmlSec_GetFilePathOrContent(PyObject* file, int* is_content);

#endif //__PYXMLSEC_UTILS_H__
