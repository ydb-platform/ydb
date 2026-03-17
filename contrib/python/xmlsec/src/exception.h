// Copyright (c) 2017 Ryan Leckey
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#ifndef __PYXMLSEC_EXCEPTIONS_H__
#define __PYXMLSEC_EXCEPTIONS_H__

#include "platform.h"

extern PyObject* PyXmlSec_Error;
extern PyObject* PyXmlSec_InternalError;
extern PyObject* PyXmlSec_VerificationError;

void PyXmlSec_SetLastError(const char* msg);

void PyXmlSec_SetLastError2(PyObject* type, const char* msg);

void PyXmlSec_ClearError(void);

void PyXmlSecEnableDebugTrace(int);

void PyXmlSec_InstallErrorCallback();

#endif //__PYXMLSEC_EXCEPTIONS_H__
