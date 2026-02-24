#pragma once

#include <yql/essentials/public/udf/udf_registrator.h>

namespace NYql::NUdf {

inline constexpr char STANDART_STREAM_PROXY_INJECTION_SCRIPT[] =
    R"(
# numpy on import may find installed openblas library and load it,
# which in turn causes it to start CPUCOUNT threads
# with approx. 40Mb memory reserved for each thread;
#
# See more detailed explanation here: https://st.yandex-team.ru/STATLIBS-1715#5bfc68ecbbc039001cec572a
#
# Thus, we reduce negative effects as much as possible
import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'


# Following part allows us later to format tracebacks via sys.excepthook
# in thread-safe manner
import sys
import threading
if sys.version_info >= (3, 0):
    from io import StringIO, TextIOWrapper as SysStderrType
else:
    from cStringIO import StringIO
    SysStderrType = file

class StderrLocal(threading.local):

    def __init__(self):
        self.is_real_mode = True
        self.buffer = StringIO()


class StderrProxy(object):
    def __init__(self, stderr):
        self._stderr = stderr
        self._tls = StderrLocal()

    def _toggle_real_mode(self):
        self._tls.is_real_mode = not self._tls.is_real_mode
        if not self._tls.is_real_mode:
            self._tls.buffer.clear()

    def _get_value(self):
        assert not self._tls.is_real_mode
        return self._tls.buffer.getvalue()

    def __getattr__(self, attr):
        target = self._stderr
        if not self._tls.is_real_mode:
            target = self._tls.buffer

        return getattr(target, attr)

if isinstance(sys.stderr, SysStderrType):
    sys.stderr = StderrProxy(sys.stderr)
)";

enum class EPythonFlavor {
    System,
    Arcadia,
};

void RegisterYqlPythonUdf(
    IRegistrator& registrator,
    ui32 flags,
    TStringBuf moduleName,
    TStringBuf resourceName,
    EPythonFlavor pythonFlavor);

TUniquePtr<IUdfModule> GetYqlPythonUdfModule(
    TStringBuf resourceName,
    EPythonFlavor pythonFlavor,
    bool standalone);

} // namespace NYql::NUdf
