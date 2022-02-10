#include "client_status.h"

template <>
void Out<NTvmAuth::TClientStatus>(IOutputStream& out, const NTvmAuth::TClientStatus& s) {
    out << s.GetCode() << ": " << s.GetLastError();
}
