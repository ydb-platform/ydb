#include "fuzz_ops.h"
#include "vm_apply.h"
#include "vm_defs.h"
#include "vm_parse.h"

#include <library/cpp/bit_io/bitinput.h>

#include <library/cpp/scheme/scheme.h>
#include <library/cpp/scheme/scimpl_private.h>

#include <util/generic/maybe.h>

namespace NSc::NUt {

    void FuzzOps(TStringBuf wire, bool log) {
        if (log) {
            NImpl::GetTlsInstance<NImpl::TSelfLoopContext>().ReportingMode = NImpl::TSelfLoopContext::EMode::Stderr;
        }

        // We start with a single TValue node
        TVMState st {wire, 1, 0};

        while (auto act = ParseNextAction(st)) {
            if (log) {
                Cerr << " STATE: " << st.ToString() << Endl;
                Cerr << "ACTION: " << (act ? act->ToString() : TString("(empty)")) << Endl;
            }

            if (!ApplyNextAction(st, *act)) {
                break;
            }
            if (!NSc::TValue::DefaultValue().IsNull()) {
                std::terminate();
            }
        }
    }
}
