#include "codegen.h"

#include <util/generic/yexception.h>

namespace NYql {
namespace NCodegen {

ICodegen::TPtr ICodegen::Make(ETarget target, ESanitize sanitize) {
    Y_UNUSED(target);
    Y_UNUSED(sanitize);
    throw yexception() << "Codegen is not available";
}

ICodegen::TSharedPtr ICodegen::MakeShared(ETarget target, ESanitize sanitize) {
    Y_UNUSED(target);
    Y_UNUSED(sanitize);
    throw yexception() << "Codegen is not available";
}

bool ICodegen::IsCodegenAvailable() {
    return false;
}

}
}
