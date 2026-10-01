#include "format.h"

#include <util/generic/yexception.h>

namespace NSQLPureAST {

bool IsQuoted(TStringBuf content) {
    return 2 <= content.size() && content.front() == '`' && content.back() == '`';
}

TString Quoted(TString content) {
    content.prepend('`');
    content.append('`');
    return content;
}

TStringBuf Unquoted(TStringBuf content) {
    Y_ENSURE(IsQuoted(content));
    return content.SubStr(1, content.size() - 2);
}

} // namespace NSQLPureAST
