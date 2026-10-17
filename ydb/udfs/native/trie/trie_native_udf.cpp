#include <ydb/udfs/wasm/trie/binary_trie.h>

#include <yql/essentials/public/udf/udf_helpers.h>

#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/system/unaligned_mem.h>

using namespace NYql::NUdf;
using namespace NBinaryTrie;

namespace {

ui32 ExtractValidSize(TStringBuf dict, ui64 offset) {
    if (offset > dict.size() - sizeof(ui32)) {
        throw yexception() << "Corrupt trie: out of range (size)";
    }
    auto size = ReadUnaligned<ui32>(dict.data() + offset);
    if (size > dict.size() - offset - sizeof(ui32)) {
        throw yexception() << "Corrupt trie: out of range (content)";
    }
    return size;
}

SIMPLE_STRICT_UDF(TLookup, i64(TAutoMap<char*>, TAutoMap<char*>)) {
    Y_UNUSED(valueBuilder);
    const TStringBuf haystack(args[0].AsStringRef());
    const TStringBuf dict(args[1].AsStringRef());
    return TUnboxedValuePod(LookupTrie(haystack, dict));
}

SIMPLE_STRICT_UDF(TLookupWithString, TOptional<char*>(TAutoMap<char*>, TAutoMap<char*>)) {
    const TStringBuf haystack(args[0].AsStringRef());
    const TStringBuf dict(args[1].AsStringRef());
    const i64 offset = LookupTrie(haystack, dict);
    if (offset < 0) {
        return TUnboxedValue();
    }
    const ui32 size = ExtractValidSize(dict, static_cast<ui64>(offset));
    return valueBuilder->NewString(TStringRef(dict.data() + offset + sizeof(ui32), size));
}

SIMPLE_MODULE(TTrieNativeModule, TLookup, TLookupWithString)

} // namespace

REGISTER_MODULES(TTrieNativeModule)
