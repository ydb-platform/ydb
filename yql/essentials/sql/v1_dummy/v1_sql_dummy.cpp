#include <yql/essentials/sql/v1/translator_iface/translator.h>

namespace NSQLTranslationV1 {

NSQLTranslation::TTranslatorPtr MakeTranslator() {
    return NSQLTranslation::MakeDummyTranslator("v1");
}

} // namespace NSQLTranslationV1
