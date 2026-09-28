#pragma once

#include <yql/essentials/sql/settings/translator.h>

namespace NSQLTranslationV1 {

// Declaration of the default SQLv1 translator factory. The implementation is
// selected at link time: sql/v1 provides the real parser, while v1_dummy
// reports that the parser is unavailable.
NSQLTranslation::TTranslatorPtr MakeTranslator();

} // namespace NSQLTranslationV1
