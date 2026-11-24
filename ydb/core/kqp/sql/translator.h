#pragma once

#include <yql/essentials/sql/settings/translator.h>

namespace NKikimr::NKqp {

NSQLTranslation::TTranslatorPtr MakeKqpSqlV1Translator(bool isAmbiguityError);

} // namespace NKikimr::NKqp
