#pragma once

#include "defs.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <contrib/libs/snowball/include/libstemmer.h>

namespace NKikimr::NFulltext {

struct TStemmerDeleter {
    void operator()(struct sb_stemmer* stemmer) {
        sb_stemmer_delete(stemmer);
    }
};
using TStemmerPtr = std::unique_ptr<struct sb_stemmer, TStemmerDeleter>;

TVector<TString> Analyze(const TString& text, const Ydb::Table::FulltextIndexSettings::Analyzers& settings, struct sb_stemmer* stemmer = nullptr);

bool ValidateColumnsMatches(const NProtoBuf::RepeatedPtrField<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error);
bool ValidateColumnsMatches(const TVector<TString>& columns, const Ydb::Table::FulltextIndexSettings& settings, TString& error);

bool ValidateSettings(const Ydb::Table::FulltextIndexSettings& settings, TString& error);
bool FillSetting(Ydb::Table::FulltextIndexSettings& settings, const TString& name, const TString& value, TString& error);

}
