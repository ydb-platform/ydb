#pragma once

#include <ydb/library/yql/core/url_preprocessing/interface/url_preprocessing.h>
#include <ydb/library/yql/core/url_preprocessing/pattern_group.h>
#include <ydb/library/yql/core/url_preprocessing/url_mapper.h>

#include <ydb/library/yql/core/yql_user_data.h>

#include <util/generic/string.h>
#include <util/generic/ptr.h>

#include <vector>
#include <utility>

namespace NYql {

class TGatewaysConfig;

class TUrlPreprocessing: public IUrlPreprocessing {
public:
    using TPtr = TIntrusivePtr<TUrlPreprocessing>;

    TUrlPreprocessing(const TGatewaysConfig& cfg) {
        Configure(false, cfg);
    }
    TUrlPreprocessing() = default;
    ~TUrlPreprocessing() = default;

    void Configure(bool restrictedUser, const TGatewaysConfig& cfg);
    std::pair<TString, TString> Preprocess(const TString& url);

private:
    bool RestrictedUser_ = false;
    TUrlMapper Mapper_;
    TPatternGroup AllowedUrls_;
};

}
