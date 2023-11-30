#pragma once
#include <ydb/library/actors/core/actor.h>
#include "http.h"

namespace NHttp {

NActors::IActor* CreateHttpStaticContentHandler(const TString& url, const TString& filePath, const TString& resourcePath, const TString& index = TString());

}
