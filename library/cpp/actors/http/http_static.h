#pragma once
#include <library/cpp/actors/core/actor.h>
#include "http.h"

namespace NHttp {

NActors::IActor* CreateHttpStaticContentHandler(const TString& url, const TString& filePath, const TString& resourcePath, const TString& index = TString());

}
