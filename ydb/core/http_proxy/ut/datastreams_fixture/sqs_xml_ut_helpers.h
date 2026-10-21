#pragma once

#include <library/cpp/json/json_value.h>

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

TString EncodeSqsXmlRequest(TStringBuf action, const NJson::TJsonMap& request);

NJson::TJsonMap ParseSqsXmlResponse(const TString& body);
