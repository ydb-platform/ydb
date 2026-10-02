#pragma once

#include <string>

namespace NLogin::NSasl {

enum class ESaslPrepReturnCodes {
	Success,
	InvalidUTF8, // input is not a valid UTF-8 string
	Prohibited,	 // output would contain prohibited characters
};

ESaslPrepReturnCodes SaslPrep(const std::string& str, std::string& result);

} // namespace NLogin::NSasl
