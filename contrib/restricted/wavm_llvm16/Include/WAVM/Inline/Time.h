#pragma once

#include "WAVM/Inline/I128.h"

namespace WAVM {
	struct Time
	{
		I128 ns;

		static constexpr Time infinity() { return Time{I128::nan()}; }
		friend bool isInfinity(Time time) { return isNaN(time.ns); }
	};
}
