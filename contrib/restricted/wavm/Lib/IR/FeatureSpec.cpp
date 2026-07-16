#include "WAVM/IR/FeatureSpec.h"
#include <array>
#include "WAVM/Inline/BasicTypes.h"

using namespace WAVM;
using namespace WAVM::IR;

void FeatureSpec::setFeatureLevel(FeatureLevel featureLevel)
{
#define SET_FEATURE(name, ...) name = true;
#define UNSET_FEATURE(name, ...) name = false;

	WAVM_ENUM_FEATURES(UNSET_FEATURE);

	switch(featureLevel)
	{
	case FeatureLevel::wavm: WAVM_ENUM_NONSTANDARD_FEATURES(SET_FEATURE); WAVM_FALLTHROUGH;
	case FeatureLevel::proposed: WAVM_ENUM_PROPOSED_FEATURES(SET_FEATURE); WAVM_FALLTHROUGH;
	case FeatureLevel::mature: WAVM_ENUM_MATURE_FEATURES(SET_FEATURE); WAVM_FALLTHROUGH;
	case FeatureLevel::standard: WAVM_ENUM_STANDARD_FEATURES(SET_FEATURE); WAVM_FALLTHROUGH;
	case FeatureLevel::mvp: mvp = true; break;
	default: WAVM_UNREACHABLE();
	}

#undef SET_FEATURE
#undef UNSET_FEATURE
}

const char* IR::getFeatureName(Feature feature)
{
	static constexpr const char* featureNames[] = {
#define VISIT_FEATURE(_, name, ...) name,
		WAVM_ENUM_FEATURES(VISIT_FEATURE)
#undef VISIT_FEATURE
	};
	static constexpr Uptr numFeatures = sizeof(featureNames) / sizeof(featureNames[0]);

	WAVM_ASSERT(Uptr(feature) < numFeatures);
	return featureNames[Uptr(feature)];
}
