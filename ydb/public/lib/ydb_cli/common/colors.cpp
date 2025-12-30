#include "colors.h"
#include "config.h"

#include <util/generic/maybe.h>
#include <util/stream/output.h>
#include <util/system/env.h>

#include <optional>

namespace NYdb::NConsoleClient {

namespace {

// Global colors mode, initialized lazily from config or set explicitly
static std::optional<EGlobalColorsMode> GlobalColorsMode;

EGlobalColorsMode ParseColorsMode(const TString& value) {
    if (value == "never") {
        return EGlobalColorsMode::Never;
    } else if (value == "always") {
        return EGlobalColorsMode::Always;
    }
    return EGlobalColorsMode::Auto;
}

} // anonymous namespace

EGlobalColorsMode GetGlobalColorsMode() {
    if (!GlobalColorsMode) {
        // Read from global config on first access
        try {
            auto config = GetGlobalConfig();
            TString modeStr = (*config)["colors.enable"].AsString("auto");
            GlobalColorsMode = ParseColorsMode(modeStr);
        } catch (const std::exception& e) {
            Cerr << "Warning: Failed to read colors.enable from config: " << e.what() << Endl;
            GlobalColorsMode = EGlobalColorsMode::Auto;
        } catch (...) {
            Cerr << "Warning: Failed to read colors.enable from config" << Endl;
            GlobalColorsMode = EGlobalColorsMode::Auto;
        }
    }
    return *GlobalColorsMode;
}

void SetGlobalColorsMode(EGlobalColorsMode mode) {
    GlobalColorsMode = mode;
}

NColorizer::TColors& AutoColors(IOutputStream& out) {
    auto& result = NColorizer::AutoColors(out);

    switch (GetGlobalColorsMode()) {
        case EGlobalColorsMode::Never:
            // Always disable colors
            result.Disable();
            break;

        case EGlobalColorsMode::Always:
            // Enable colors, ignore NO_COLOR
            result.Enable();
            break;

        case EGlobalColorsMode::Auto:
        default:
            // Default behavior: respect NO_COLOR
            // no-color.org
            if (TryGetEnv("NO_COLOR").Defined()) {
                result.Disable();
            } else {
                // Re-enable colors (in case they were previously disabled)
                result.Enable();
            }
            break;
    }

    return result;
}

}