#include <yql/essentials/public/udf/abi_version_check/test/current_probe/abi_probe_current.h>
#include <yql/essentials/public/udf/abi_version_check/test/stable_probe/abi_probe_stable.h>

int main() {
    // Never reached: the two probes carry two ABIs, and the check runs before main.
    return StableProbeAbiVersion() == CurrentProbeAbiVersion() ? 1 : 0;
}
