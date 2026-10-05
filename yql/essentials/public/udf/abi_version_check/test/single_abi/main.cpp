#include <yql/essentials/public/udf/abi_version_check/test/stable_probe/abi_probe_stable.h>

int main() {
    // Calling the probe is what pulls its object, and with it the ABI record, into the binary.
    return StableProbeAbiVersion() == 0 ? 1 : 0;
}
