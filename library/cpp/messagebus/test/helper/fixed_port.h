#pragma once

namespace NBus {
    namespace NTest {
        bool IsFixedPortTestAllowed();

        // Must not be in range OS uses for bind on random port.
        const unsigned FixedPort = 4927;

    }
}
