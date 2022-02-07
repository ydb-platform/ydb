#pragma once

//    Do nothing if there is no support for IPv4
#define ASSUME_IP_V4_ENABLED                          \
    do {                                              \
        try {                                         \
            TNetworkAddress("192.168.0.42", 80);      \
        } catch (const TNetworkResolutionError& ex) { \
            Y_UNUSED(ex);                             \
            return;                                   \
        }                                             \
    } while (0)
