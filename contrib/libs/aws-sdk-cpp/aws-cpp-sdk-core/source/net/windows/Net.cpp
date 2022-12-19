/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <WinSock2.h>
#include <cassert>
#include <aws/core/utils/logging/LogMacros.h>

namespace Aws
{
    namespace Net
    {
        static bool s_globalNetworkInitiated = false;

        bool IsNetworkInitiated() 
        {
            return s_globalNetworkInitiated;
        }

        void InitNetwork()
        {
            if (IsNetworkInitiated())
            {
                return;
            }
            // Initialize Winsock( requires winsock version 2.2)
            WSADATA wsaData;
            int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
            assert(result == NO_ERROR);
            if (result != NO_ERROR)
            {
                AWS_LOGSTREAM_ERROR("WinSock2", "Failed to Initate WinSock2.2");
                s_globalNetworkInitiated = false;
            }
            else
            {
                s_globalNetworkInitiated = true;
            }
        }

        void CleanupNetwork()
        {
            WSACleanup();
            s_globalNetworkInitiated = false;
        }
    }
}
