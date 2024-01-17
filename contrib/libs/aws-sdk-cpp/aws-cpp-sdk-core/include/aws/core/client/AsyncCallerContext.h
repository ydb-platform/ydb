/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/utils/memory/stl/AWSString.h>

namespace Aws
{
    namespace Client
    {
        /**
        * Call-back context for all async client methods. This allows you to pass a context to your callbacks so that you can identify your requests.
        * It is entirely intended that you override this class in-lieu of using a void* for the user context. The base class just gives you the ability to
        * pass a uuid for your context.
        */
        class AWS_CORE_API AsyncCallerContext
        {
        public:
            /**
             * Initializes object with generated UUID
             */
            AsyncCallerContext();

            /**
             * Initializes object with UUID
             */
            AsyncCallerContext(const Aws::String& uuid) : m_uuid(uuid) {}

            /**
            * Initializes object with UUID
            */
            AsyncCallerContext(const char* uuid) : m_uuid(uuid) {}

            virtual ~AsyncCallerContext() {}

            /**
             * Gets underlying UUID
             */
            inline const Aws::String& GetUUID() const { return m_uuid; }

            /**
             * Sets underlying UUID
             */
            inline void SetUUID(const Aws::String& value) { m_uuid = value; }

            /**
             * Sets underlying UUID
             */
            inline void SetUUID(const char* value) { m_uuid.assign(value); }

        private:
            Aws::String m_uuid;
        };
    }
}
