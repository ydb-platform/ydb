/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/Core_EXPORTS.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/common/logging.h>

#include <atomic>

namespace Aws
{
    namespace Utils
    {
        namespace Logging
        {
            enum class LogLevel : int;

            /**
             * Interface for CRT (common runtime libraries) logging implementations.
             * A wrapper on the top of aws_logger, the logging interface used by common runtime libraries.
             */
            class AWS_CORE_API CRTLogSystemInterface
            {
            public:
                virtual ~CRTLogSystemInterface() = default;
            };

            /**
             * The default CRT log system will just do a redirection of all logs from common runtime libraries to C++ SDK.
             * You can override virtual function Log() in your subclass to change the default behaviors.
             */
            class AWS_CORE_API DefaultCRTLogSystem : public CRTLogSystemInterface
            {
            public:
                DefaultCRTLogSystem(LogLevel logLevel);
                virtual ~DefaultCRTLogSystem();

                /**
                 * Gets the currently configured log level.
                 */
                LogLevel GetLogLevel() const { return m_logLevel; }
                /**
                 * Set a new log level. This has the immediate effect of changing the log output to the new level.
                 */
                void SetLogLevel(LogLevel logLevel) { m_logLevel.store(logLevel); }

                /**
                 * Handle the logging information from common runtime libraries.
                 * Redirect them to C++ SDK logging system by default.
                 */
                virtual void Log(LogLevel logLevel, const char* subjectName, const char* formatStr, va_list args);

            protected:
                std::atomic<LogLevel> m_logLevel;
                /**
                 * Underlying logging interface used by common runtime libraries.
                 */
                aws_logger m_logger;
            };

        } // namespace Logging
    } // namespace Utils
} // namespace Aws
