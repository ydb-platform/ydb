/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/utils/logging/CRTLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/Array.h>
#include <aws/common/common.h>
#include <cstdarg>

using namespace Aws::Utils;
using namespace Aws::Utils::Logging;

namespace Aws
{
    namespace Utils
    {
        namespace Logging
        {
            static int s_aws_logger_redirect_log(
                struct aws_logger *logger,
                enum aws_log_level log_level,
                aws_log_subject_t subject,
                const char *format, ...)
            {
                DefaultCRTLogSystem* crtLogSystem = reinterpret_cast<DefaultCRTLogSystem*>(logger->p_impl);
                Logging::LogLevel logLevel = static_cast<LogLevel>(log_level);
                const char* subjectName = aws_log_subject_name(subject);
                va_list args;
                va_start(args, format);
                crtLogSystem->Log(logLevel, subjectName, format, args);
                va_end(args);
                return AWS_OP_SUCCESS;
            }

            static enum aws_log_level s_aws_logger_redirect_get_log_level(struct aws_logger *logger, aws_log_subject_t subject) {
                (void)subject;
                DefaultCRTLogSystem* crtLogSystem = reinterpret_cast<DefaultCRTLogSystem*>(logger->p_impl);
                return (aws_log_level)(crtLogSystem->GetLogLevel());
            }

            static void s_aws_logger_redirect_clean_up(struct aws_logger *logger) {
                (void)logger;
            }

            static int s_aws_logger_redirect_set_log_level(struct aws_logger *logger, enum aws_log_level log_level)
            {
                DefaultCRTLogSystem* crtLogSystem = reinterpret_cast<DefaultCRTLogSystem*>(logger->p_impl);
                crtLogSystem->SetLogLevel(static_cast<LogLevel>(log_level));
                return AWS_OP_SUCCESS;
            }

            static struct aws_logger_vtable s_aws_logger_redirect_vtable = {
                s_aws_logger_redirect_log, // .log
                s_aws_logger_redirect_get_log_level, // .get_log_level
                s_aws_logger_redirect_clean_up, // .clean_up
                s_aws_logger_redirect_set_log_level // set_log_level
            };

            DefaultCRTLogSystem::DefaultCRTLogSystem(LogLevel logLevel) :
                m_logLevel(logLevel),
                m_logger()
            {
                m_logger.vtable = &s_aws_logger_redirect_vtable;
                m_logger.allocator = Aws::get_aws_allocator();
                m_logger.p_impl = this;

                aws_logger_set(&m_logger);
            }

            DefaultCRTLogSystem::~DefaultCRTLogSystem()
            {
                if (aws_logger_get() == &m_logger)
                {
                    aws_logger_set(NULL);
                    aws_logger_clean_up(&m_logger);
                }
            }

            void DefaultCRTLogSystem::Log(LogLevel logLevel, const char* subjectName, const char* formatStr, va_list args)
            {
                va_list tmp_args;
                va_copy(tmp_args, args);
                #ifdef _WIN32
                    const int requiredLength = _vscprintf(formatStr, tmp_args) + 1;
                #else
                    const int requiredLength = vsnprintf(nullptr, 0, formatStr, tmp_args) + 1;
                #endif
                va_end(tmp_args);

                Array<char> outputBuff(requiredLength);
                #ifdef _WIN32
                    vsnprintf_s(outputBuff.GetUnderlyingData(), requiredLength, _TRUNCATE, formatStr, args);
                #else
                    vsnprintf(outputBuff.GetUnderlyingData(), requiredLength, formatStr, args);
                #endif // _WIN32

                Aws::OStringStream logStream;
                logStream << outputBuff.GetUnderlyingData();
                Logging::GetLogSystem()->LogStream(logLevel, subjectName, logStream);
            }
        }
    }
}

