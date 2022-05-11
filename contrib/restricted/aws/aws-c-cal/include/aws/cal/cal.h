#ifndef AWS_CAL_CAL_H
#define AWS_CAL_CAL_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/common.h>
#include <aws/common/logging.h>

#include <aws/cal/exports.h>

struct aws_allocator;

#define AWS_C_CAL_PACKAGE_ID 7

enum aws_cal_errors {
    AWS_ERROR_CAL_SIGNATURE_VALIDATION_FAILED = AWS_ERROR_ENUM_BEGIN_RANGE(AWS_C_CAL_PACKAGE_ID),
    AWS_ERROR_CAL_MISSING_REQUIRED_KEY_COMPONENT,
    AWS_ERROR_CAL_INVALID_KEY_LENGTH_FOR_ALGORITHM,
    AWS_ERROR_CAL_UNKNOWN_OBJECT_IDENTIFIER,
    AWS_ERROR_CAL_MALFORMED_ASN1_ENCOUNTERED,
    AWS_ERROR_CAL_MISMATCHED_DER_TYPE,
    AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM,

    AWS_ERROR_CAL_END_RANGE = AWS_ERROR_ENUM_END_RANGE(AWS_C_CAL_PACKAGE_ID)
};

enum aws_cal_log_subject {
    AWS_LS_CAL_GENERAL = AWS_LOG_SUBJECT_BEGIN_RANGE(AWS_C_CAL_PACKAGE_ID),
    AWS_LS_CAL_ECC,
    AWS_LS_CAL_HASH,
    AWS_LS_CAL_HMAC,
    AWS_LS_CAL_DER,
    AWS_LS_CAL_LIBCRYPTO_RESOLVE,

    AWS_LS_CAL_LAST = AWS_LOG_SUBJECT_END_RANGE(AWS_C_CAL_PACKAGE_ID)
};

AWS_EXTERN_C_BEGIN

AWS_CAL_API void aws_cal_library_init(struct aws_allocator *allocator);
AWS_CAL_API void aws_cal_library_clean_up(void);

AWS_EXTERN_C_END

#endif /* AWS_CAL_CAL_H */
