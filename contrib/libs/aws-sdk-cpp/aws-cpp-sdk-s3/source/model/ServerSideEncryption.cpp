/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/model/ServerSideEncryption.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/Globals.h>
#include <aws/core/utils/EnumParseOverflowContainer.h>

using namespace Aws::Utils;


namespace Aws
{
  namespace S3
  {
    namespace Model
    {
      namespace ServerSideEncryptionMapper
      {

        static const int AES256_HASH = HashingUtils::HashString("AES256");
        static const int aws_kms_HASH = HashingUtils::HashString("aws:kms");


        ServerSideEncryption GetServerSideEncryptionForName(const Aws::String& name)
        {
          int hashCode = HashingUtils::HashString(name.c_str());
          if (hashCode == AES256_HASH)
          {
            return ServerSideEncryption::AES256;
          }
          else if (hashCode == aws_kms_HASH)
          {
            return ServerSideEncryption::aws_kms;
          }
          EnumParseOverflowContainer* overflowContainer = Aws::GetEnumOverflowContainer();
          if(overflowContainer)
          {
            overflowContainer->StoreOverflow(hashCode, name);
            return static_cast<ServerSideEncryption>(hashCode);
          }

          return ServerSideEncryption::NOT_SET;
        }

        Aws::String GetNameForServerSideEncryption(ServerSideEncryption enumValue)
        {
          switch(enumValue)
          {
          case ServerSideEncryption::AES256:
            return "AES256";
          case ServerSideEncryption::aws_kms:
            return "aws:kms";
          default:
            EnumParseOverflowContainer* overflowContainer = Aws::GetEnumOverflowContainer();
            if(overflowContainer)
            {
              return overflowContainer->RetrieveOverflow(static_cast<int>(enumValue));
            }

            return {};
          }
        }

      } // namespace ServerSideEncryptionMapper
    } // namespace Model
  } // namespace S3
} // namespace Aws
