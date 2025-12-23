/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/sqs/model/MessageSystemAttributeNameForSends.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/Globals.h>
#include <aws/core/utils/EnumParseOverflowContainer.h>

using namespace Aws::Utils;


namespace Aws
{
  namespace SQS
  {
    namespace Model
    {
      namespace MessageSystemAttributeNameForSendsMapper
      {

        static const int AWSTraceHeader_HASH = HashingUtils::HashString("AWSTraceHeader");


        MessageSystemAttributeNameForSends GetMessageSystemAttributeNameForSendsForName(const Aws::String& name)
        {
          int hashCode = HashingUtils::HashString(name.c_str());
          if (hashCode == AWSTraceHeader_HASH)
          {
            return MessageSystemAttributeNameForSends::AWSTraceHeader;
          }
          EnumParseOverflowContainer* overflowContainer = Aws::GetEnumOverflowContainer();
          if(overflowContainer)
          {
            overflowContainer->StoreOverflow(hashCode, name);
            return static_cast<MessageSystemAttributeNameForSends>(hashCode);
          }

          return MessageSystemAttributeNameForSends::NOT_SET;
        }

        Aws::String GetNameForMessageSystemAttributeNameForSends(MessageSystemAttributeNameForSends enumValue)
        {
          switch(enumValue)
          {
          case MessageSystemAttributeNameForSends::AWSTraceHeader:
            return "AWSTraceHeader";
          default:
            EnumParseOverflowContainer* overflowContainer = Aws::GetEnumOverflowContainer();
            if(overflowContainer)
            {
              return overflowContainer->RetrieveOverflow(static_cast<int>(enumValue));
            }

            return {};
          }
        }

      } // namespace MessageSystemAttributeNameForSendsMapper
    } // namespace Model
  } // namespace SQS
} // namespace Aws
