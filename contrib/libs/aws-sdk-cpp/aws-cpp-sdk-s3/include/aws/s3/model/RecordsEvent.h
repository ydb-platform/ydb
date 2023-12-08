/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/Array.h>
#include <utility>

namespace Aws
{
namespace S3
{
namespace Model
{
  /**
   * <p>The container for the records event.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/RecordsEvent">AWS API
   * Reference</a></p>
   */
  class RecordsEvent
  {
  public:
    AWS_S3_API RecordsEvent() = default;
    AWS_S3_API RecordsEvent(Aws::Vector<unsigned char>&& value) { m_payload = std::move(value); }

    /**
     * <p>The byte array of partial, one or more result records.</p>
     */
    inline const Aws::Vector<unsigned char>& GetPayload() const { return m_payload; }

    /**
     * <p>The byte array of partial, one or more result records.</p>
     */
    inline Aws::Vector<unsigned char>&& GetPayloadWithOwnership() { return std::move(m_payload); }

    /**
     * <p>The byte array of partial, one or more result records.</p>
     */
    inline void SetPayload(const Aws::Vector<unsigned char>& value) { m_payloadHasBeenSet = true; m_payload = value; }

    /**
     * <p>The byte array of partial, one or more result records.</p>
     */
    inline void SetPayload(Aws::Vector<unsigned char>&& value) { m_payloadHasBeenSet = true; m_payload = std::move(value); }

    /**
     * <p>The byte array of partial, one or more result records.</p>
     */
    inline RecordsEvent& WithPayload(const Aws::Vector<unsigned char>& value) { SetPayload(value); return *this;}

    /**
     * <p>The byte array of partial, one or more result records.</p>
     */
    inline RecordsEvent& WithPayload(Aws::Vector<unsigned char>&& value) { SetPayload(std::move(value)); return *this;}

  private:

    Aws::Vector<unsigned char> m_payload;
    bool m_payloadHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
