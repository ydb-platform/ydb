/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace Utils
{
namespace Xml
{
  class XmlNode;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{

  /**
   * <p>Specifies the use of SSE-KMS to encrypt delivered inventory
   * reports.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/SSEKMS">AWS API
   * Reference</a></p>
   */
  class AWS_S3_API SSEKMS
  {
  public:
    SSEKMS();
    SSEKMS(const Aws::Utils::Xml::XmlNode& xmlNode);
    SSEKMS& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Specifies the ID of the AWS Key Management Service (AWS KMS) symmetric
     * customer managed customer master key (CMK) to use for encrypting inventory
     * reports.</p>
     */
    inline const Aws::String& GetKeyId() const{ return m_keyId; }

    /**
     * <p>Specifies the ID of the AWS Key Management Service (AWS KMS) symmetric
     * customer managed customer master key (CMK) to use for encrypting inventory
     * reports.</p>
     */
    inline bool KeyIdHasBeenSet() const { return m_keyIdHasBeenSet; }

    /**
     * <p>Specifies the ID of the AWS Key Management Service (AWS KMS) symmetric
     * customer managed customer master key (CMK) to use for encrypting inventory
     * reports.</p>
     */
    inline void SetKeyId(const Aws::String& value) { m_keyIdHasBeenSet = true; m_keyId = value; }

    /**
     * <p>Specifies the ID of the AWS Key Management Service (AWS KMS) symmetric
     * customer managed customer master key (CMK) to use for encrypting inventory
     * reports.</p>
     */
    inline void SetKeyId(Aws::String&& value) { m_keyIdHasBeenSet = true; m_keyId = std::move(value); }

    /**
     * <p>Specifies the ID of the AWS Key Management Service (AWS KMS) symmetric
     * customer managed customer master key (CMK) to use for encrypting inventory
     * reports.</p>
     */
    inline void SetKeyId(const char* value) { m_keyIdHasBeenSet = true; m_keyId.assign(value); }

    /**
     * <p>Specifies the ID of the AWS Key Management Service (AWS KMS) symmetric
     * customer managed customer master key (CMK) to use for encrypting inventory
     * reports.</p>
     */
    inline SSEKMS& WithKeyId(const Aws::String& value) { SetKeyId(value); return *this;}

    /**
     * <p>Specifies the ID of the AWS Key Management Service (AWS KMS) symmetric
     * customer managed customer master key (CMK) to use for encrypting inventory
     * reports.</p>
     */
    inline SSEKMS& WithKeyId(Aws::String&& value) { SetKeyId(std::move(value)); return *this;}

    /**
     * <p>Specifies the ID of the AWS Key Management Service (AWS KMS) symmetric
     * customer managed customer master key (CMK) to use for encrypting inventory
     * reports.</p>
     */
    inline SSEKMS& WithKeyId(const char* value) { SetKeyId(value); return *this;}

  private:

    Aws::String m_keyId;
    bool m_keyIdHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
