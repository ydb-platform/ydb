/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/SseKmsEncryptedObjectsStatus.h>
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
   * <p>A container for filter information for the selection of S3 objects encrypted
   * with AWS KMS.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/SseKmsEncryptedObjects">AWS
   * API Reference</a></p>
   */
  class AWS_S3_API SseKmsEncryptedObjects
  {
  public:
    SseKmsEncryptedObjects();
    SseKmsEncryptedObjects(const Aws::Utils::Xml::XmlNode& xmlNode);
    SseKmsEncryptedObjects& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Specifies whether Amazon S3 replicates objects created with server-side
     * encryption using a customer master key (CMK) stored in AWS Key Management
     * Service.</p>
     */
    inline const SseKmsEncryptedObjectsStatus& GetStatus() const{ return m_status; }

    /**
     * <p>Specifies whether Amazon S3 replicates objects created with server-side
     * encryption using a customer master key (CMK) stored in AWS Key Management
     * Service.</p>
     */
    inline bool StatusHasBeenSet() const { return m_statusHasBeenSet; }

    /**
     * <p>Specifies whether Amazon S3 replicates objects created with server-side
     * encryption using a customer master key (CMK) stored in AWS Key Management
     * Service.</p>
     */
    inline void SetStatus(const SseKmsEncryptedObjectsStatus& value) { m_statusHasBeenSet = true; m_status = value; }

    /**
     * <p>Specifies whether Amazon S3 replicates objects created with server-side
     * encryption using a customer master key (CMK) stored in AWS Key Management
     * Service.</p>
     */
    inline void SetStatus(SseKmsEncryptedObjectsStatus&& value) { m_statusHasBeenSet = true; m_status = std::move(value); }

    /**
     * <p>Specifies whether Amazon S3 replicates objects created with server-side
     * encryption using a customer master key (CMK) stored in AWS Key Management
     * Service.</p>
     */
    inline SseKmsEncryptedObjects& WithStatus(const SseKmsEncryptedObjectsStatus& value) { SetStatus(value); return *this;}

    /**
     * <p>Specifies whether Amazon S3 replicates objects created with server-side
     * encryption using a customer master key (CMK) stored in AWS Key Management
     * Service.</p>
     */
    inline SseKmsEncryptedObjects& WithStatus(SseKmsEncryptedObjectsStatus&& value) { SetStatus(std::move(value)); return *this;}

  private:

    SseKmsEncryptedObjectsStatus m_status;
    bool m_statusHasBeenSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
