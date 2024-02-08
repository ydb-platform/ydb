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
   * <p>Container element that identifies who initiated the multipart upload.
   * </p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/Initiator">AWS API
   * Reference</a></p>
   */
  class Initiator
  {
  public:
    AWS_S3_API Initiator();
    AWS_S3_API Initiator(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API Initiator& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>If the principal is an Amazon Web Services account, it provides the Canonical
     * User ID. If the principal is an IAM User, it provides a user ARN value.</p>
     */
    inline const Aws::String& GetID() const{ return m_iD; }

    /**
     * <p>If the principal is an Amazon Web Services account, it provides the Canonical
     * User ID. If the principal is an IAM User, it provides a user ARN value.</p>
     */
    inline bool IDHasBeenSet() const { return m_iDHasBeenSet; }

    /**
     * <p>If the principal is an Amazon Web Services account, it provides the Canonical
     * User ID. If the principal is an IAM User, it provides a user ARN value.</p>
     */
    inline void SetID(const Aws::String& value) { m_iDHasBeenSet = true; m_iD = value; }

    /**
     * <p>If the principal is an Amazon Web Services account, it provides the Canonical
     * User ID. If the principal is an IAM User, it provides a user ARN value.</p>
     */
    inline void SetID(Aws::String&& value) { m_iDHasBeenSet = true; m_iD = std::move(value); }

    /**
     * <p>If the principal is an Amazon Web Services account, it provides the Canonical
     * User ID. If the principal is an IAM User, it provides a user ARN value.</p>
     */
    inline void SetID(const char* value) { m_iDHasBeenSet = true; m_iD.assign(value); }

    /**
     * <p>If the principal is an Amazon Web Services account, it provides the Canonical
     * User ID. If the principal is an IAM User, it provides a user ARN value.</p>
     */
    inline Initiator& WithID(const Aws::String& value) { SetID(value); return *this;}

    /**
     * <p>If the principal is an Amazon Web Services account, it provides the Canonical
     * User ID. If the principal is an IAM User, it provides a user ARN value.</p>
     */
    inline Initiator& WithID(Aws::String&& value) { SetID(std::move(value)); return *this;}

    /**
     * <p>If the principal is an Amazon Web Services account, it provides the Canonical
     * User ID. If the principal is an IAM User, it provides a user ARN value.</p>
     */
    inline Initiator& WithID(const char* value) { SetID(value); return *this;}


    /**
     * <p>Name of the Principal.</p>
     */
    inline const Aws::String& GetDisplayName() const{ return m_displayName; }

    /**
     * <p>Name of the Principal.</p>
     */
    inline bool DisplayNameHasBeenSet() const { return m_displayNameHasBeenSet; }

    /**
     * <p>Name of the Principal.</p>
     */
    inline void SetDisplayName(const Aws::String& value) { m_displayNameHasBeenSet = true; m_displayName = value; }

    /**
     * <p>Name of the Principal.</p>
     */
    inline void SetDisplayName(Aws::String&& value) { m_displayNameHasBeenSet = true; m_displayName = std::move(value); }

    /**
     * <p>Name of the Principal.</p>
     */
    inline void SetDisplayName(const char* value) { m_displayNameHasBeenSet = true; m_displayName.assign(value); }

    /**
     * <p>Name of the Principal.</p>
     */
    inline Initiator& WithDisplayName(const Aws::String& value) { SetDisplayName(value); return *this;}

    /**
     * <p>Name of the Principal.</p>
     */
    inline Initiator& WithDisplayName(Aws::String&& value) { SetDisplayName(std::move(value)); return *this;}

    /**
     * <p>Name of the Principal.</p>
     */
    inline Initiator& WithDisplayName(const char* value) { SetDisplayName(value); return *this;}

  private:

    Aws::String m_iD;
    bool m_iDHasBeenSet = false;

    Aws::String m_displayName;
    bool m_displayNameHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
