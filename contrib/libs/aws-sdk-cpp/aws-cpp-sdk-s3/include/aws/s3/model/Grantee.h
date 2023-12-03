/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/Type.h>
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
   * <p>Container for the person being granted permissions.</p><p><h3>See Also:</h3> 
   * <a href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/Grantee">AWS API
   * Reference</a></p>
   */
  class Grantee
  {
  public:
    AWS_S3_API Grantee();
    AWS_S3_API Grantee(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API Grantee& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Screen name of the grantee.</p>
     */
    inline const Aws::String& GetDisplayName() const{ return m_displayName; }

    /**
     * <p>Screen name of the grantee.</p>
     */
    inline bool DisplayNameHasBeenSet() const { return m_displayNameHasBeenSet; }

    /**
     * <p>Screen name of the grantee.</p>
     */
    inline void SetDisplayName(const Aws::String& value) { m_displayNameHasBeenSet = true; m_displayName = value; }

    /**
     * <p>Screen name of the grantee.</p>
     */
    inline void SetDisplayName(Aws::String&& value) { m_displayNameHasBeenSet = true; m_displayName = std::move(value); }

    /**
     * <p>Screen name of the grantee.</p>
     */
    inline void SetDisplayName(const char* value) { m_displayNameHasBeenSet = true; m_displayName.assign(value); }

    /**
     * <p>Screen name of the grantee.</p>
     */
    inline Grantee& WithDisplayName(const Aws::String& value) { SetDisplayName(value); return *this;}

    /**
     * <p>Screen name of the grantee.</p>
     */
    inline Grantee& WithDisplayName(Aws::String&& value) { SetDisplayName(std::move(value)); return *this;}

    /**
     * <p>Screen name of the grantee.</p>
     */
    inline Grantee& WithDisplayName(const char* value) { SetDisplayName(value); return *this;}


    /**
     * <p>Email address of the grantee.</p>  <p>Using email addresses to specify
     * a grantee is only supported in the following Amazon Web Services Regions: </p>
     * <ul> <li> <p>US East (N. Virginia)</p> </li> <li> <p>US West (N. California)</p>
     * </li> <li> <p> US West (Oregon)</p> </li> <li> <p> Asia Pacific (Singapore)</p>
     * </li> <li> <p>Asia Pacific (Sydney)</p> </li> <li> <p>Asia Pacific (Tokyo)</p>
     * </li> <li> <p>Europe (Ireland)</p> </li> <li> <p>South America (São Paulo)</p>
     * </li> </ul> <p>For a list of all the Amazon S3 supported Regions and endpoints,
     * see <a
     * href="https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">Regions
     * and Endpoints</a> in the Amazon Web Services General Reference.</p> 
     */
    inline const Aws::String& GetEmailAddress() const{ return m_emailAddress; }

    /**
     * <p>Email address of the grantee.</p>  <p>Using email addresses to specify
     * a grantee is only supported in the following Amazon Web Services Regions: </p>
     * <ul> <li> <p>US East (N. Virginia)</p> </li> <li> <p>US West (N. California)</p>
     * </li> <li> <p> US West (Oregon)</p> </li> <li> <p> Asia Pacific (Singapore)</p>
     * </li> <li> <p>Asia Pacific (Sydney)</p> </li> <li> <p>Asia Pacific (Tokyo)</p>
     * </li> <li> <p>Europe (Ireland)</p> </li> <li> <p>South America (São Paulo)</p>
     * </li> </ul> <p>For a list of all the Amazon S3 supported Regions and endpoints,
     * see <a
     * href="https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">Regions
     * and Endpoints</a> in the Amazon Web Services General Reference.</p> 
     */
    inline bool EmailAddressHasBeenSet() const { return m_emailAddressHasBeenSet; }

    /**
     * <p>Email address of the grantee.</p>  <p>Using email addresses to specify
     * a grantee is only supported in the following Amazon Web Services Regions: </p>
     * <ul> <li> <p>US East (N. Virginia)</p> </li> <li> <p>US West (N. California)</p>
     * </li> <li> <p> US West (Oregon)</p> </li> <li> <p> Asia Pacific (Singapore)</p>
     * </li> <li> <p>Asia Pacific (Sydney)</p> </li> <li> <p>Asia Pacific (Tokyo)</p>
     * </li> <li> <p>Europe (Ireland)</p> </li> <li> <p>South America (São Paulo)</p>
     * </li> </ul> <p>For a list of all the Amazon S3 supported Regions and endpoints,
     * see <a
     * href="https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">Regions
     * and Endpoints</a> in the Amazon Web Services General Reference.</p> 
     */
    inline void SetEmailAddress(const Aws::String& value) { m_emailAddressHasBeenSet = true; m_emailAddress = value; }

    /**
     * <p>Email address of the grantee.</p>  <p>Using email addresses to specify
     * a grantee is only supported in the following Amazon Web Services Regions: </p>
     * <ul> <li> <p>US East (N. Virginia)</p> </li> <li> <p>US West (N. California)</p>
     * </li> <li> <p> US West (Oregon)</p> </li> <li> <p> Asia Pacific (Singapore)</p>
     * </li> <li> <p>Asia Pacific (Sydney)</p> </li> <li> <p>Asia Pacific (Tokyo)</p>
     * </li> <li> <p>Europe (Ireland)</p> </li> <li> <p>South America (São Paulo)</p>
     * </li> </ul> <p>For a list of all the Amazon S3 supported Regions and endpoints,
     * see <a
     * href="https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">Regions
     * and Endpoints</a> in the Amazon Web Services General Reference.</p> 
     */
    inline void SetEmailAddress(Aws::String&& value) { m_emailAddressHasBeenSet = true; m_emailAddress = std::move(value); }

    /**
     * <p>Email address of the grantee.</p>  <p>Using email addresses to specify
     * a grantee is only supported in the following Amazon Web Services Regions: </p>
     * <ul> <li> <p>US East (N. Virginia)</p> </li> <li> <p>US West (N. California)</p>
     * </li> <li> <p> US West (Oregon)</p> </li> <li> <p> Asia Pacific (Singapore)</p>
     * </li> <li> <p>Asia Pacific (Sydney)</p> </li> <li> <p>Asia Pacific (Tokyo)</p>
     * </li> <li> <p>Europe (Ireland)</p> </li> <li> <p>South America (São Paulo)</p>
     * </li> </ul> <p>For a list of all the Amazon S3 supported Regions and endpoints,
     * see <a
     * href="https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">Regions
     * and Endpoints</a> in the Amazon Web Services General Reference.</p> 
     */
    inline void SetEmailAddress(const char* value) { m_emailAddressHasBeenSet = true; m_emailAddress.assign(value); }

    /**
     * <p>Email address of the grantee.</p>  <p>Using email addresses to specify
     * a grantee is only supported in the following Amazon Web Services Regions: </p>
     * <ul> <li> <p>US East (N. Virginia)</p> </li> <li> <p>US West (N. California)</p>
     * </li> <li> <p> US West (Oregon)</p> </li> <li> <p> Asia Pacific (Singapore)</p>
     * </li> <li> <p>Asia Pacific (Sydney)</p> </li> <li> <p>Asia Pacific (Tokyo)</p>
     * </li> <li> <p>Europe (Ireland)</p> </li> <li> <p>South America (São Paulo)</p>
     * </li> </ul> <p>For a list of all the Amazon S3 supported Regions and endpoints,
     * see <a
     * href="https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">Regions
     * and Endpoints</a> in the Amazon Web Services General Reference.</p> 
     */
    inline Grantee& WithEmailAddress(const Aws::String& value) { SetEmailAddress(value); return *this;}

    /**
     * <p>Email address of the grantee.</p>  <p>Using email addresses to specify
     * a grantee is only supported in the following Amazon Web Services Regions: </p>
     * <ul> <li> <p>US East (N. Virginia)</p> </li> <li> <p>US West (N. California)</p>
     * </li> <li> <p> US West (Oregon)</p> </li> <li> <p> Asia Pacific (Singapore)</p>
     * </li> <li> <p>Asia Pacific (Sydney)</p> </li> <li> <p>Asia Pacific (Tokyo)</p>
     * </li> <li> <p>Europe (Ireland)</p> </li> <li> <p>South America (São Paulo)</p>
     * </li> </ul> <p>For a list of all the Amazon S3 supported Regions and endpoints,
     * see <a
     * href="https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">Regions
     * and Endpoints</a> in the Amazon Web Services General Reference.</p> 
     */
    inline Grantee& WithEmailAddress(Aws::String&& value) { SetEmailAddress(std::move(value)); return *this;}

    /**
     * <p>Email address of the grantee.</p>  <p>Using email addresses to specify
     * a grantee is only supported in the following Amazon Web Services Regions: </p>
     * <ul> <li> <p>US East (N. Virginia)</p> </li> <li> <p>US West (N. California)</p>
     * </li> <li> <p> US West (Oregon)</p> </li> <li> <p> Asia Pacific (Singapore)</p>
     * </li> <li> <p>Asia Pacific (Sydney)</p> </li> <li> <p>Asia Pacific (Tokyo)</p>
     * </li> <li> <p>Europe (Ireland)</p> </li> <li> <p>South America (São Paulo)</p>
     * </li> </ul> <p>For a list of all the Amazon S3 supported Regions and endpoints,
     * see <a
     * href="https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">Regions
     * and Endpoints</a> in the Amazon Web Services General Reference.</p> 
     */
    inline Grantee& WithEmailAddress(const char* value) { SetEmailAddress(value); return *this;}


    /**
     * <p>The canonical user ID of the grantee.</p>
     */
    inline const Aws::String& GetID() const{ return m_iD; }

    /**
     * <p>The canonical user ID of the grantee.</p>
     */
    inline bool IDHasBeenSet() const { return m_iDHasBeenSet; }

    /**
     * <p>The canonical user ID of the grantee.</p>
     */
    inline void SetID(const Aws::String& value) { m_iDHasBeenSet = true; m_iD = value; }

    /**
     * <p>The canonical user ID of the grantee.</p>
     */
    inline void SetID(Aws::String&& value) { m_iDHasBeenSet = true; m_iD = std::move(value); }

    /**
     * <p>The canonical user ID of the grantee.</p>
     */
    inline void SetID(const char* value) { m_iDHasBeenSet = true; m_iD.assign(value); }

    /**
     * <p>The canonical user ID of the grantee.</p>
     */
    inline Grantee& WithID(const Aws::String& value) { SetID(value); return *this;}

    /**
     * <p>The canonical user ID of the grantee.</p>
     */
    inline Grantee& WithID(Aws::String&& value) { SetID(std::move(value)); return *this;}

    /**
     * <p>The canonical user ID of the grantee.</p>
     */
    inline Grantee& WithID(const char* value) { SetID(value); return *this;}


    /**
     * <p>Type of grantee</p>
     */
    inline const Type& GetType() const{ return m_type; }

    /**
     * <p>Type of grantee</p>
     */
    inline bool TypeHasBeenSet() const { return m_typeHasBeenSet; }

    /**
     * <p>Type of grantee</p>
     */
    inline void SetType(const Type& value) { m_typeHasBeenSet = true; m_type = value; }

    /**
     * <p>Type of grantee</p>
     */
    inline void SetType(Type&& value) { m_typeHasBeenSet = true; m_type = std::move(value); }

    /**
     * <p>Type of grantee</p>
     */
    inline Grantee& WithType(const Type& value) { SetType(value); return *this;}

    /**
     * <p>Type of grantee</p>
     */
    inline Grantee& WithType(Type&& value) { SetType(std::move(value)); return *this;}


    /**
     * <p>URI of the grantee group.</p>
     */
    inline const Aws::String& GetURI() const{ return m_uRI; }

    /**
     * <p>URI of the grantee group.</p>
     */
    inline bool URIHasBeenSet() const { return m_uRIHasBeenSet; }

    /**
     * <p>URI of the grantee group.</p>
     */
    inline void SetURI(const Aws::String& value) { m_uRIHasBeenSet = true; m_uRI = value; }

    /**
     * <p>URI of the grantee group.</p>
     */
    inline void SetURI(Aws::String&& value) { m_uRIHasBeenSet = true; m_uRI = std::move(value); }

    /**
     * <p>URI of the grantee group.</p>
     */
    inline void SetURI(const char* value) { m_uRIHasBeenSet = true; m_uRI.assign(value); }

    /**
     * <p>URI of the grantee group.</p>
     */
    inline Grantee& WithURI(const Aws::String& value) { SetURI(value); return *this;}

    /**
     * <p>URI of the grantee group.</p>
     */
    inline Grantee& WithURI(Aws::String&& value) { SetURI(std::move(value)); return *this;}

    /**
     * <p>URI of the grantee group.</p>
     */
    inline Grantee& WithURI(const char* value) { SetURI(value); return *this;}

  private:

    Aws::String m_displayName;
    bool m_displayNameHasBeenSet = false;

    Aws::String m_emailAddress;
    bool m_emailAddressHasBeenSet = false;

    Aws::String m_iD;
    bool m_iDHasBeenSet = false;

    Type m_type;
    bool m_typeHasBeenSet = false;

    Aws::String m_uRI;
    bool m_uRIHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
