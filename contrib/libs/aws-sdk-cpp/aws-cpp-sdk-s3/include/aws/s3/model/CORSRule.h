/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
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
   * <p>Specifies a cross-origin access rule for an Amazon S3 bucket.</p><p><h3>See
   * Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/s3-2006-03-01/CORSRule">AWS API
   * Reference</a></p>
   */
  class CORSRule
  {
  public:
    AWS_S3_API CORSRule();
    AWS_S3_API CORSRule(const Aws::Utils::Xml::XmlNode& xmlNode);
    AWS_S3_API CORSRule& operator=(const Aws::Utils::Xml::XmlNode& xmlNode);

    AWS_S3_API void AddToNode(Aws::Utils::Xml::XmlNode& parentNode) const;


    /**
     * <p>Unique identifier for the rule. The value cannot be longer than 255
     * characters.</p>
     */
    inline const Aws::String& GetID() const{ return m_iD; }

    /**
     * <p>Unique identifier for the rule. The value cannot be longer than 255
     * characters.</p>
     */
    inline bool IDHasBeenSet() const { return m_iDHasBeenSet; }

    /**
     * <p>Unique identifier for the rule. The value cannot be longer than 255
     * characters.</p>
     */
    inline void SetID(const Aws::String& value) { m_iDHasBeenSet = true; m_iD = value; }

    /**
     * <p>Unique identifier for the rule. The value cannot be longer than 255
     * characters.</p>
     */
    inline void SetID(Aws::String&& value) { m_iDHasBeenSet = true; m_iD = std::move(value); }

    /**
     * <p>Unique identifier for the rule. The value cannot be longer than 255
     * characters.</p>
     */
    inline void SetID(const char* value) { m_iDHasBeenSet = true; m_iD.assign(value); }

    /**
     * <p>Unique identifier for the rule. The value cannot be longer than 255
     * characters.</p>
     */
    inline CORSRule& WithID(const Aws::String& value) { SetID(value); return *this;}

    /**
     * <p>Unique identifier for the rule. The value cannot be longer than 255
     * characters.</p>
     */
    inline CORSRule& WithID(Aws::String&& value) { SetID(std::move(value)); return *this;}

    /**
     * <p>Unique identifier for the rule. The value cannot be longer than 255
     * characters.</p>
     */
    inline CORSRule& WithID(const char* value) { SetID(value); return *this;}


    /**
     * <p>Headers that are specified in the <code>Access-Control-Request-Headers</code>
     * header. These headers are allowed in a preflight OPTIONS request. In response to
     * any preflight OPTIONS request, Amazon S3 returns any requested headers that are
     * allowed.</p>
     */
    inline const Aws::Vector<Aws::String>& GetAllowedHeaders() const{ return m_allowedHeaders; }

    /**
     * <p>Headers that are specified in the <code>Access-Control-Request-Headers</code>
     * header. These headers are allowed in a preflight OPTIONS request. In response to
     * any preflight OPTIONS request, Amazon S3 returns any requested headers that are
     * allowed.</p>
     */
    inline bool AllowedHeadersHasBeenSet() const { return m_allowedHeadersHasBeenSet; }

    /**
     * <p>Headers that are specified in the <code>Access-Control-Request-Headers</code>
     * header. These headers are allowed in a preflight OPTIONS request. In response to
     * any preflight OPTIONS request, Amazon S3 returns any requested headers that are
     * allowed.</p>
     */
    inline void SetAllowedHeaders(const Aws::Vector<Aws::String>& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders = value; }

    /**
     * <p>Headers that are specified in the <code>Access-Control-Request-Headers</code>
     * header. These headers are allowed in a preflight OPTIONS request. In response to
     * any preflight OPTIONS request, Amazon S3 returns any requested headers that are
     * allowed.</p>
     */
    inline void SetAllowedHeaders(Aws::Vector<Aws::String>&& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders = std::move(value); }

    /**
     * <p>Headers that are specified in the <code>Access-Control-Request-Headers</code>
     * header. These headers are allowed in a preflight OPTIONS request. In response to
     * any preflight OPTIONS request, Amazon S3 returns any requested headers that are
     * allowed.</p>
     */
    inline CORSRule& WithAllowedHeaders(const Aws::Vector<Aws::String>& value) { SetAllowedHeaders(value); return *this;}

    /**
     * <p>Headers that are specified in the <code>Access-Control-Request-Headers</code>
     * header. These headers are allowed in a preflight OPTIONS request. In response to
     * any preflight OPTIONS request, Amazon S3 returns any requested headers that are
     * allowed.</p>
     */
    inline CORSRule& WithAllowedHeaders(Aws::Vector<Aws::String>&& value) { SetAllowedHeaders(std::move(value)); return *this;}

    /**
     * <p>Headers that are specified in the <code>Access-Control-Request-Headers</code>
     * header. These headers are allowed in a preflight OPTIONS request. In response to
     * any preflight OPTIONS request, Amazon S3 returns any requested headers that are
     * allowed.</p>
     */
    inline CORSRule& AddAllowedHeaders(const Aws::String& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders.push_back(value); return *this; }

    /**
     * <p>Headers that are specified in the <code>Access-Control-Request-Headers</code>
     * header. These headers are allowed in a preflight OPTIONS request. In response to
     * any preflight OPTIONS request, Amazon S3 returns any requested headers that are
     * allowed.</p>
     */
    inline CORSRule& AddAllowedHeaders(Aws::String&& value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders.push_back(std::move(value)); return *this; }

    /**
     * <p>Headers that are specified in the <code>Access-Control-Request-Headers</code>
     * header. These headers are allowed in a preflight OPTIONS request. In response to
     * any preflight OPTIONS request, Amazon S3 returns any requested headers that are
     * allowed.</p>
     */
    inline CORSRule& AddAllowedHeaders(const char* value) { m_allowedHeadersHasBeenSet = true; m_allowedHeaders.push_back(value); return *this; }


    /**
     * <p>An HTTP method that you allow the origin to execute. Valid values are
     * <code>GET</code>, <code>PUT</code>, <code>HEAD</code>, <code>POST</code>, and
     * <code>DELETE</code>.</p>
     */
    inline const Aws::Vector<Aws::String>& GetAllowedMethods() const{ return m_allowedMethods; }

    /**
     * <p>An HTTP method that you allow the origin to execute. Valid values are
     * <code>GET</code>, <code>PUT</code>, <code>HEAD</code>, <code>POST</code>, and
     * <code>DELETE</code>.</p>
     */
    inline bool AllowedMethodsHasBeenSet() const { return m_allowedMethodsHasBeenSet; }

    /**
     * <p>An HTTP method that you allow the origin to execute. Valid values are
     * <code>GET</code>, <code>PUT</code>, <code>HEAD</code>, <code>POST</code>, and
     * <code>DELETE</code>.</p>
     */
    inline void SetAllowedMethods(const Aws::Vector<Aws::String>& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods = value; }

    /**
     * <p>An HTTP method that you allow the origin to execute. Valid values are
     * <code>GET</code>, <code>PUT</code>, <code>HEAD</code>, <code>POST</code>, and
     * <code>DELETE</code>.</p>
     */
    inline void SetAllowedMethods(Aws::Vector<Aws::String>&& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods = std::move(value); }

    /**
     * <p>An HTTP method that you allow the origin to execute. Valid values are
     * <code>GET</code>, <code>PUT</code>, <code>HEAD</code>, <code>POST</code>, and
     * <code>DELETE</code>.</p>
     */
    inline CORSRule& WithAllowedMethods(const Aws::Vector<Aws::String>& value) { SetAllowedMethods(value); return *this;}

    /**
     * <p>An HTTP method that you allow the origin to execute. Valid values are
     * <code>GET</code>, <code>PUT</code>, <code>HEAD</code>, <code>POST</code>, and
     * <code>DELETE</code>.</p>
     */
    inline CORSRule& WithAllowedMethods(Aws::Vector<Aws::String>&& value) { SetAllowedMethods(std::move(value)); return *this;}

    /**
     * <p>An HTTP method that you allow the origin to execute. Valid values are
     * <code>GET</code>, <code>PUT</code>, <code>HEAD</code>, <code>POST</code>, and
     * <code>DELETE</code>.</p>
     */
    inline CORSRule& AddAllowedMethods(const Aws::String& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods.push_back(value); return *this; }

    /**
     * <p>An HTTP method that you allow the origin to execute. Valid values are
     * <code>GET</code>, <code>PUT</code>, <code>HEAD</code>, <code>POST</code>, and
     * <code>DELETE</code>.</p>
     */
    inline CORSRule& AddAllowedMethods(Aws::String&& value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods.push_back(std::move(value)); return *this; }

    /**
     * <p>An HTTP method that you allow the origin to execute. Valid values are
     * <code>GET</code>, <code>PUT</code>, <code>HEAD</code>, <code>POST</code>, and
     * <code>DELETE</code>.</p>
     */
    inline CORSRule& AddAllowedMethods(const char* value) { m_allowedMethodsHasBeenSet = true; m_allowedMethods.push_back(value); return *this; }


    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline const Aws::Vector<Aws::String>& GetAllowedOrigins() const{ return m_allowedOrigins; }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline bool AllowedOriginsHasBeenSet() const { return m_allowedOriginsHasBeenSet; }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline void SetAllowedOrigins(const Aws::Vector<Aws::String>& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins = value; }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline void SetAllowedOrigins(Aws::Vector<Aws::String>&& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins = std::move(value); }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline CORSRule& WithAllowedOrigins(const Aws::Vector<Aws::String>& value) { SetAllowedOrigins(value); return *this;}

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline CORSRule& WithAllowedOrigins(Aws::Vector<Aws::String>&& value) { SetAllowedOrigins(std::move(value)); return *this;}

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline CORSRule& AddAllowedOrigins(const Aws::String& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins.push_back(value); return *this; }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline CORSRule& AddAllowedOrigins(Aws::String&& value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins.push_back(std::move(value)); return *this; }

    /**
     * <p>One or more origins you want customers to be able to access the bucket
     * from.</p>
     */
    inline CORSRule& AddAllowedOrigins(const char* value) { m_allowedOriginsHasBeenSet = true; m_allowedOrigins.push_back(value); return *this; }


    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript
     * <code>XMLHttpRequest</code> object).</p>
     */
    inline const Aws::Vector<Aws::String>& GetExposeHeaders() const{ return m_exposeHeaders; }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript
     * <code>XMLHttpRequest</code> object).</p>
     */
    inline bool ExposeHeadersHasBeenSet() const { return m_exposeHeadersHasBeenSet; }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript
     * <code>XMLHttpRequest</code> object).</p>
     */
    inline void SetExposeHeaders(const Aws::Vector<Aws::String>& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders = value; }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript
     * <code>XMLHttpRequest</code> object).</p>
     */
    inline void SetExposeHeaders(Aws::Vector<Aws::String>&& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders = std::move(value); }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript
     * <code>XMLHttpRequest</code> object).</p>
     */
    inline CORSRule& WithExposeHeaders(const Aws::Vector<Aws::String>& value) { SetExposeHeaders(value); return *this;}

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript
     * <code>XMLHttpRequest</code> object).</p>
     */
    inline CORSRule& WithExposeHeaders(Aws::Vector<Aws::String>&& value) { SetExposeHeaders(std::move(value)); return *this;}

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript
     * <code>XMLHttpRequest</code> object).</p>
     */
    inline CORSRule& AddExposeHeaders(const Aws::String& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders.push_back(value); return *this; }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript
     * <code>XMLHttpRequest</code> object).</p>
     */
    inline CORSRule& AddExposeHeaders(Aws::String&& value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders.push_back(std::move(value)); return *this; }

    /**
     * <p>One or more headers in the response that you want customers to be able to
     * access from their applications (for example, from a JavaScript
     * <code>XMLHttpRequest</code> object).</p>
     */
    inline CORSRule& AddExposeHeaders(const char* value) { m_exposeHeadersHasBeenSet = true; m_exposeHeaders.push_back(value); return *this; }


    /**
     * <p>The time in seconds that your browser is to cache the preflight response for
     * the specified resource.</p>
     */
    inline int GetMaxAgeSeconds() const{ return m_maxAgeSeconds; }

    /**
     * <p>The time in seconds that your browser is to cache the preflight response for
     * the specified resource.</p>
     */
    inline bool MaxAgeSecondsHasBeenSet() const { return m_maxAgeSecondsHasBeenSet; }

    /**
     * <p>The time in seconds that your browser is to cache the preflight response for
     * the specified resource.</p>
     */
    inline void SetMaxAgeSeconds(int value) { m_maxAgeSecondsHasBeenSet = true; m_maxAgeSeconds = value; }

    /**
     * <p>The time in seconds that your browser is to cache the preflight response for
     * the specified resource.</p>
     */
    inline CORSRule& WithMaxAgeSeconds(int value) { SetMaxAgeSeconds(value); return *this;}

  private:

    Aws::String m_iD;
    bool m_iDHasBeenSet = false;

    Aws::Vector<Aws::String> m_allowedHeaders;
    bool m_allowedHeadersHasBeenSet = false;

    Aws::Vector<Aws::String> m_allowedMethods;
    bool m_allowedMethodsHasBeenSet = false;

    Aws::Vector<Aws::String> m_allowedOrigins;
    bool m_allowedOriginsHasBeenSet = false;

    Aws::Vector<Aws::String> m_exposeHeaders;
    bool m_exposeHeadersHasBeenSet = false;

    int m_maxAgeSeconds;
    bool m_maxAgeSecondsHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
