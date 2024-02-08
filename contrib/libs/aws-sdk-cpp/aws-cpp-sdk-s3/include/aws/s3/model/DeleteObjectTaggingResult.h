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
template<typename RESULT_TYPE>
class AmazonWebServiceResult;

namespace Utils
{
namespace Xml
{
  class XmlDocument;
} // namespace Xml
} // namespace Utils
namespace S3
{
namespace Model
{
  class DeleteObjectTaggingResult
  {
  public:
    AWS_S3_API DeleteObjectTaggingResult();
    AWS_S3_API DeleteObjectTaggingResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API DeleteObjectTaggingResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>The versionId of the object the tag-set was removed from.</p>
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * <p>The versionId of the object the tag-set was removed from.</p>
     */
    inline void SetVersionId(const Aws::String& value) { m_versionId = value; }

    /**
     * <p>The versionId of the object the tag-set was removed from.</p>
     */
    inline void SetVersionId(Aws::String&& value) { m_versionId = std::move(value); }

    /**
     * <p>The versionId of the object the tag-set was removed from.</p>
     */
    inline void SetVersionId(const char* value) { m_versionId.assign(value); }

    /**
     * <p>The versionId of the object the tag-set was removed from.</p>
     */
    inline DeleteObjectTaggingResult& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * <p>The versionId of the object the tag-set was removed from.</p>
     */
    inline DeleteObjectTaggingResult& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * <p>The versionId of the object the tag-set was removed from.</p>
     */
    inline DeleteObjectTaggingResult& WithVersionId(const char* value) { SetVersionId(value); return *this;}

  private:

    Aws::String m_versionId;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
