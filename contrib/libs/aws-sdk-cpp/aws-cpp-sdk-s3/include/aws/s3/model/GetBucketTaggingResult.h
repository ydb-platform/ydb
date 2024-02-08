/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/Tag.h>
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
  class GetBucketTaggingResult
  {
  public:
    AWS_S3_API GetBucketTaggingResult();
    AWS_S3_API GetBucketTaggingResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API GetBucketTaggingResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>Contains the tag set.</p>
     */
    inline const Aws::Vector<Tag>& GetTagSet() const{ return m_tagSet; }

    /**
     * <p>Contains the tag set.</p>
     */
    inline void SetTagSet(const Aws::Vector<Tag>& value) { m_tagSet = value; }

    /**
     * <p>Contains the tag set.</p>
     */
    inline void SetTagSet(Aws::Vector<Tag>&& value) { m_tagSet = std::move(value); }

    /**
     * <p>Contains the tag set.</p>
     */
    inline GetBucketTaggingResult& WithTagSet(const Aws::Vector<Tag>& value) { SetTagSet(value); return *this;}

    /**
     * <p>Contains the tag set.</p>
     */
    inline GetBucketTaggingResult& WithTagSet(Aws::Vector<Tag>&& value) { SetTagSet(std::move(value)); return *this;}

    /**
     * <p>Contains the tag set.</p>
     */
    inline GetBucketTaggingResult& AddTagSet(const Tag& value) { m_tagSet.push_back(value); return *this; }

    /**
     * <p>Contains the tag set.</p>
     */
    inline GetBucketTaggingResult& AddTagSet(Tag&& value) { m_tagSet.push_back(std::move(value)); return *this; }

  private:

    Aws::Vector<Tag> m_tagSet;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
