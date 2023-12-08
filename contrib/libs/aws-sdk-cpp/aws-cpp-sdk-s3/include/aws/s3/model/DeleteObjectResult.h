/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/s3/model/RequestCharged.h>
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
  class DeleteObjectResult
  {
  public:
    AWS_S3_API DeleteObjectResult();
    AWS_S3_API DeleteObjectResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API DeleteObjectResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>Specifies whether the versioned object that was permanently deleted was
     * (true) or was not (false) a delete marker.</p>
     */
    inline bool GetDeleteMarker() const{ return m_deleteMarker; }

    /**
     * <p>Specifies whether the versioned object that was permanently deleted was
     * (true) or was not (false) a delete marker.</p>
     */
    inline void SetDeleteMarker(bool value) { m_deleteMarker = value; }

    /**
     * <p>Specifies whether the versioned object that was permanently deleted was
     * (true) or was not (false) a delete marker.</p>
     */
    inline DeleteObjectResult& WithDeleteMarker(bool value) { SetDeleteMarker(value); return *this;}


    /**
     * <p>Returns the version ID of the delete marker created as a result of the DELETE
     * operation.</p>
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * <p>Returns the version ID of the delete marker created as a result of the DELETE
     * operation.</p>
     */
    inline void SetVersionId(const Aws::String& value) { m_versionId = value; }

    /**
     * <p>Returns the version ID of the delete marker created as a result of the DELETE
     * operation.</p>
     */
    inline void SetVersionId(Aws::String&& value) { m_versionId = std::move(value); }

    /**
     * <p>Returns the version ID of the delete marker created as a result of the DELETE
     * operation.</p>
     */
    inline void SetVersionId(const char* value) { m_versionId.assign(value); }

    /**
     * <p>Returns the version ID of the delete marker created as a result of the DELETE
     * operation.</p>
     */
    inline DeleteObjectResult& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * <p>Returns the version ID of the delete marker created as a result of the DELETE
     * operation.</p>
     */
    inline DeleteObjectResult& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * <p>Returns the version ID of the delete marker created as a result of the DELETE
     * operation.</p>
     */
    inline DeleteObjectResult& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    
    inline const RequestCharged& GetRequestCharged() const{ return m_requestCharged; }

    
    inline void SetRequestCharged(const RequestCharged& value) { m_requestCharged = value; }

    
    inline void SetRequestCharged(RequestCharged&& value) { m_requestCharged = std::move(value); }

    
    inline DeleteObjectResult& WithRequestCharged(const RequestCharged& value) { SetRequestCharged(value); return *this;}

    
    inline DeleteObjectResult& WithRequestCharged(RequestCharged&& value) { SetRequestCharged(std::move(value)); return *this;}

  private:

    bool m_deleteMarker;

    Aws::String m_versionId;

    RequestCharged m_requestCharged;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
