/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/ObjectLockLegalHold.h>
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
  class GetObjectLegalHoldResult
  {
  public:
    AWS_S3_API GetObjectLegalHoldResult();
    AWS_S3_API GetObjectLegalHoldResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API GetObjectLegalHoldResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>The current legal hold status for the specified object.</p>
     */
    inline const ObjectLockLegalHold& GetLegalHold() const{ return m_legalHold; }

    /**
     * <p>The current legal hold status for the specified object.</p>
     */
    inline void SetLegalHold(const ObjectLockLegalHold& value) { m_legalHold = value; }

    /**
     * <p>The current legal hold status for the specified object.</p>
     */
    inline void SetLegalHold(ObjectLockLegalHold&& value) { m_legalHold = std::move(value); }

    /**
     * <p>The current legal hold status for the specified object.</p>
     */
    inline GetObjectLegalHoldResult& WithLegalHold(const ObjectLockLegalHold& value) { SetLegalHold(value); return *this;}

    /**
     * <p>The current legal hold status for the specified object.</p>
     */
    inline GetObjectLegalHoldResult& WithLegalHold(ObjectLockLegalHold&& value) { SetLegalHold(std::move(value)); return *this;}

  private:

    ObjectLockLegalHold m_legalHold;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
