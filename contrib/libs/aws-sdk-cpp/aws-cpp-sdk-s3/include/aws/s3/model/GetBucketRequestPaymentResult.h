/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/model/Payer.h>
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
  class GetBucketRequestPaymentResult
  {
  public:
    AWS_S3_API GetBucketRequestPaymentResult();
    AWS_S3_API GetBucketRequestPaymentResult(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);
    AWS_S3_API GetBucketRequestPaymentResult& operator=(const Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>& result);


    /**
     * <p>Specifies who pays for the download and request fees.</p>
     */
    inline const Payer& GetPayer() const{ return m_payer; }

    /**
     * <p>Specifies who pays for the download and request fees.</p>
     */
    inline void SetPayer(const Payer& value) { m_payer = value; }

    /**
     * <p>Specifies who pays for the download and request fees.</p>
     */
    inline void SetPayer(Payer&& value) { m_payer = std::move(value); }

    /**
     * <p>Specifies who pays for the download and request fees.</p>
     */
    inline GetBucketRequestPaymentResult& WithPayer(const Payer& value) { SetPayer(value); return *this;}

    /**
     * <p>Specifies who pays for the download and request fees.</p>
     */
    inline GetBucketRequestPaymentResult& WithPayer(Payer&& value) { SetPayer(std::move(value)); return *this;}

  private:

    Payer m_payer;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
