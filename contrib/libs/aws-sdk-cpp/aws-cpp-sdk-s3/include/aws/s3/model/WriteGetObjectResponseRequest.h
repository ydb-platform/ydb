/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/s3/S3_EXPORTS.h>
#include <aws/s3/S3Request.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/DateTime.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/s3/model/ObjectLockMode.h>
#include <aws/s3/model/ObjectLockLegalHoldStatus.h>
#include <aws/s3/model/ReplicationStatus.h>
#include <aws/s3/model/RequestCharged.h>
#include <aws/s3/model/ServerSideEncryption.h>
#include <aws/s3/model/StorageClass.h>
#include <utility>

namespace Aws
{
namespace Http
{
    class URI;
} //namespace Http
namespace S3
{
namespace Model
{

  /**
   */
  class WriteGetObjectResponseRequest : public StreamingS3Request
  {
  public:
    AWS_S3_API WriteGetObjectResponseRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "WriteGetObjectResponse"; }

    AWS_S3_API void AddQueryStringParameters(Aws::Http::URI& uri) const override;

    AWS_S3_API Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;

    AWS_S3_API bool SignBody() const override { return false; }

    AWS_S3_API bool IsChunked() const override { return true; }

    /**
     * Helper function to collect parameters (configurable and static hardcoded) required for endpoint computation.
     */
    AWS_S3_API EndpointParameters GetEndpointContextParams() const override;

    /**
     * <p>Route prefix to the HTTP URL generated.</p>
     */
    inline const Aws::String& GetRequestRoute() const{ return m_requestRoute; }

    /**
     * <p>Route prefix to the HTTP URL generated.</p>
     */
    inline bool RequestRouteHasBeenSet() const { return m_requestRouteHasBeenSet; }

    /**
     * <p>Route prefix to the HTTP URL generated.</p>
     */
    inline void SetRequestRoute(const Aws::String& value) { m_requestRouteHasBeenSet = true; m_requestRoute = value; }

    /**
     * <p>Route prefix to the HTTP URL generated.</p>
     */
    inline void SetRequestRoute(Aws::String&& value) { m_requestRouteHasBeenSet = true; m_requestRoute = std::move(value); }

    /**
     * <p>Route prefix to the HTTP URL generated.</p>
     */
    inline void SetRequestRoute(const char* value) { m_requestRouteHasBeenSet = true; m_requestRoute.assign(value); }

    /**
     * <p>Route prefix to the HTTP URL generated.</p>
     */
    inline WriteGetObjectResponseRequest& WithRequestRoute(const Aws::String& value) { SetRequestRoute(value); return *this;}

    /**
     * <p>Route prefix to the HTTP URL generated.</p>
     */
    inline WriteGetObjectResponseRequest& WithRequestRoute(Aws::String&& value) { SetRequestRoute(std::move(value)); return *this;}

    /**
     * <p>Route prefix to the HTTP URL generated.</p>
     */
    inline WriteGetObjectResponseRequest& WithRequestRoute(const char* value) { SetRequestRoute(value); return *this;}


    /**
     * <p>A single use encrypted token that maps <code>WriteGetObjectResponse</code> to
     * the end user <code>GetObject</code> request.</p>
     */
    inline const Aws::String& GetRequestToken() const{ return m_requestToken; }

    /**
     * <p>A single use encrypted token that maps <code>WriteGetObjectResponse</code> to
     * the end user <code>GetObject</code> request.</p>
     */
    inline bool RequestTokenHasBeenSet() const { return m_requestTokenHasBeenSet; }

    /**
     * <p>A single use encrypted token that maps <code>WriteGetObjectResponse</code> to
     * the end user <code>GetObject</code> request.</p>
     */
    inline void SetRequestToken(const Aws::String& value) { m_requestTokenHasBeenSet = true; m_requestToken = value; }

    /**
     * <p>A single use encrypted token that maps <code>WriteGetObjectResponse</code> to
     * the end user <code>GetObject</code> request.</p>
     */
    inline void SetRequestToken(Aws::String&& value) { m_requestTokenHasBeenSet = true; m_requestToken = std::move(value); }

    /**
     * <p>A single use encrypted token that maps <code>WriteGetObjectResponse</code> to
     * the end user <code>GetObject</code> request.</p>
     */
    inline void SetRequestToken(const char* value) { m_requestTokenHasBeenSet = true; m_requestToken.assign(value); }

    /**
     * <p>A single use encrypted token that maps <code>WriteGetObjectResponse</code> to
     * the end user <code>GetObject</code> request.</p>
     */
    inline WriteGetObjectResponseRequest& WithRequestToken(const Aws::String& value) { SetRequestToken(value); return *this;}

    /**
     * <p>A single use encrypted token that maps <code>WriteGetObjectResponse</code> to
     * the end user <code>GetObject</code> request.</p>
     */
    inline WriteGetObjectResponseRequest& WithRequestToken(Aws::String&& value) { SetRequestToken(std::move(value)); return *this;}

    /**
     * <p>A single use encrypted token that maps <code>WriteGetObjectResponse</code> to
     * the end user <code>GetObject</code> request.</p>
     */
    inline WriteGetObjectResponseRequest& WithRequestToken(const char* value) { SetRequestToken(value); return *this;}


    /**
     * <p>The integer status code for an HTTP response of a corresponding
     * <code>GetObject</code> request.</p> <p class="title"> <b>Status Codes</b> </p>
     * <ul> <li> <p> <code>200 - OK</code> </p> </li> <li> <p> <code>206 - Partial
     * Content</code> </p> </li> <li> <p> <code>304 - Not Modified</code> </p> </li>
     * <li> <p> <code>400 - Bad Request</code> </p> </li> <li> <p> <code>401 -
     * Unauthorized</code> </p> </li> <li> <p> <code>403 - Forbidden</code> </p> </li>
     * <li> <p> <code>404 - Not Found</code> </p> </li> <li> <p> <code>405 - Method Not
     * Allowed</code> </p> </li> <li> <p> <code>409 - Conflict</code> </p> </li> <li>
     * <p> <code>411 - Length Required</code> </p> </li> <li> <p> <code>412 -
     * Precondition Failed</code> </p> </li> <li> <p> <code>416 - Range Not
     * Satisfiable</code> </p> </li> <li> <p> <code>500 - Internal Server Error</code>
     * </p> </li> <li> <p> <code>503 - Service Unavailable</code> </p> </li> </ul>
     */
    inline int GetStatusCode() const{ return m_statusCode; }

    /**
     * <p>The integer status code for an HTTP response of a corresponding
     * <code>GetObject</code> request.</p> <p class="title"> <b>Status Codes</b> </p>
     * <ul> <li> <p> <code>200 - OK</code> </p> </li> <li> <p> <code>206 - Partial
     * Content</code> </p> </li> <li> <p> <code>304 - Not Modified</code> </p> </li>
     * <li> <p> <code>400 - Bad Request</code> </p> </li> <li> <p> <code>401 -
     * Unauthorized</code> </p> </li> <li> <p> <code>403 - Forbidden</code> </p> </li>
     * <li> <p> <code>404 - Not Found</code> </p> </li> <li> <p> <code>405 - Method Not
     * Allowed</code> </p> </li> <li> <p> <code>409 - Conflict</code> </p> </li> <li>
     * <p> <code>411 - Length Required</code> </p> </li> <li> <p> <code>412 -
     * Precondition Failed</code> </p> </li> <li> <p> <code>416 - Range Not
     * Satisfiable</code> </p> </li> <li> <p> <code>500 - Internal Server Error</code>
     * </p> </li> <li> <p> <code>503 - Service Unavailable</code> </p> </li> </ul>
     */
    inline bool StatusCodeHasBeenSet() const { return m_statusCodeHasBeenSet; }

    /**
     * <p>The integer status code for an HTTP response of a corresponding
     * <code>GetObject</code> request.</p> <p class="title"> <b>Status Codes</b> </p>
     * <ul> <li> <p> <code>200 - OK</code> </p> </li> <li> <p> <code>206 - Partial
     * Content</code> </p> </li> <li> <p> <code>304 - Not Modified</code> </p> </li>
     * <li> <p> <code>400 - Bad Request</code> </p> </li> <li> <p> <code>401 -
     * Unauthorized</code> </p> </li> <li> <p> <code>403 - Forbidden</code> </p> </li>
     * <li> <p> <code>404 - Not Found</code> </p> </li> <li> <p> <code>405 - Method Not
     * Allowed</code> </p> </li> <li> <p> <code>409 - Conflict</code> </p> </li> <li>
     * <p> <code>411 - Length Required</code> </p> </li> <li> <p> <code>412 -
     * Precondition Failed</code> </p> </li> <li> <p> <code>416 - Range Not
     * Satisfiable</code> </p> </li> <li> <p> <code>500 - Internal Server Error</code>
     * </p> </li> <li> <p> <code>503 - Service Unavailable</code> </p> </li> </ul>
     */
    inline void SetStatusCode(int value) { m_statusCodeHasBeenSet = true; m_statusCode = value; }

    /**
     * <p>The integer status code for an HTTP response of a corresponding
     * <code>GetObject</code> request.</p> <p class="title"> <b>Status Codes</b> </p>
     * <ul> <li> <p> <code>200 - OK</code> </p> </li> <li> <p> <code>206 - Partial
     * Content</code> </p> </li> <li> <p> <code>304 - Not Modified</code> </p> </li>
     * <li> <p> <code>400 - Bad Request</code> </p> </li> <li> <p> <code>401 -
     * Unauthorized</code> </p> </li> <li> <p> <code>403 - Forbidden</code> </p> </li>
     * <li> <p> <code>404 - Not Found</code> </p> </li> <li> <p> <code>405 - Method Not
     * Allowed</code> </p> </li> <li> <p> <code>409 - Conflict</code> </p> </li> <li>
     * <p> <code>411 - Length Required</code> </p> </li> <li> <p> <code>412 -
     * Precondition Failed</code> </p> </li> <li> <p> <code>416 - Range Not
     * Satisfiable</code> </p> </li> <li> <p> <code>500 - Internal Server Error</code>
     * </p> </li> <li> <p> <code>503 - Service Unavailable</code> </p> </li> </ul>
     */
    inline WriteGetObjectResponseRequest& WithStatusCode(int value) { SetStatusCode(value); return *this;}


    /**
     * <p>A string that uniquely identifies an error condition. Returned in the
     * &lt;Code&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in the
     * body. All error codes from S3 are sentence-cased. The regular expression (regex)
     * value is <code>"^[A-Z][a-zA-Z]+$"</code>.</p>
     */
    inline const Aws::String& GetErrorCode() const{ return m_errorCode; }

    /**
     * <p>A string that uniquely identifies an error condition. Returned in the
     * &lt;Code&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in the
     * body. All error codes from S3 are sentence-cased. The regular expression (regex)
     * value is <code>"^[A-Z][a-zA-Z]+$"</code>.</p>
     */
    inline bool ErrorCodeHasBeenSet() const { return m_errorCodeHasBeenSet; }

    /**
     * <p>A string that uniquely identifies an error condition. Returned in the
     * &lt;Code&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in the
     * body. All error codes from S3 are sentence-cased. The regular expression (regex)
     * value is <code>"^[A-Z][a-zA-Z]+$"</code>.</p>
     */
    inline void SetErrorCode(const Aws::String& value) { m_errorCodeHasBeenSet = true; m_errorCode = value; }

    /**
     * <p>A string that uniquely identifies an error condition. Returned in the
     * &lt;Code&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in the
     * body. All error codes from S3 are sentence-cased. The regular expression (regex)
     * value is <code>"^[A-Z][a-zA-Z]+$"</code>.</p>
     */
    inline void SetErrorCode(Aws::String&& value) { m_errorCodeHasBeenSet = true; m_errorCode = std::move(value); }

    /**
     * <p>A string that uniquely identifies an error condition. Returned in the
     * &lt;Code&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in the
     * body. All error codes from S3 are sentence-cased. The regular expression (regex)
     * value is <code>"^[A-Z][a-zA-Z]+$"</code>.</p>
     */
    inline void SetErrorCode(const char* value) { m_errorCodeHasBeenSet = true; m_errorCode.assign(value); }

    /**
     * <p>A string that uniquely identifies an error condition. Returned in the
     * &lt;Code&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in the
     * body. All error codes from S3 are sentence-cased. The regular expression (regex)
     * value is <code>"^[A-Z][a-zA-Z]+$"</code>.</p>
     */
    inline WriteGetObjectResponseRequest& WithErrorCode(const Aws::String& value) { SetErrorCode(value); return *this;}

    /**
     * <p>A string that uniquely identifies an error condition. Returned in the
     * &lt;Code&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in the
     * body. All error codes from S3 are sentence-cased. The regular expression (regex)
     * value is <code>"^[A-Z][a-zA-Z]+$"</code>.</p>
     */
    inline WriteGetObjectResponseRequest& WithErrorCode(Aws::String&& value) { SetErrorCode(std::move(value)); return *this;}

    /**
     * <p>A string that uniquely identifies an error condition. Returned in the
     * &lt;Code&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in the
     * body. All error codes from S3 are sentence-cased. The regular expression (regex)
     * value is <code>"^[A-Z][a-zA-Z]+$"</code>.</p>
     */
    inline WriteGetObjectResponseRequest& WithErrorCode(const char* value) { SetErrorCode(value); return *this;}


    /**
     * <p>Contains a generic description of the error condition. Returned in the
     * &lt;Message&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in
     * body.</p>
     */
    inline const Aws::String& GetErrorMessage() const{ return m_errorMessage; }

    /**
     * <p>Contains a generic description of the error condition. Returned in the
     * &lt;Message&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in
     * body.</p>
     */
    inline bool ErrorMessageHasBeenSet() const { return m_errorMessageHasBeenSet; }

    /**
     * <p>Contains a generic description of the error condition. Returned in the
     * &lt;Message&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in
     * body.</p>
     */
    inline void SetErrorMessage(const Aws::String& value) { m_errorMessageHasBeenSet = true; m_errorMessage = value; }

    /**
     * <p>Contains a generic description of the error condition. Returned in the
     * &lt;Message&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in
     * body.</p>
     */
    inline void SetErrorMessage(Aws::String&& value) { m_errorMessageHasBeenSet = true; m_errorMessage = std::move(value); }

    /**
     * <p>Contains a generic description of the error condition. Returned in the
     * &lt;Message&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in
     * body.</p>
     */
    inline void SetErrorMessage(const char* value) { m_errorMessageHasBeenSet = true; m_errorMessage.assign(value); }

    /**
     * <p>Contains a generic description of the error condition. Returned in the
     * &lt;Message&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in
     * body.</p>
     */
    inline WriteGetObjectResponseRequest& WithErrorMessage(const Aws::String& value) { SetErrorMessage(value); return *this;}

    /**
     * <p>Contains a generic description of the error condition. Returned in the
     * &lt;Message&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in
     * body.</p>
     */
    inline WriteGetObjectResponseRequest& WithErrorMessage(Aws::String&& value) { SetErrorMessage(std::move(value)); return *this;}

    /**
     * <p>Contains a generic description of the error condition. Returned in the
     * &lt;Message&gt; tag of the error XML response for a corresponding
     * <code>GetObject</code> call. Cannot be used with a successful
     * <code>StatusCode</code> header or when the transformed object is provided in
     * body.</p>
     */
    inline WriteGetObjectResponseRequest& WithErrorMessage(const char* value) { SetErrorMessage(value); return *this;}


    /**
     * <p>Indicates that a range of bytes was specified.</p>
     */
    inline const Aws::String& GetAcceptRanges() const{ return m_acceptRanges; }

    /**
     * <p>Indicates that a range of bytes was specified.</p>
     */
    inline bool AcceptRangesHasBeenSet() const { return m_acceptRangesHasBeenSet; }

    /**
     * <p>Indicates that a range of bytes was specified.</p>
     */
    inline void SetAcceptRanges(const Aws::String& value) { m_acceptRangesHasBeenSet = true; m_acceptRanges = value; }

    /**
     * <p>Indicates that a range of bytes was specified.</p>
     */
    inline void SetAcceptRanges(Aws::String&& value) { m_acceptRangesHasBeenSet = true; m_acceptRanges = std::move(value); }

    /**
     * <p>Indicates that a range of bytes was specified.</p>
     */
    inline void SetAcceptRanges(const char* value) { m_acceptRangesHasBeenSet = true; m_acceptRanges.assign(value); }

    /**
     * <p>Indicates that a range of bytes was specified.</p>
     */
    inline WriteGetObjectResponseRequest& WithAcceptRanges(const Aws::String& value) { SetAcceptRanges(value); return *this;}

    /**
     * <p>Indicates that a range of bytes was specified.</p>
     */
    inline WriteGetObjectResponseRequest& WithAcceptRanges(Aws::String&& value) { SetAcceptRanges(std::move(value)); return *this;}

    /**
     * <p>Indicates that a range of bytes was specified.</p>
     */
    inline WriteGetObjectResponseRequest& WithAcceptRanges(const char* value) { SetAcceptRanges(value); return *this;}


    /**
     * <p>Specifies caching behavior along the request/reply chain.</p>
     */
    inline const Aws::String& GetCacheControl() const{ return m_cacheControl; }

    /**
     * <p>Specifies caching behavior along the request/reply chain.</p>
     */
    inline bool CacheControlHasBeenSet() const { return m_cacheControlHasBeenSet; }

    /**
     * <p>Specifies caching behavior along the request/reply chain.</p>
     */
    inline void SetCacheControl(const Aws::String& value) { m_cacheControlHasBeenSet = true; m_cacheControl = value; }

    /**
     * <p>Specifies caching behavior along the request/reply chain.</p>
     */
    inline void SetCacheControl(Aws::String&& value) { m_cacheControlHasBeenSet = true; m_cacheControl = std::move(value); }

    /**
     * <p>Specifies caching behavior along the request/reply chain.</p>
     */
    inline void SetCacheControl(const char* value) { m_cacheControlHasBeenSet = true; m_cacheControl.assign(value); }

    /**
     * <p>Specifies caching behavior along the request/reply chain.</p>
     */
    inline WriteGetObjectResponseRequest& WithCacheControl(const Aws::String& value) { SetCacheControl(value); return *this;}

    /**
     * <p>Specifies caching behavior along the request/reply chain.</p>
     */
    inline WriteGetObjectResponseRequest& WithCacheControl(Aws::String&& value) { SetCacheControl(std::move(value)); return *this;}

    /**
     * <p>Specifies caching behavior along the request/reply chain.</p>
     */
    inline WriteGetObjectResponseRequest& WithCacheControl(const char* value) { SetCacheControl(value); return *this;}


    /**
     * <p>Specifies presentational information for the object.</p>
     */
    inline const Aws::String& GetContentDisposition() const{ return m_contentDisposition; }

    /**
     * <p>Specifies presentational information for the object.</p>
     */
    inline bool ContentDispositionHasBeenSet() const { return m_contentDispositionHasBeenSet; }

    /**
     * <p>Specifies presentational information for the object.</p>
     */
    inline void SetContentDisposition(const Aws::String& value) { m_contentDispositionHasBeenSet = true; m_contentDisposition = value; }

    /**
     * <p>Specifies presentational information for the object.</p>
     */
    inline void SetContentDisposition(Aws::String&& value) { m_contentDispositionHasBeenSet = true; m_contentDisposition = std::move(value); }

    /**
     * <p>Specifies presentational information for the object.</p>
     */
    inline void SetContentDisposition(const char* value) { m_contentDispositionHasBeenSet = true; m_contentDisposition.assign(value); }

    /**
     * <p>Specifies presentational information for the object.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentDisposition(const Aws::String& value) { SetContentDisposition(value); return *this;}

    /**
     * <p>Specifies presentational information for the object.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentDisposition(Aws::String&& value) { SetContentDisposition(std::move(value)); return *this;}

    /**
     * <p>Specifies presentational information for the object.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentDisposition(const char* value) { SetContentDisposition(value); return *this;}


    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field.</p>
     */
    inline const Aws::String& GetContentEncoding() const{ return m_contentEncoding; }

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field.</p>
     */
    inline bool ContentEncodingHasBeenSet() const { return m_contentEncodingHasBeenSet; }

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field.</p>
     */
    inline void SetContentEncoding(const Aws::String& value) { m_contentEncodingHasBeenSet = true; m_contentEncoding = value; }

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field.</p>
     */
    inline void SetContentEncoding(Aws::String&& value) { m_contentEncodingHasBeenSet = true; m_contentEncoding = std::move(value); }

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field.</p>
     */
    inline void SetContentEncoding(const char* value) { m_contentEncodingHasBeenSet = true; m_contentEncoding.assign(value); }

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentEncoding(const Aws::String& value) { SetContentEncoding(value); return *this;}

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentEncoding(Aws::String&& value) { SetContentEncoding(std::move(value)); return *this;}

    /**
     * <p>Specifies what content encodings have been applied to the object and thus
     * what decoding mechanisms must be applied to obtain the media-type referenced by
     * the Content-Type header field.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentEncoding(const char* value) { SetContentEncoding(value); return *this;}


    /**
     * <p>The language the content is in.</p>
     */
    inline const Aws::String& GetContentLanguage() const{ return m_contentLanguage; }

    /**
     * <p>The language the content is in.</p>
     */
    inline bool ContentLanguageHasBeenSet() const { return m_contentLanguageHasBeenSet; }

    /**
     * <p>The language the content is in.</p>
     */
    inline void SetContentLanguage(const Aws::String& value) { m_contentLanguageHasBeenSet = true; m_contentLanguage = value; }

    /**
     * <p>The language the content is in.</p>
     */
    inline void SetContentLanguage(Aws::String&& value) { m_contentLanguageHasBeenSet = true; m_contentLanguage = std::move(value); }

    /**
     * <p>The language the content is in.</p>
     */
    inline void SetContentLanguage(const char* value) { m_contentLanguageHasBeenSet = true; m_contentLanguage.assign(value); }

    /**
     * <p>The language the content is in.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentLanguage(const Aws::String& value) { SetContentLanguage(value); return *this;}

    /**
     * <p>The language the content is in.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentLanguage(Aws::String&& value) { SetContentLanguage(std::move(value)); return *this;}

    /**
     * <p>The language the content is in.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentLanguage(const char* value) { SetContentLanguage(value); return *this;}


    /**
     * <p>The size of the content body in bytes.</p>
     */
    inline long long GetContentLength() const{ return m_contentLength; }

    /**
     * <p>The size of the content body in bytes.</p>
     */
    inline bool ContentLengthHasBeenSet() const { return m_contentLengthHasBeenSet; }

    /**
     * <p>The size of the content body in bytes.</p>
     */
    inline void SetContentLength(long long value) { m_contentLengthHasBeenSet = true; m_contentLength = value; }

    /**
     * <p>The size of the content body in bytes.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentLength(long long value) { SetContentLength(value); return *this;}


    /**
     * <p>The portion of the object returned in the response.</p>
     */
    inline const Aws::String& GetContentRange() const{ return m_contentRange; }

    /**
     * <p>The portion of the object returned in the response.</p>
     */
    inline bool ContentRangeHasBeenSet() const { return m_contentRangeHasBeenSet; }

    /**
     * <p>The portion of the object returned in the response.</p>
     */
    inline void SetContentRange(const Aws::String& value) { m_contentRangeHasBeenSet = true; m_contentRange = value; }

    /**
     * <p>The portion of the object returned in the response.</p>
     */
    inline void SetContentRange(Aws::String&& value) { m_contentRangeHasBeenSet = true; m_contentRange = std::move(value); }

    /**
     * <p>The portion of the object returned in the response.</p>
     */
    inline void SetContentRange(const char* value) { m_contentRangeHasBeenSet = true; m_contentRange.assign(value); }

    /**
     * <p>The portion of the object returned in the response.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentRange(const Aws::String& value) { SetContentRange(value); return *this;}

    /**
     * <p>The portion of the object returned in the response.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentRange(Aws::String&& value) { SetContentRange(std::move(value)); return *this;}

    /**
     * <p>The portion of the object returned in the response.</p>
     */
    inline WriteGetObjectResponseRequest& WithContentRange(const char* value) { SetContentRange(value); return *this;}


    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p> <p/>
     */
    inline const Aws::String& GetChecksumCRC32() const{ return m_checksumCRC32; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p> <p/>
     */
    inline bool ChecksumCRC32HasBeenSet() const { return m_checksumCRC32HasBeenSet; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p> <p/>
     */
    inline void SetChecksumCRC32(const Aws::String& value) { m_checksumCRC32HasBeenSet = true; m_checksumCRC32 = value; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p> <p/>
     */
    inline void SetChecksumCRC32(Aws::String&& value) { m_checksumCRC32HasBeenSet = true; m_checksumCRC32 = std::move(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p> <p/>
     */
    inline void SetChecksumCRC32(const char* value) { m_checksumCRC32HasBeenSet = true; m_checksumCRC32.assign(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p> <p/>
     */
    inline WriteGetObjectResponseRequest& WithChecksumCRC32(const Aws::String& value) { SetChecksumCRC32(value); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p> <p/>
     */
    inline WriteGetObjectResponseRequest& WithChecksumCRC32(Aws::String&& value) { SetChecksumCRC32(std::move(value)); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32 checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p> <p/>
     */
    inline WriteGetObjectResponseRequest& WithChecksumCRC32(const char* value) { SetChecksumCRC32(value); return *this;}


    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline const Aws::String& GetChecksumCRC32C() const{ return m_checksumCRC32C; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline bool ChecksumCRC32CHasBeenSet() const { return m_checksumCRC32CHasBeenSet; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline void SetChecksumCRC32C(const Aws::String& value) { m_checksumCRC32CHasBeenSet = true; m_checksumCRC32C = value; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline void SetChecksumCRC32C(Aws::String&& value) { m_checksumCRC32CHasBeenSet = true; m_checksumCRC32C = std::move(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline void SetChecksumCRC32C(const char* value) { m_checksumCRC32CHasBeenSet = true; m_checksumCRC32C.assign(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline WriteGetObjectResponseRequest& WithChecksumCRC32C(const Aws::String& value) { SetChecksumCRC32C(value); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline WriteGetObjectResponseRequest& WithChecksumCRC32C(Aws::String&& value) { SetChecksumCRC32C(std::move(value)); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 32-bit CRC32C checksum of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline WriteGetObjectResponseRequest& WithChecksumCRC32C(const char* value) { SetChecksumCRC32C(value); return *this;}


    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object returned by the Object Lambda
     * function. This may not match the checksum for the object stored in Amazon S3.
     * Amazon S3 will perform validation of the checksum values only when the original
     * <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline const Aws::String& GetChecksumSHA1() const{ return m_checksumSHA1; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object returned by the Object Lambda
     * function. This may not match the checksum for the object stored in Amazon S3.
     * Amazon S3 will perform validation of the checksum values only when the original
     * <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline bool ChecksumSHA1HasBeenSet() const { return m_checksumSHA1HasBeenSet; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object returned by the Object Lambda
     * function. This may not match the checksum for the object stored in Amazon S3.
     * Amazon S3 will perform validation of the checksum values only when the original
     * <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline void SetChecksumSHA1(const Aws::String& value) { m_checksumSHA1HasBeenSet = true; m_checksumSHA1 = value; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object returned by the Object Lambda
     * function. This may not match the checksum for the object stored in Amazon S3.
     * Amazon S3 will perform validation of the checksum values only when the original
     * <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline void SetChecksumSHA1(Aws::String&& value) { m_checksumSHA1HasBeenSet = true; m_checksumSHA1 = std::move(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object returned by the Object Lambda
     * function. This may not match the checksum for the object stored in Amazon S3.
     * Amazon S3 will perform validation of the checksum values only when the original
     * <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline void SetChecksumSHA1(const char* value) { m_checksumSHA1HasBeenSet = true; m_checksumSHA1.assign(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object returned by the Object Lambda
     * function. This may not match the checksum for the object stored in Amazon S3.
     * Amazon S3 will perform validation of the checksum values only when the original
     * <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline WriteGetObjectResponseRequest& WithChecksumSHA1(const Aws::String& value) { SetChecksumSHA1(value); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object returned by the Object Lambda
     * function. This may not match the checksum for the object stored in Amazon S3.
     * Amazon S3 will perform validation of the checksum values only when the original
     * <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline WriteGetObjectResponseRequest& WithChecksumSHA1(Aws::String&& value) { SetChecksumSHA1(std::move(value)); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 160-bit SHA-1 digest of the object returned by the Object Lambda
     * function. This may not match the checksum for the object stored in Amazon S3.
     * Amazon S3 will perform validation of the checksum values only when the original
     * <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline WriteGetObjectResponseRequest& WithChecksumSHA1(const char* value) { SetChecksumSHA1(value); return *this;}


    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline const Aws::String& GetChecksumSHA256() const{ return m_checksumSHA256; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline bool ChecksumSHA256HasBeenSet() const { return m_checksumSHA256HasBeenSet; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline void SetChecksumSHA256(const Aws::String& value) { m_checksumSHA256HasBeenSet = true; m_checksumSHA256 = value; }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline void SetChecksumSHA256(Aws::String&& value) { m_checksumSHA256HasBeenSet = true; m_checksumSHA256 = std::move(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline void SetChecksumSHA256(const char* value) { m_checksumSHA256HasBeenSet = true; m_checksumSHA256.assign(value); }

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline WriteGetObjectResponseRequest& WithChecksumSHA256(const Aws::String& value) { SetChecksumSHA256(value); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline WriteGetObjectResponseRequest& WithChecksumSHA256(Aws::String&& value) { SetChecksumSHA256(std::move(value)); return *this;}

    /**
     * <p>This header can be used as a data integrity check to verify that the data
     * received is the same data that was originally sent. This specifies the
     * base64-encoded, 256-bit SHA-256 digest of the object returned by the Object
     * Lambda function. This may not match the checksum for the object stored in Amazon
     * S3. Amazon S3 will perform validation of the checksum values only when the
     * original <code>GetObject</code> request required checksum validation. For more
     * information about checksums, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html">Checking
     * object integrity</a> in the <i>Amazon S3 User Guide</i>.</p> <p>Only one
     * checksum header can be specified at a time. If you supply multiple checksum
     * headers, this request will fail.</p>
     */
    inline WriteGetObjectResponseRequest& WithChecksumSHA256(const char* value) { SetChecksumSHA256(value); return *this;}


    /**
     * <p>Specifies whether an object stored in Amazon S3 is (<code>true</code>) or is
     * not (<code>false</code>) a delete marker. </p>
     */
    inline bool GetDeleteMarker() const{ return m_deleteMarker; }

    /**
     * <p>Specifies whether an object stored in Amazon S3 is (<code>true</code>) or is
     * not (<code>false</code>) a delete marker. </p>
     */
    inline bool DeleteMarkerHasBeenSet() const { return m_deleteMarkerHasBeenSet; }

    /**
     * <p>Specifies whether an object stored in Amazon S3 is (<code>true</code>) or is
     * not (<code>false</code>) a delete marker. </p>
     */
    inline void SetDeleteMarker(bool value) { m_deleteMarkerHasBeenSet = true; m_deleteMarker = value; }

    /**
     * <p>Specifies whether an object stored in Amazon S3 is (<code>true</code>) or is
     * not (<code>false</code>) a delete marker. </p>
     */
    inline WriteGetObjectResponseRequest& WithDeleteMarker(bool value) { SetDeleteMarker(value); return *this;}


    /**
     * <p>An opaque identifier assigned by a web server to a specific version of a
     * resource found at a URL. </p>
     */
    inline const Aws::String& GetETag() const{ return m_eTag; }

    /**
     * <p>An opaque identifier assigned by a web server to a specific version of a
     * resource found at a URL. </p>
     */
    inline bool ETagHasBeenSet() const { return m_eTagHasBeenSet; }

    /**
     * <p>An opaque identifier assigned by a web server to a specific version of a
     * resource found at a URL. </p>
     */
    inline void SetETag(const Aws::String& value) { m_eTagHasBeenSet = true; m_eTag = value; }

    /**
     * <p>An opaque identifier assigned by a web server to a specific version of a
     * resource found at a URL. </p>
     */
    inline void SetETag(Aws::String&& value) { m_eTagHasBeenSet = true; m_eTag = std::move(value); }

    /**
     * <p>An opaque identifier assigned by a web server to a specific version of a
     * resource found at a URL. </p>
     */
    inline void SetETag(const char* value) { m_eTagHasBeenSet = true; m_eTag.assign(value); }

    /**
     * <p>An opaque identifier assigned by a web server to a specific version of a
     * resource found at a URL. </p>
     */
    inline WriteGetObjectResponseRequest& WithETag(const Aws::String& value) { SetETag(value); return *this;}

    /**
     * <p>An opaque identifier assigned by a web server to a specific version of a
     * resource found at a URL. </p>
     */
    inline WriteGetObjectResponseRequest& WithETag(Aws::String&& value) { SetETag(std::move(value)); return *this;}

    /**
     * <p>An opaque identifier assigned by a web server to a specific version of a
     * resource found at a URL. </p>
     */
    inline WriteGetObjectResponseRequest& WithETag(const char* value) { SetETag(value); return *this;}


    /**
     * <p>The date and time at which the object is no longer cacheable.</p>
     */
    inline const Aws::Utils::DateTime& GetExpires() const{ return m_expires; }

    /**
     * <p>The date and time at which the object is no longer cacheable.</p>
     */
    inline bool ExpiresHasBeenSet() const { return m_expiresHasBeenSet; }

    /**
     * <p>The date and time at which the object is no longer cacheable.</p>
     */
    inline void SetExpires(const Aws::Utils::DateTime& value) { m_expiresHasBeenSet = true; m_expires = value; }

    /**
     * <p>The date and time at which the object is no longer cacheable.</p>
     */
    inline void SetExpires(Aws::Utils::DateTime&& value) { m_expiresHasBeenSet = true; m_expires = std::move(value); }

    /**
     * <p>The date and time at which the object is no longer cacheable.</p>
     */
    inline WriteGetObjectResponseRequest& WithExpires(const Aws::Utils::DateTime& value) { SetExpires(value); return *this;}

    /**
     * <p>The date and time at which the object is no longer cacheable.</p>
     */
    inline WriteGetObjectResponseRequest& WithExpires(Aws::Utils::DateTime&& value) { SetExpires(std::move(value)); return *this;}


    /**
     * <p>If the object expiration is configured (see PUT Bucket lifecycle), the
     * response includes this header. It includes the <code>expiry-date</code> and
     * <code>rule-id</code> key-value pairs that provide the object expiration
     * information. The value of the <code>rule-id</code> is URL-encoded. </p>
     */
    inline const Aws::String& GetExpiration() const{ return m_expiration; }

    /**
     * <p>If the object expiration is configured (see PUT Bucket lifecycle), the
     * response includes this header. It includes the <code>expiry-date</code> and
     * <code>rule-id</code> key-value pairs that provide the object expiration
     * information. The value of the <code>rule-id</code> is URL-encoded. </p>
     */
    inline bool ExpirationHasBeenSet() const { return m_expirationHasBeenSet; }

    /**
     * <p>If the object expiration is configured (see PUT Bucket lifecycle), the
     * response includes this header. It includes the <code>expiry-date</code> and
     * <code>rule-id</code> key-value pairs that provide the object expiration
     * information. The value of the <code>rule-id</code> is URL-encoded. </p>
     */
    inline void SetExpiration(const Aws::String& value) { m_expirationHasBeenSet = true; m_expiration = value; }

    /**
     * <p>If the object expiration is configured (see PUT Bucket lifecycle), the
     * response includes this header. It includes the <code>expiry-date</code> and
     * <code>rule-id</code> key-value pairs that provide the object expiration
     * information. The value of the <code>rule-id</code> is URL-encoded. </p>
     */
    inline void SetExpiration(Aws::String&& value) { m_expirationHasBeenSet = true; m_expiration = std::move(value); }

    /**
     * <p>If the object expiration is configured (see PUT Bucket lifecycle), the
     * response includes this header. It includes the <code>expiry-date</code> and
     * <code>rule-id</code> key-value pairs that provide the object expiration
     * information. The value of the <code>rule-id</code> is URL-encoded. </p>
     */
    inline void SetExpiration(const char* value) { m_expirationHasBeenSet = true; m_expiration.assign(value); }

    /**
     * <p>If the object expiration is configured (see PUT Bucket lifecycle), the
     * response includes this header. It includes the <code>expiry-date</code> and
     * <code>rule-id</code> key-value pairs that provide the object expiration
     * information. The value of the <code>rule-id</code> is URL-encoded. </p>
     */
    inline WriteGetObjectResponseRequest& WithExpiration(const Aws::String& value) { SetExpiration(value); return *this;}

    /**
     * <p>If the object expiration is configured (see PUT Bucket lifecycle), the
     * response includes this header. It includes the <code>expiry-date</code> and
     * <code>rule-id</code> key-value pairs that provide the object expiration
     * information. The value of the <code>rule-id</code> is URL-encoded. </p>
     */
    inline WriteGetObjectResponseRequest& WithExpiration(Aws::String&& value) { SetExpiration(std::move(value)); return *this;}

    /**
     * <p>If the object expiration is configured (see PUT Bucket lifecycle), the
     * response includes this header. It includes the <code>expiry-date</code> and
     * <code>rule-id</code> key-value pairs that provide the object expiration
     * information. The value of the <code>rule-id</code> is URL-encoded. </p>
     */
    inline WriteGetObjectResponseRequest& WithExpiration(const char* value) { SetExpiration(value); return *this;}


    /**
     * <p>The date and time that the object was last modified.</p>
     */
    inline const Aws::Utils::DateTime& GetLastModified() const{ return m_lastModified; }

    /**
     * <p>The date and time that the object was last modified.</p>
     */
    inline bool LastModifiedHasBeenSet() const { return m_lastModifiedHasBeenSet; }

    /**
     * <p>The date and time that the object was last modified.</p>
     */
    inline void SetLastModified(const Aws::Utils::DateTime& value) { m_lastModifiedHasBeenSet = true; m_lastModified = value; }

    /**
     * <p>The date and time that the object was last modified.</p>
     */
    inline void SetLastModified(Aws::Utils::DateTime&& value) { m_lastModifiedHasBeenSet = true; m_lastModified = std::move(value); }

    /**
     * <p>The date and time that the object was last modified.</p>
     */
    inline WriteGetObjectResponseRequest& WithLastModified(const Aws::Utils::DateTime& value) { SetLastModified(value); return *this;}

    /**
     * <p>The date and time that the object was last modified.</p>
     */
    inline WriteGetObjectResponseRequest& WithLastModified(Aws::Utils::DateTime&& value) { SetLastModified(std::move(value)); return *this;}


    /**
     * <p>Set to the number of metadata entries not returned in <code>x-amz-meta</code>
     * headers. This can happen if you create metadata using an API like SOAP that
     * supports more flexible metadata than the REST API. For example, using SOAP, you
     * can create metadata whose values are not legal HTTP headers.</p>
     */
    inline int GetMissingMeta() const{ return m_missingMeta; }

    /**
     * <p>Set to the number of metadata entries not returned in <code>x-amz-meta</code>
     * headers. This can happen if you create metadata using an API like SOAP that
     * supports more flexible metadata than the REST API. For example, using SOAP, you
     * can create metadata whose values are not legal HTTP headers.</p>
     */
    inline bool MissingMetaHasBeenSet() const { return m_missingMetaHasBeenSet; }

    /**
     * <p>Set to the number of metadata entries not returned in <code>x-amz-meta</code>
     * headers. This can happen if you create metadata using an API like SOAP that
     * supports more flexible metadata than the REST API. For example, using SOAP, you
     * can create metadata whose values are not legal HTTP headers.</p>
     */
    inline void SetMissingMeta(int value) { m_missingMetaHasBeenSet = true; m_missingMeta = value; }

    /**
     * <p>Set to the number of metadata entries not returned in <code>x-amz-meta</code>
     * headers. This can happen if you create metadata using an API like SOAP that
     * supports more flexible metadata than the REST API. For example, using SOAP, you
     * can create metadata whose values are not legal HTTP headers.</p>
     */
    inline WriteGetObjectResponseRequest& WithMissingMeta(int value) { SetMissingMeta(value); return *this;}


    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline const Aws::Map<Aws::String, Aws::String>& GetMetadata() const{ return m_metadata; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline bool MetadataHasBeenSet() const { return m_metadataHasBeenSet; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline void SetMetadata(const Aws::Map<Aws::String, Aws::String>& value) { m_metadataHasBeenSet = true; m_metadata = value; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline void SetMetadata(Aws::Map<Aws::String, Aws::String>&& value) { m_metadataHasBeenSet = true; m_metadata = std::move(value); }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline WriteGetObjectResponseRequest& WithMetadata(const Aws::Map<Aws::String, Aws::String>& value) { SetMetadata(value); return *this;}

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline WriteGetObjectResponseRequest& WithMetadata(Aws::Map<Aws::String, Aws::String>&& value) { SetMetadata(std::move(value)); return *this;}

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline WriteGetObjectResponseRequest& AddMetadata(const Aws::String& key, const Aws::String& value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, value); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline WriteGetObjectResponseRequest& AddMetadata(Aws::String&& key, const Aws::String& value) { m_metadataHasBeenSet = true; m_metadata.emplace(std::move(key), value); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline WriteGetObjectResponseRequest& AddMetadata(const Aws::String& key, Aws::String&& value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, std::move(value)); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline WriteGetObjectResponseRequest& AddMetadata(Aws::String&& key, Aws::String&& value) { m_metadataHasBeenSet = true; m_metadata.emplace(std::move(key), std::move(value)); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline WriteGetObjectResponseRequest& AddMetadata(const char* key, Aws::String&& value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, std::move(value)); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline WriteGetObjectResponseRequest& AddMetadata(Aws::String&& key, const char* value) { m_metadataHasBeenSet = true; m_metadata.emplace(std::move(key), value); return *this; }

    /**
     * <p>A map of metadata to store with the object in S3.</p>
     */
    inline WriteGetObjectResponseRequest& AddMetadata(const char* key, const char* value) { m_metadataHasBeenSet = true; m_metadata.emplace(key, value); return *this; }


    /**
     * <p>Indicates whether an object stored in Amazon S3 has Object Lock enabled. For
     * more information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline const ObjectLockMode& GetObjectLockMode() const{ return m_objectLockMode; }

    /**
     * <p>Indicates whether an object stored in Amazon S3 has Object Lock enabled. For
     * more information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline bool ObjectLockModeHasBeenSet() const { return m_objectLockModeHasBeenSet; }

    /**
     * <p>Indicates whether an object stored in Amazon S3 has Object Lock enabled. For
     * more information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline void SetObjectLockMode(const ObjectLockMode& value) { m_objectLockModeHasBeenSet = true; m_objectLockMode = value; }

    /**
     * <p>Indicates whether an object stored in Amazon S3 has Object Lock enabled. For
     * more information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline void SetObjectLockMode(ObjectLockMode&& value) { m_objectLockModeHasBeenSet = true; m_objectLockMode = std::move(value); }

    /**
     * <p>Indicates whether an object stored in Amazon S3 has Object Lock enabled. For
     * more information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline WriteGetObjectResponseRequest& WithObjectLockMode(const ObjectLockMode& value) { SetObjectLockMode(value); return *this;}

    /**
     * <p>Indicates whether an object stored in Amazon S3 has Object Lock enabled. For
     * more information about S3 Object Lock, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html">Object
     * Lock</a>.</p>
     */
    inline WriteGetObjectResponseRequest& WithObjectLockMode(ObjectLockMode&& value) { SetObjectLockMode(std::move(value)); return *this;}


    /**
     * <p>Indicates whether an object stored in Amazon S3 has an active legal hold.</p>
     */
    inline const ObjectLockLegalHoldStatus& GetObjectLockLegalHoldStatus() const{ return m_objectLockLegalHoldStatus; }

    /**
     * <p>Indicates whether an object stored in Amazon S3 has an active legal hold.</p>
     */
    inline bool ObjectLockLegalHoldStatusHasBeenSet() const { return m_objectLockLegalHoldStatusHasBeenSet; }

    /**
     * <p>Indicates whether an object stored in Amazon S3 has an active legal hold.</p>
     */
    inline void SetObjectLockLegalHoldStatus(const ObjectLockLegalHoldStatus& value) { m_objectLockLegalHoldStatusHasBeenSet = true; m_objectLockLegalHoldStatus = value; }

    /**
     * <p>Indicates whether an object stored in Amazon S3 has an active legal hold.</p>
     */
    inline void SetObjectLockLegalHoldStatus(ObjectLockLegalHoldStatus&& value) { m_objectLockLegalHoldStatusHasBeenSet = true; m_objectLockLegalHoldStatus = std::move(value); }

    /**
     * <p>Indicates whether an object stored in Amazon S3 has an active legal hold.</p>
     */
    inline WriteGetObjectResponseRequest& WithObjectLockLegalHoldStatus(const ObjectLockLegalHoldStatus& value) { SetObjectLockLegalHoldStatus(value); return *this;}

    /**
     * <p>Indicates whether an object stored in Amazon S3 has an active legal hold.</p>
     */
    inline WriteGetObjectResponseRequest& WithObjectLockLegalHoldStatus(ObjectLockLegalHoldStatus&& value) { SetObjectLockLegalHoldStatus(std::move(value)); return *this;}


    /**
     * <p>The date and time when Object Lock is configured to expire.</p>
     */
    inline const Aws::Utils::DateTime& GetObjectLockRetainUntilDate() const{ return m_objectLockRetainUntilDate; }

    /**
     * <p>The date and time when Object Lock is configured to expire.</p>
     */
    inline bool ObjectLockRetainUntilDateHasBeenSet() const { return m_objectLockRetainUntilDateHasBeenSet; }

    /**
     * <p>The date and time when Object Lock is configured to expire.</p>
     */
    inline void SetObjectLockRetainUntilDate(const Aws::Utils::DateTime& value) { m_objectLockRetainUntilDateHasBeenSet = true; m_objectLockRetainUntilDate = value; }

    /**
     * <p>The date and time when Object Lock is configured to expire.</p>
     */
    inline void SetObjectLockRetainUntilDate(Aws::Utils::DateTime&& value) { m_objectLockRetainUntilDateHasBeenSet = true; m_objectLockRetainUntilDate = std::move(value); }

    /**
     * <p>The date and time when Object Lock is configured to expire.</p>
     */
    inline WriteGetObjectResponseRequest& WithObjectLockRetainUntilDate(const Aws::Utils::DateTime& value) { SetObjectLockRetainUntilDate(value); return *this;}

    /**
     * <p>The date and time when Object Lock is configured to expire.</p>
     */
    inline WriteGetObjectResponseRequest& WithObjectLockRetainUntilDate(Aws::Utils::DateTime&& value) { SetObjectLockRetainUntilDate(std::move(value)); return *this;}


    /**
     * <p>The count of parts this object has.</p>
     */
    inline int GetPartsCount() const{ return m_partsCount; }

    /**
     * <p>The count of parts this object has.</p>
     */
    inline bool PartsCountHasBeenSet() const { return m_partsCountHasBeenSet; }

    /**
     * <p>The count of parts this object has.</p>
     */
    inline void SetPartsCount(int value) { m_partsCountHasBeenSet = true; m_partsCount = value; }

    /**
     * <p>The count of parts this object has.</p>
     */
    inline WriteGetObjectResponseRequest& WithPartsCount(int value) { SetPartsCount(value); return *this;}


    /**
     * <p>Indicates if request involves bucket that is either a source or destination
     * in a Replication rule. For more information about S3 Replication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html">Replication</a>.</p>
     */
    inline const ReplicationStatus& GetReplicationStatus() const{ return m_replicationStatus; }

    /**
     * <p>Indicates if request involves bucket that is either a source or destination
     * in a Replication rule. For more information about S3 Replication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html">Replication</a>.</p>
     */
    inline bool ReplicationStatusHasBeenSet() const { return m_replicationStatusHasBeenSet; }

    /**
     * <p>Indicates if request involves bucket that is either a source or destination
     * in a Replication rule. For more information about S3 Replication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html">Replication</a>.</p>
     */
    inline void SetReplicationStatus(const ReplicationStatus& value) { m_replicationStatusHasBeenSet = true; m_replicationStatus = value; }

    /**
     * <p>Indicates if request involves bucket that is either a source or destination
     * in a Replication rule. For more information about S3 Replication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html">Replication</a>.</p>
     */
    inline void SetReplicationStatus(ReplicationStatus&& value) { m_replicationStatusHasBeenSet = true; m_replicationStatus = std::move(value); }

    /**
     * <p>Indicates if request involves bucket that is either a source or destination
     * in a Replication rule. For more information about S3 Replication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html">Replication</a>.</p>
     */
    inline WriteGetObjectResponseRequest& WithReplicationStatus(const ReplicationStatus& value) { SetReplicationStatus(value); return *this;}

    /**
     * <p>Indicates if request involves bucket that is either a source or destination
     * in a Replication rule. For more information about S3 Replication, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html">Replication</a>.</p>
     */
    inline WriteGetObjectResponseRequest& WithReplicationStatus(ReplicationStatus&& value) { SetReplicationStatus(std::move(value)); return *this;}


    
    inline const RequestCharged& GetRequestCharged() const{ return m_requestCharged; }

    
    inline bool RequestChargedHasBeenSet() const { return m_requestChargedHasBeenSet; }

    
    inline void SetRequestCharged(const RequestCharged& value) { m_requestChargedHasBeenSet = true; m_requestCharged = value; }

    
    inline void SetRequestCharged(RequestCharged&& value) { m_requestChargedHasBeenSet = true; m_requestCharged = std::move(value); }

    
    inline WriteGetObjectResponseRequest& WithRequestCharged(const RequestCharged& value) { SetRequestCharged(value); return *this;}

    
    inline WriteGetObjectResponseRequest& WithRequestCharged(RequestCharged&& value) { SetRequestCharged(std::move(value)); return *this;}


    /**
     * <p>Provides information about object restoration operation and expiration time
     * of the restored object copy.</p>
     */
    inline const Aws::String& GetRestore() const{ return m_restore; }

    /**
     * <p>Provides information about object restoration operation and expiration time
     * of the restored object copy.</p>
     */
    inline bool RestoreHasBeenSet() const { return m_restoreHasBeenSet; }

    /**
     * <p>Provides information about object restoration operation and expiration time
     * of the restored object copy.</p>
     */
    inline void SetRestore(const Aws::String& value) { m_restoreHasBeenSet = true; m_restore = value; }

    /**
     * <p>Provides information about object restoration operation and expiration time
     * of the restored object copy.</p>
     */
    inline void SetRestore(Aws::String&& value) { m_restoreHasBeenSet = true; m_restore = std::move(value); }

    /**
     * <p>Provides information about object restoration operation and expiration time
     * of the restored object copy.</p>
     */
    inline void SetRestore(const char* value) { m_restoreHasBeenSet = true; m_restore.assign(value); }

    /**
     * <p>Provides information about object restoration operation and expiration time
     * of the restored object copy.</p>
     */
    inline WriteGetObjectResponseRequest& WithRestore(const Aws::String& value) { SetRestore(value); return *this;}

    /**
     * <p>Provides information about object restoration operation and expiration time
     * of the restored object copy.</p>
     */
    inline WriteGetObjectResponseRequest& WithRestore(Aws::String&& value) { SetRestore(std::move(value)); return *this;}

    /**
     * <p>Provides information about object restoration operation and expiration time
     * of the restored object copy.</p>
     */
    inline WriteGetObjectResponseRequest& WithRestore(const char* value) { SetRestore(value); return *this;}


    /**
     * <p> The server-side encryption algorithm used when storing requested object in
     * Amazon S3 (for example, AES256, aws:kms).</p>
     */
    inline const ServerSideEncryption& GetServerSideEncryption() const{ return m_serverSideEncryption; }

    /**
     * <p> The server-side encryption algorithm used when storing requested object in
     * Amazon S3 (for example, AES256, aws:kms).</p>
     */
    inline bool ServerSideEncryptionHasBeenSet() const { return m_serverSideEncryptionHasBeenSet; }

    /**
     * <p> The server-side encryption algorithm used when storing requested object in
     * Amazon S3 (for example, AES256, aws:kms).</p>
     */
    inline void SetServerSideEncryption(const ServerSideEncryption& value) { m_serverSideEncryptionHasBeenSet = true; m_serverSideEncryption = value; }

    /**
     * <p> The server-side encryption algorithm used when storing requested object in
     * Amazon S3 (for example, AES256, aws:kms).</p>
     */
    inline void SetServerSideEncryption(ServerSideEncryption&& value) { m_serverSideEncryptionHasBeenSet = true; m_serverSideEncryption = std::move(value); }

    /**
     * <p> The server-side encryption algorithm used when storing requested object in
     * Amazon S3 (for example, AES256, aws:kms).</p>
     */
    inline WriteGetObjectResponseRequest& WithServerSideEncryption(const ServerSideEncryption& value) { SetServerSideEncryption(value); return *this;}

    /**
     * <p> The server-side encryption algorithm used when storing requested object in
     * Amazon S3 (for example, AES256, aws:kms).</p>
     */
    inline WriteGetObjectResponseRequest& WithServerSideEncryption(ServerSideEncryption&& value) { SetServerSideEncryption(std::move(value)); return *this;}


    /**
     * <p>Encryption algorithm used if server-side encryption with a customer-provided
     * encryption key was specified for object stored in Amazon S3.</p>
     */
    inline const Aws::String& GetSSECustomerAlgorithm() const{ return m_sSECustomerAlgorithm; }

    /**
     * <p>Encryption algorithm used if server-side encryption with a customer-provided
     * encryption key was specified for object stored in Amazon S3.</p>
     */
    inline bool SSECustomerAlgorithmHasBeenSet() const { return m_sSECustomerAlgorithmHasBeenSet; }

    /**
     * <p>Encryption algorithm used if server-side encryption with a customer-provided
     * encryption key was specified for object stored in Amazon S3.</p>
     */
    inline void SetSSECustomerAlgorithm(const Aws::String& value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm = value; }

    /**
     * <p>Encryption algorithm used if server-side encryption with a customer-provided
     * encryption key was specified for object stored in Amazon S3.</p>
     */
    inline void SetSSECustomerAlgorithm(Aws::String&& value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm = std::move(value); }

    /**
     * <p>Encryption algorithm used if server-side encryption with a customer-provided
     * encryption key was specified for object stored in Amazon S3.</p>
     */
    inline void SetSSECustomerAlgorithm(const char* value) { m_sSECustomerAlgorithmHasBeenSet = true; m_sSECustomerAlgorithm.assign(value); }

    /**
     * <p>Encryption algorithm used if server-side encryption with a customer-provided
     * encryption key was specified for object stored in Amazon S3.</p>
     */
    inline WriteGetObjectResponseRequest& WithSSECustomerAlgorithm(const Aws::String& value) { SetSSECustomerAlgorithm(value); return *this;}

    /**
     * <p>Encryption algorithm used if server-side encryption with a customer-provided
     * encryption key was specified for object stored in Amazon S3.</p>
     */
    inline WriteGetObjectResponseRequest& WithSSECustomerAlgorithm(Aws::String&& value) { SetSSECustomerAlgorithm(std::move(value)); return *this;}

    /**
     * <p>Encryption algorithm used if server-side encryption with a customer-provided
     * encryption key was specified for object stored in Amazon S3.</p>
     */
    inline WriteGetObjectResponseRequest& WithSSECustomerAlgorithm(const char* value) { SetSSECustomerAlgorithm(value); return *this;}


    /**
     * <p> If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for stored in Amazon S3 object. </p>
     */
    inline const Aws::String& GetSSEKMSKeyId() const{ return m_sSEKMSKeyId; }

    /**
     * <p> If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for stored in Amazon S3 object. </p>
     */
    inline bool SSEKMSKeyIdHasBeenSet() const { return m_sSEKMSKeyIdHasBeenSet; }

    /**
     * <p> If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for stored in Amazon S3 object. </p>
     */
    inline void SetSSEKMSKeyId(const Aws::String& value) { m_sSEKMSKeyIdHasBeenSet = true; m_sSEKMSKeyId = value; }

    /**
     * <p> If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for stored in Amazon S3 object. </p>
     */
    inline void SetSSEKMSKeyId(Aws::String&& value) { m_sSEKMSKeyIdHasBeenSet = true; m_sSEKMSKeyId = std::move(value); }

    /**
     * <p> If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for stored in Amazon S3 object. </p>
     */
    inline void SetSSEKMSKeyId(const char* value) { m_sSEKMSKeyIdHasBeenSet = true; m_sSEKMSKeyId.assign(value); }

    /**
     * <p> If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for stored in Amazon S3 object. </p>
     */
    inline WriteGetObjectResponseRequest& WithSSEKMSKeyId(const Aws::String& value) { SetSSEKMSKeyId(value); return *this;}

    /**
     * <p> If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for stored in Amazon S3 object. </p>
     */
    inline WriteGetObjectResponseRequest& WithSSEKMSKeyId(Aws::String&& value) { SetSSEKMSKeyId(std::move(value)); return *this;}

    /**
     * <p> If present, specifies the ID of the Amazon Web Services Key Management
     * Service (Amazon Web Services KMS) symmetric customer managed key that was used
     * for stored in Amazon S3 object. </p>
     */
    inline WriteGetObjectResponseRequest& WithSSEKMSKeyId(const char* value) { SetSSEKMSKeyId(value); return *this;}


    /**
     * <p> 128-bit MD5 digest of customer-provided encryption key used in Amazon S3 to
     * encrypt data stored in S3. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html">Protecting
     * data using server-side encryption with customer-provided encryption keys
     * (SSE-C)</a>.</p>
     */
    inline const Aws::String& GetSSECustomerKeyMD5() const{ return m_sSECustomerKeyMD5; }

    /**
     * <p> 128-bit MD5 digest of customer-provided encryption key used in Amazon S3 to
     * encrypt data stored in S3. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html">Protecting
     * data using server-side encryption with customer-provided encryption keys
     * (SSE-C)</a>.</p>
     */
    inline bool SSECustomerKeyMD5HasBeenSet() const { return m_sSECustomerKeyMD5HasBeenSet; }

    /**
     * <p> 128-bit MD5 digest of customer-provided encryption key used in Amazon S3 to
     * encrypt data stored in S3. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html">Protecting
     * data using server-side encryption with customer-provided encryption keys
     * (SSE-C)</a>.</p>
     */
    inline void SetSSECustomerKeyMD5(const Aws::String& value) { m_sSECustomerKeyMD5HasBeenSet = true; m_sSECustomerKeyMD5 = value; }

    /**
     * <p> 128-bit MD5 digest of customer-provided encryption key used in Amazon S3 to
     * encrypt data stored in S3. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html">Protecting
     * data using server-side encryption with customer-provided encryption keys
     * (SSE-C)</a>.</p>
     */
    inline void SetSSECustomerKeyMD5(Aws::String&& value) { m_sSECustomerKeyMD5HasBeenSet = true; m_sSECustomerKeyMD5 = std::move(value); }

    /**
     * <p> 128-bit MD5 digest of customer-provided encryption key used in Amazon S3 to
     * encrypt data stored in S3. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html">Protecting
     * data using server-side encryption with customer-provided encryption keys
     * (SSE-C)</a>.</p>
     */
    inline void SetSSECustomerKeyMD5(const char* value) { m_sSECustomerKeyMD5HasBeenSet = true; m_sSECustomerKeyMD5.assign(value); }

    /**
     * <p> 128-bit MD5 digest of customer-provided encryption key used in Amazon S3 to
     * encrypt data stored in S3. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html">Protecting
     * data using server-side encryption with customer-provided encryption keys
     * (SSE-C)</a>.</p>
     */
    inline WriteGetObjectResponseRequest& WithSSECustomerKeyMD5(const Aws::String& value) { SetSSECustomerKeyMD5(value); return *this;}

    /**
     * <p> 128-bit MD5 digest of customer-provided encryption key used in Amazon S3 to
     * encrypt data stored in S3. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html">Protecting
     * data using server-side encryption with customer-provided encryption keys
     * (SSE-C)</a>.</p>
     */
    inline WriteGetObjectResponseRequest& WithSSECustomerKeyMD5(Aws::String&& value) { SetSSECustomerKeyMD5(std::move(value)); return *this;}

    /**
     * <p> 128-bit MD5 digest of customer-provided encryption key used in Amazon S3 to
     * encrypt data stored in S3. For more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html">Protecting
     * data using server-side encryption with customer-provided encryption keys
     * (SSE-C)</a>.</p>
     */
    inline WriteGetObjectResponseRequest& WithSSECustomerKeyMD5(const char* value) { SetSSECustomerKeyMD5(value); return *this;}


    /**
     * <p>Provides storage class information of the object. Amazon S3 returns this
     * header for all objects except for S3 Standard storage class objects.</p> <p>For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a>.</p>
     */
    inline const StorageClass& GetStorageClass() const{ return m_storageClass; }

    /**
     * <p>Provides storage class information of the object. Amazon S3 returns this
     * header for all objects except for S3 Standard storage class objects.</p> <p>For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a>.</p>
     */
    inline bool StorageClassHasBeenSet() const { return m_storageClassHasBeenSet; }

    /**
     * <p>Provides storage class information of the object. Amazon S3 returns this
     * header for all objects except for S3 Standard storage class objects.</p> <p>For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a>.</p>
     */
    inline void SetStorageClass(const StorageClass& value) { m_storageClassHasBeenSet = true; m_storageClass = value; }

    /**
     * <p>Provides storage class information of the object. Amazon S3 returns this
     * header for all objects except for S3 Standard storage class objects.</p> <p>For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a>.</p>
     */
    inline void SetStorageClass(StorageClass&& value) { m_storageClassHasBeenSet = true; m_storageClass = std::move(value); }

    /**
     * <p>Provides storage class information of the object. Amazon S3 returns this
     * header for all objects except for S3 Standard storage class objects.</p> <p>For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a>.</p>
     */
    inline WriteGetObjectResponseRequest& WithStorageClass(const StorageClass& value) { SetStorageClass(value); return *this;}

    /**
     * <p>Provides storage class information of the object. Amazon S3 returns this
     * header for all objects except for S3 Standard storage class objects.</p> <p>For
     * more information, see <a
     * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html">Storage
     * Classes</a>.</p>
     */
    inline WriteGetObjectResponseRequest& WithStorageClass(StorageClass&& value) { SetStorageClass(std::move(value)); return *this;}


    /**
     * <p>The number of tags, if any, on the object.</p>
     */
    inline int GetTagCount() const{ return m_tagCount; }

    /**
     * <p>The number of tags, if any, on the object.</p>
     */
    inline bool TagCountHasBeenSet() const { return m_tagCountHasBeenSet; }

    /**
     * <p>The number of tags, if any, on the object.</p>
     */
    inline void SetTagCount(int value) { m_tagCountHasBeenSet = true; m_tagCount = value; }

    /**
     * <p>The number of tags, if any, on the object.</p>
     */
    inline WriteGetObjectResponseRequest& WithTagCount(int value) { SetTagCount(value); return *this;}


    /**
     * <p>An ID used to reference a specific version of the object.</p>
     */
    inline const Aws::String& GetVersionId() const{ return m_versionId; }

    /**
     * <p>An ID used to reference a specific version of the object.</p>
     */
    inline bool VersionIdHasBeenSet() const { return m_versionIdHasBeenSet; }

    /**
     * <p>An ID used to reference a specific version of the object.</p>
     */
    inline void SetVersionId(const Aws::String& value) { m_versionIdHasBeenSet = true; m_versionId = value; }

    /**
     * <p>An ID used to reference a specific version of the object.</p>
     */
    inline void SetVersionId(Aws::String&& value) { m_versionIdHasBeenSet = true; m_versionId = std::move(value); }

    /**
     * <p>An ID used to reference a specific version of the object.</p>
     */
    inline void SetVersionId(const char* value) { m_versionIdHasBeenSet = true; m_versionId.assign(value); }

    /**
     * <p>An ID used to reference a specific version of the object.</p>
     */
    inline WriteGetObjectResponseRequest& WithVersionId(const Aws::String& value) { SetVersionId(value); return *this;}

    /**
     * <p>An ID used to reference a specific version of the object.</p>
     */
    inline WriteGetObjectResponseRequest& WithVersionId(Aws::String&& value) { SetVersionId(std::move(value)); return *this;}

    /**
     * <p>An ID used to reference a specific version of the object.</p>
     */
    inline WriteGetObjectResponseRequest& WithVersionId(const char* value) { SetVersionId(value); return *this;}


    /**
     * <p> Indicates whether the object stored in Amazon S3 uses an S3 bucket key for
     * server-side encryption with Amazon Web Services KMS (SSE-KMS).</p>
     */
    inline bool GetBucketKeyEnabled() const{ return m_bucketKeyEnabled; }

    /**
     * <p> Indicates whether the object stored in Amazon S3 uses an S3 bucket key for
     * server-side encryption with Amazon Web Services KMS (SSE-KMS).</p>
     */
    inline bool BucketKeyEnabledHasBeenSet() const { return m_bucketKeyEnabledHasBeenSet; }

    /**
     * <p> Indicates whether the object stored in Amazon S3 uses an S3 bucket key for
     * server-side encryption with Amazon Web Services KMS (SSE-KMS).</p>
     */
    inline void SetBucketKeyEnabled(bool value) { m_bucketKeyEnabledHasBeenSet = true; m_bucketKeyEnabled = value; }

    /**
     * <p> Indicates whether the object stored in Amazon S3 uses an S3 bucket key for
     * server-side encryption with Amazon Web Services KMS (SSE-KMS).</p>
     */
    inline WriteGetObjectResponseRequest& WithBucketKeyEnabled(bool value) { SetBucketKeyEnabled(value); return *this;}


    
    inline const Aws::Map<Aws::String, Aws::String>& GetCustomizedAccessLogTag() const{ return m_customizedAccessLogTag; }

    
    inline bool CustomizedAccessLogTagHasBeenSet() const { return m_customizedAccessLogTagHasBeenSet; }

    
    inline void SetCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = value; }

    
    inline void SetCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag = std::move(value); }

    
    inline WriteGetObjectResponseRequest& WithCustomizedAccessLogTag(const Aws::Map<Aws::String, Aws::String>& value) { SetCustomizedAccessLogTag(value); return *this;}

    
    inline WriteGetObjectResponseRequest& WithCustomizedAccessLogTag(Aws::Map<Aws::String, Aws::String>&& value) { SetCustomizedAccessLogTag(std::move(value)); return *this;}

    
    inline WriteGetObjectResponseRequest& AddCustomizedAccessLogTag(const Aws::String& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

    
    inline WriteGetObjectResponseRequest& AddCustomizedAccessLogTag(Aws::String&& key, const Aws::String& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline WriteGetObjectResponseRequest& AddCustomizedAccessLogTag(const Aws::String& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline WriteGetObjectResponseRequest& AddCustomizedAccessLogTag(Aws::String&& key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), std::move(value)); return *this; }

    
    inline WriteGetObjectResponseRequest& AddCustomizedAccessLogTag(const char* key, Aws::String&& value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, std::move(value)); return *this; }

    
    inline WriteGetObjectResponseRequest& AddCustomizedAccessLogTag(Aws::String&& key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(std::move(key), value); return *this; }

    
    inline WriteGetObjectResponseRequest& AddCustomizedAccessLogTag(const char* key, const char* value) { m_customizedAccessLogTagHasBeenSet = true; m_customizedAccessLogTag.emplace(key, value); return *this; }

  private:

    Aws::String m_requestRoute;
    bool m_requestRouteHasBeenSet = false;

    Aws::String m_requestToken;
    bool m_requestTokenHasBeenSet = false;


    int m_statusCode;
    bool m_statusCodeHasBeenSet = false;

    Aws::String m_errorCode;
    bool m_errorCodeHasBeenSet = false;

    Aws::String m_errorMessage;
    bool m_errorMessageHasBeenSet = false;

    Aws::String m_acceptRanges;
    bool m_acceptRangesHasBeenSet = false;

    Aws::String m_cacheControl;
    bool m_cacheControlHasBeenSet = false;

    Aws::String m_contentDisposition;
    bool m_contentDispositionHasBeenSet = false;

    Aws::String m_contentEncoding;
    bool m_contentEncodingHasBeenSet = false;

    Aws::String m_contentLanguage;
    bool m_contentLanguageHasBeenSet = false;

    long long m_contentLength;
    bool m_contentLengthHasBeenSet = false;

    Aws::String m_contentRange;
    bool m_contentRangeHasBeenSet = false;

    Aws::String m_checksumCRC32;
    bool m_checksumCRC32HasBeenSet = false;

    Aws::String m_checksumCRC32C;
    bool m_checksumCRC32CHasBeenSet = false;

    Aws::String m_checksumSHA1;
    bool m_checksumSHA1HasBeenSet = false;

    Aws::String m_checksumSHA256;
    bool m_checksumSHA256HasBeenSet = false;

    bool m_deleteMarker;
    bool m_deleteMarkerHasBeenSet = false;

    Aws::String m_eTag;
    bool m_eTagHasBeenSet = false;

    Aws::Utils::DateTime m_expires;
    bool m_expiresHasBeenSet = false;

    Aws::String m_expiration;
    bool m_expirationHasBeenSet = false;

    Aws::Utils::DateTime m_lastModified;
    bool m_lastModifiedHasBeenSet = false;

    int m_missingMeta;
    bool m_missingMetaHasBeenSet = false;

    Aws::Map<Aws::String, Aws::String> m_metadata;
    bool m_metadataHasBeenSet = false;

    ObjectLockMode m_objectLockMode;
    bool m_objectLockModeHasBeenSet = false;

    ObjectLockLegalHoldStatus m_objectLockLegalHoldStatus;
    bool m_objectLockLegalHoldStatusHasBeenSet = false;

    Aws::Utils::DateTime m_objectLockRetainUntilDate;
    bool m_objectLockRetainUntilDateHasBeenSet = false;

    int m_partsCount;
    bool m_partsCountHasBeenSet = false;

    ReplicationStatus m_replicationStatus;
    bool m_replicationStatusHasBeenSet = false;

    RequestCharged m_requestCharged;
    bool m_requestChargedHasBeenSet = false;

    Aws::String m_restore;
    bool m_restoreHasBeenSet = false;

    ServerSideEncryption m_serverSideEncryption;
    bool m_serverSideEncryptionHasBeenSet = false;

    Aws::String m_sSECustomerAlgorithm;
    bool m_sSECustomerAlgorithmHasBeenSet = false;

    Aws::String m_sSEKMSKeyId;
    bool m_sSEKMSKeyIdHasBeenSet = false;

    Aws::String m_sSECustomerKeyMD5;
    bool m_sSECustomerKeyMD5HasBeenSet = false;

    StorageClass m_storageClass;
    bool m_storageClassHasBeenSet = false;

    int m_tagCount;
    bool m_tagCountHasBeenSet = false;

    Aws::String m_versionId;
    bool m_versionIdHasBeenSet = false;

    bool m_bucketKeyEnabled;
    bool m_bucketKeyEnabledHasBeenSet = false;

    Aws::Map<Aws::String, Aws::String> m_customizedAccessLogTag;
    bool m_customizedAccessLogTagHasBeenSet = false;
  };

} // namespace Model
} // namespace S3
} // namespace Aws
