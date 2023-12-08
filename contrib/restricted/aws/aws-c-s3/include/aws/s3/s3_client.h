#ifndef AWS_S3_CLIENT_H
#define AWS_S3_CLIENT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/auth/signing_config.h>
#include <aws/io/retry_strategy.h>
#include <aws/s3/s3.h>

struct aws_allocator;

struct aws_http_stream;
struct aws_http_message;
struct aws_http_headers;
struct aws_tls_connection_options;
struct aws_input_stream;

struct aws_s3_client;
struct aws_s3_request;
struct aws_s3_meta_request;
struct aws_s3_meta_request_result;
struct aws_s3_meta_request_resume_token;
struct aws_uri;
struct aws_string;

/**
 * A Meta Request represents a group of generated requests that are being done on behalf of the
 * original request. For example, one large GetObject request can be transformed into a series
 * of ranged GetObject requests that are executed in parallel to improve throughput.
 *
 * The aws_s3_meta_request_type is a hint of transformation to be applied.
 */
enum aws_s3_meta_request_type {

    /**
     * The Default meta request type sends any request to S3 as-is (with no transformation). For example,
     * it can be used to pass a CreateBucket request.
     */
    AWS_S3_META_REQUEST_TYPE_DEFAULT,

    /**
     * The GetObject request will be split into a series of ranged GetObject requests that are
     * executed in parallel to improve throughput, when possible.
     */
    AWS_S3_META_REQUEST_TYPE_GET_OBJECT,

    /**
     * The PutObject request will be split into MultiPart uploads that are executed in parallel
     * to improve throughput, when possible.
     */
    AWS_S3_META_REQUEST_TYPE_PUT_OBJECT,

    /**
     * The CopyObject meta request performs a multi-part copy
     * using multiple S3 UploadPartCopy requests in parallel, or bypasses
     * a CopyObject request to S3 if the object size is not large enough for
     * a multipart upload.
     */
    AWS_S3_META_REQUEST_TYPE_COPY_OBJECT,

    AWS_S3_META_REQUEST_TYPE_MAX,
};

/**
 * Invoked to provide response headers received during execution of the meta request, both for
 * success and error HTTP status codes.
 *
 * Return AWS_OP_SUCCESS to continue processing the request.
 * Return AWS_OP_ERR to indicate failure and cancel the request.
 */
typedef int(aws_s3_meta_request_headers_callback_fn)(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data);

/**
 * Invoked to provide the response body as it is received.
 *
 * Note: If you set `enable_read_backpressure` true on the S3 client,
 * you must maintain the flow-control window.
 * The flow-control window shrinks as you receive body data via this callback.
 * Whenever the flow-control window reaches 0 you will stop downloading data.
 * Use aws_s3_meta_request_increment_read_window() to increment the window and keep data flowing.
 * Maintain a larger window to keep up a high download throughput,
 * parts cannot download in parallel unless the window is large enough to hold multiple parts.
 * Maintain a smaller window to limit the amount of data buffered in memory.
 *
 * If `manual_window_management` is false, you do not need to maintain the flow-control window.
 * No back-pressure is applied and data arrives as fast as possible.
 *
 * Return AWS_OP_SUCCESS to continue processing the request.
 * Return AWS_OP_ERR to indicate failure and cancel the request.
 */
typedef int(aws_s3_meta_request_receive_body_callback_fn)(

    /* The meta request that the callback is being issued for. */
    struct aws_s3_meta_request *meta_request,

    /* The body data for this chunk of the object. */
    const struct aws_byte_cursor *body,

    /* The byte index of the object that this refers to. For example, for an HTTP message that has a range header, the
       first chunk received will have a range_start that matches the range header's range-start.*/
    uint64_t range_start,

    /* User data specified by aws_s3_meta_request_options.*/
    void *user_data);

/**
 * Invoked when the entire meta request execution is complete.
 */
typedef void(aws_s3_meta_request_finish_fn)(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data);

/**
 * Information sent in the meta_request progress callback.
 */
struct aws_s3_meta_request_progress {

    /* Bytes transferred since the previous progress update */
    uint64_t bytes_transferred;

    /* Length of the entire meta request operation */
    uint64_t content_length;
};

/**
 * Invoked to report progress of multi-part upload and copy object requests.
 */
typedef void(aws_s3_meta_request_progress_fn)(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_progress *progress,
    void *user_data);

typedef void(aws_s3_meta_request_shutdown_fn)(void *user_data);

typedef void(aws_s3_client_shutdown_complete_callback_fn)(void *user_data);

enum aws_s3_meta_request_tls_mode {
    AWS_MR_TLS_ENABLED,
    AWS_MR_TLS_DISABLED,
};

enum aws_s3_meta_request_compute_content_md5 {
    AWS_MR_CONTENT_MD5_DISABLED,
    AWS_MR_CONTENT_MD5_ENABLED,
};

enum aws_s3_checksum_algorithm {
    AWS_SCA_NONE = 0,
    AWS_SCA_INIT,
    AWS_SCA_CRC32C = AWS_SCA_INIT,
    AWS_SCA_CRC32,
    AWS_SCA_SHA1,
    AWS_SCA_SHA256,
    AWS_SCA_END = AWS_SCA_SHA256,
};

enum aws_s3_checksum_location {
    AWS_SCL_NONE = 0,
    AWS_SCL_HEADER,
    AWS_SCL_TRAILER,
};

/* Keepalive properties are TCP only.
 * If interval or timeout are zero, then default values are used.
 */
struct aws_s3_tcp_keep_alive_options {

    uint16_t keep_alive_interval_sec;
    uint16_t keep_alive_timeout_sec;

    /* If set, sets the number of keep alive probes allowed to fail before the connection is considered
     * lost. If zero OS defaults are used. On Windows, this option is meaningless until Windows 10 1703.*/
    uint16_t keep_alive_max_failed_probes;
};

/* Options for a new client. */
struct aws_s3_client_config {

    /* When set, this will cap the number of active connections. When 0, the client will determine this value based on
     * throughput_target_gbps. (Recommended) */
    uint32_t max_active_connections_override;

    /* Region that the S3 bucket lives in. */
    struct aws_byte_cursor region;

    /* Client bootstrap used for common staples such as event loop group, host resolver, etc.. s*/
    struct aws_client_bootstrap *client_bootstrap;

    /* How tls should be used while performing the request
     * If this is ENABLED:
     *     If tls_connection_options is not-null, then those tls options will be used
     *     If tls_connection_options is NULL, then default tls options will be used
     * If this is DISABLED:
     *     No tls options will be used, regardless of tls_connection_options value.
     */
    enum aws_s3_meta_request_tls_mode tls_mode;

    /* TLS Options to be used for each connection, if tls_mode is ENABLED. When compiling with BYO_CRYPTO, and tls_mode
     * is ENABLED, this is required. Otherwise, this is optional. */
    struct aws_tls_connection_options *tls_connection_options;

    /* Signing options to be used for each request. Specify NULL to not sign requests. */
    struct aws_signing_config_aws *signing_config;

    /* Size of parts the files will be downloaded or uploaded in. */
    size_t part_size;

    /* If the part size needs to be adjusted for service limits, this is the maximum size it will be adjusted to.. */
    size_t max_part_size;

    /* Throughput target in Gbps that we are trying to reach. */
    double throughput_target_gbps;

    /* Retry strategy to use. If NULL, a default retry strategy will be used. */
    struct aws_retry_strategy *retry_strategy;

    /**
     * TODO: move MD5 config to checksum config.
     * For multi-part upload, content-md5 will be calculated if the AWS_MR_CONTENT_MD5_ENABLED is specified
     *     or initial request has content-md5 header.
     * For single-part upload, keep the content-md5 in the initial request unchanged. */
    enum aws_s3_meta_request_compute_content_md5 compute_content_md5;

    /* Callback and associated user data for when the client has completed its shutdown process. */
    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback;
    void *shutdown_callback_user_data;

    /**
     * Optional.
     * Proxy configuration for http connection.
     * If the connection_type is AWS_HPCT_HTTP_LEGACY, it will be converted to AWS_HPCT_HTTP_TUNNEL if tls_mode is
     * ENABLED. Otherwise, it will be converted to AWS_HPCT_HTTP_FORWARD.
     */
    struct aws_http_proxy_options *proxy_options;

    /**
     * Optional.
     * Configuration for fetching proxy configuration from environment.
     * By Default proxy_ev_settings.aws_http_proxy_env_var_type is set to AWS_HPEV_ENABLE which means read proxy
     * configuration from environment.
     * Only works when proxy_options is not set. If both are set, configuration from proxy_options is used.
     */
    struct proxy_env_var_settings *proxy_ev_settings;

    /**
     * Optional.
     * If set to 0, default value is used.
     */
    uint32_t connect_timeout_ms;

    /**
     * Optional.
     * Set keepalive to periodically transmit messages for detecting a disconnected peer.
     */
    struct aws_s3_tcp_keep_alive_options *tcp_keep_alive_options;

    /**
     * Optional.
     * Configuration options for connection monitoring.
     * If the transfer speed falls below the specified minimum_throughput_bytes_per_second, the operation is aborted.
     * If set to NULL, default values are used.
     */
    struct aws_http_connection_monitoring_options *monitoring_options;

    /**
     * Enable backpressure and prevent response data from downloading faster than you can handle it.
     *
     * If false (default), no backpressure is applied and data will download as fast as possible.
     *
     * If true, each meta request has a flow-control window that shrinks as
     * response body data is downloaded (headers do not affect the window).
     * `initial_read_window` determines the starting size of each meta request's window.
     * You will stop downloading data whenever the flow-control window reaches 0
     * You must call aws_s3_meta_request_increment_read_window() to keep data flowing.
     *
     * WARNING: This feature is experimental.
     * Currently, backpressure is only applied to GetObject requests which are split into multiple parts,
     * and you may still receive some data after the window reaches 0.
     */
    bool enable_read_backpressure;

    /**
     * The starting size of each meta request's flow-control window, in bytes.
     * Ignored unless `enable_read_backpressure` is true.
     */
    size_t initial_read_window;
};

struct aws_s3_checksum_config {

    /**
     * The location of client added checksum header.
     *
     * If AWS_SCL_NONE. No request payload checksum will be add and calculated.
     *
     * If AWS_SCL_HEADER, the checksum will be calculated by client and added related header to the request sent.
     *
     * If AWS_SCL_TRAILER, the payload will be aws_chunked encoded, The checksum will be calculate while reading the
     * payload by client. Related header will be added to the trailer part of the encoded payload. Note the payload of
     * the original request cannot be aws-chunked encoded already. Otherwise, error will be raised.
     */
    enum aws_s3_checksum_location location;

    /**
     * The checksum algorithm used.
     * Must be set if location is not AWS_SCL_NONE. Must be AWS_SCA_NONE if location is AWS_SCL_NONE.
     */
    enum aws_s3_checksum_algorithm checksum_algorithm;

    /**
     * Enable checksum mode header will be attached to get requests, this will tell s3 to send back checksums headers if
     * they exist. Calculate the corresponding checksum on the response bodies. The meta request will finish with a did
     * validate field and set the error code to AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH if the calculated
     * checksum, and checksum found in the response header do not match.
     */
    bool validate_response_checksum;

    /**
     * Optional array of `enum aws_s3_checksum_algorithm`.
     *
     * Ignored when validate_response_checksum is not set.
     * If not set all the algorithms will be selected as default behavior.
     * Owned by the caller.
     *
     * The list of algorithms for user to pick up when validate the checksum. Client will pick up the algorithm from the
     * list with the priority based on performance, and the algorithm sent by server. The priority based on performance
     * is [CRC32C, CRC32, SHA1, SHA256].
     *
     * If the response checksum was validated by client, the result will indicate which algorithm was picked.
     */
    struct aws_array_list *validate_checksum_algorithms;
};

/* Options for a new meta request, ie, file transfer that will be handled by the high performance client. */
struct aws_s3_meta_request_options {
    /* TODO: The meta request options cannot control the request to be split or not. Should consider to add one */

    /* The type of meta request we will be trying to accelerate. */
    enum aws_s3_meta_request_type type;

    /* Signing options to be used for each request created for this meta request.  If NULL, options in the client will
     * be used. If not NULL, these options will override the client options. */
    const struct aws_signing_config_aws *signing_config;

    /* Initial HTTP message that defines what operation we are doing.
     * When uploading a file, you should set `send_filepath` (instead of the message's body-stream)
     * for better performance. */
    struct aws_http_message *message;

    /**
     * Optional.
     * If set, this file is sent as the request body, and the `message` body-stream is ignored.
     * This can give better performance than sending data using the body-stream.
     */
    struct aws_byte_cursor send_filepath;

    /**
     * Optional.
     * if set, the flexible checksum will be performed by client based on the config.
     */
    const struct aws_s3_checksum_config *checksum_config;

    /* User data for all callbacks. */
    void *user_data;

    /**
     * Optional.
     * Invoked to provide response headers received during execution of the meta request.
     * Note: this callback will not be fired for cases when resuming an
     * operation that was already completed (ex. pausing put object after it
     * uploaded all data and then resuming it)
     * See `aws_s3_meta_request_headers_callback_fn`.
     */
    aws_s3_meta_request_headers_callback_fn *headers_callback;

    /**
     * Invoked to provide the response body as it is received.
     * See `aws_s3_meta_request_receive_body_callback_fn`.
     */
    aws_s3_meta_request_receive_body_callback_fn *body_callback;

    /**
     * Invoked when the entire meta request execution is complete.
     * See `aws_s3_meta_request_finish_fn`.
     */
    aws_s3_meta_request_finish_fn *finish_callback;

    /* Callback for when the meta request has completely cleaned up. */
    aws_s3_meta_request_shutdown_fn *shutdown_callback;

    /**
     * Invoked to report progress of the meta request execution.
     * Currently, the progress callback is invoked only for the CopyObject meta request type.
     * TODO: support this callback for all the types of meta requests
     * See `aws_s3_meta_request_progress_fn`
     */
    aws_s3_meta_request_progress_fn *progress_callback;

    /**
     * Optional.
     * Endpoint override for request. Can be used to override scheme and port of
     * the endpoint.
     * There is some overlap between Host header and Endpoint and corner cases
     * are handled as follows:
     * - Only Host header is set - Host is used to construct endpoint. https is
     *   default with corresponding port
     * - Only endpoint is set - Host header is created from endpoint. Port and
     *   Scheme from endpoint is used.
     * - Both Host and Endpoint is set - Host header must match Authority of
     *   Endpoint uri. Port and Scheme from endpoint is used.
     */
    struct aws_uri *endpoint;

    /**
     * Optional.
     * For meta requests that support pause/resume (e.g. PutObject), serialized resume token returned by
     * aws_s3_meta_request_pause() can be provided here.
     * Note: If PutObject request specifies a checksum algorithm, client will calculate checksums while skipping parts
     * from the buffer and compare them them to previously uploaded part checksums.
     */
    struct aws_s3_meta_request_resume_token *resume_token;
};

/* Result details of a meta request.
 *
 * If error_code is AWS_ERROR_SUCCESS, then response_status will match the response_status passed earlier by the header
 * callback and error_response_headers and error_response_body will be NULL.
 *
 * If error_code is equal to AWS_ERROR_S3_INVALID_RESPONSE_STATUS, then error_response_headers, error_response_body, and
 * response_status will be populated by the failed request.
 *
 * For all other error codes, response_status will be 0, and the error_response variables will be NULL.
 */
struct aws_s3_meta_request_result {

    /* HTTP Headers for the failed request that triggered finish of the meta request.  NULL if no request failed. */
    struct aws_http_headers *error_response_headers;

    /* Response body for the failed request that triggered finishing of the meta request.  NUll if no request failed.*/
    struct aws_byte_buf *error_response_body;

    /* Response status of the failed request or of the entire meta request. */
    int response_status;

    /* Only set for GET request.
     * Was the server side checksum compared against a calculated checksum of the response body. This may be false
     * even if validate_get_response_checksum was set because the object was uploaded without a checksum, or was
     * uploaded as a multipart object.
     *
     * If the object to get is multipart object, the part checksum MAY be validated if the part size to get matches the
     * part size uploaded. In that case, if any part mismatch the checksum received, the meta request will failed with
     * checksum mismatch. However, even if the parts checksum were validated, this will NOT be set to true, as the
     * checksum for the whole meta request was NOT validated.
     **/
    bool did_validate;

    /* algorithm used to validate checksum */
    enum aws_s3_checksum_algorithm validation_algorithm;

    /* Final error code of the meta request. */
    int error_code;
};

AWS_EXTERN_C_BEGIN

AWS_S3_API
struct aws_s3_client *aws_s3_client_new(
    struct aws_allocator *allocator,
    const struct aws_s3_client_config *client_config);

/**
 * Add a reference, keeping this object alive.
 * The reference must be released when you are done with it, or it's memory will never be cleaned up.
 * You must not pass in NULL.
 * Always returns the same pointer that was passed in.
 */
AWS_S3_API
struct aws_s3_client *aws_s3_client_acquire(struct aws_s3_client *client);

/**
 * Release a reference.
 * When the reference count drops to 0, this object will be cleaned up.
 * It's OK to pass in NULL (nothing happens).
 * Always returns NULL.
 */
AWS_S3_API
struct aws_s3_client *aws_s3_client_release(struct aws_s3_client *client);

AWS_S3_API
struct aws_s3_meta_request *aws_s3_client_make_meta_request(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

/**
 * Increment the flow-control window, so that response data continues downloading.
 *
 * If the client was created with `enable_read_backpressure` set true,
 * each meta request has a flow-control window that shrinks as response
 * body data is downloaded (headers do not affect the size of the window).
 * The client's `initial_read_window` determines the starting size of each meta request's window.
 * If a meta request's flow-control window reaches 0, no further data will be downloaded.
 * If the `initial_read_window` is 0, the request will not start until the window is incremented.
 * Maintain a larger window to keep up a high download throughput,
 * parts cannot download in parallel unless the window is large enough to hold multiple parts.
 * Maintain a smaller window to limit the amount of data buffered in memory.
 *
 * If `enable_read_backpressure` is false this call will have no effect,
 * no backpressure is being applied and data is being downloaded as fast as possible.
 *
 * WARNING: This feature is experimental.
 * Currently, backpressure is only applied to GetObject requests which are split into multiple parts,
 * and you may still receive some data after the window reaches 0.
 */
AWS_S3_API
void aws_s3_meta_request_increment_read_window(struct aws_s3_meta_request *meta_request, uint64_t bytes);

AWS_S3_API
void aws_s3_meta_request_cancel(struct aws_s3_meta_request *meta_request);

/**
 * Note: pause is currently only supported on upload requests.
 * In order to pause an ongoing upload, call aws_s3_meta_request_pause() that
 * will return resume token. Token can be used to query the state of operation
 * at the pausing time.
 * To resume an upload that was paused, supply resume token in the meta
 * request options structure member aws_s3_meta_request_options.resume_token.
 * The upload can be resumed either from the same client or a different one.
 * Corner cases for resume upload are as follows:
 * - upload is not MPU - fail with AWS_ERROR_UNSUPPORTED_OPERATION
 * - pausing before MPU is created - NULL resume token returned. NULL resume
 *   token is equivalent to restarting upload
 * - pausing in the middle of part transfer - return resume token. scheduling of
 *   new part uploads stops.
 * - pausing after completeMPU started - return resume token. if s3 cannot find
 *   find associated MPU id when resuming with that token and num of parts
 *   uploaded equals to total num parts, then operation is a no op. Otherwise
 *   operation fails.
 * Note: for no op case the call will succeed and finish/shutdown request callbacks will
 *   fire, but on headers callback will not fire.
 * Note: similar to cancel pause does not cancel requests already in flight and
 * and parts might complete after pause is requested.
 * @param meta_request pointer to the aws_s3_meta_request of the upload to be paused
 * @param resume_token resume token
 * @return either AWS_OP_ERR or AWS_OP_SUCCESS
 */
AWS_S3_API
int aws_s3_meta_request_pause(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_resume_token **out_resume_token);

/*
 * Options to construct upload resume token.
 * Note: fields correspond to getters on the token below and it up to the caller
 * to persist those in whichever way they choose.
 */
struct aws_s3_upload_resume_token_options {
    struct aws_byte_cursor upload_id; /* Required */
    size_t part_size;                 /* Required */
    size_t total_num_parts;           /* Required */

    /**
     * Optional.
     *
     * Note: during resume num_parts_uploaded is used for sanity checking against
     * uploads on s3 side.
     * In cases where upload id does not exist (already resumed using this token
     * or pause called after upload completes, etc...) and num_parts_uploaded
     * equals to total num parts, resume will become a noop.
     */
    size_t num_parts_completed;
};

/**
 * Create upload resume token from persisted data.
 * Note: Data required for resume token varies per operation.
 */
AWS_S3_API
struct aws_s3_meta_request_resume_token *aws_s3_meta_request_resume_token_new_upload(
    struct aws_allocator *allocator,
    const struct aws_s3_upload_resume_token_options *options);

/*
 * Increment resume token ref count.
 */
AWS_S3_API
struct aws_s3_meta_request_resume_token *aws_s3_meta_request_resume_token_acquire(
    struct aws_s3_meta_request_resume_token *resume_token);

/*
 * Decrement resume token ref count.
 */
AWS_S3_API
struct aws_s3_meta_request_resume_token *aws_s3_meta_request_resume_token_release(
    struct aws_s3_meta_request_resume_token *resume_token);

/*
 * Type of resume token.
 */
AWS_S3_API
enum aws_s3_meta_request_type aws_s3_meta_request_resume_token_type(
    struct aws_s3_meta_request_resume_token *resume_token);

/*
 * Part size associated with operation.
 */
AWS_S3_API
size_t aws_s3_meta_request_resume_token_part_size(struct aws_s3_meta_request_resume_token *resume_token);

/*
 * Total num parts associated with operation.
 */
AWS_S3_API
size_t aws_s3_meta_request_resume_token_total_num_parts(struct aws_s3_meta_request_resume_token *resume_token);

/*
 * Num parts completed.
 */
AWS_S3_API
size_t aws_s3_meta_request_resume_token_num_parts_completed(struct aws_s3_meta_request_resume_token *resume_token);

/*
 * Upload id associated with operation.
 * Only valid for tokens returned from upload operation. For all other operations
 * this will return empty.
 */
AWS_S3_API
struct aws_byte_cursor aws_s3_meta_request_resume_token_upload_id(
    struct aws_s3_meta_request_resume_token *resume_token);

/**
 * Add a reference, keeping this object alive.
 * The reference must be released when you are done with it, or it's memory will never be cleaned up.
 * You must not pass in NULL.
 * Always returns the same pointer that was passed in.
 */
AWS_S3_API
struct aws_s3_meta_request *aws_s3_meta_request_acquire(struct aws_s3_meta_request *meta_request);

/**
 * Release a reference.
 * When the reference count drops to 0, this object will be cleaned up.
 * It's OK to pass in NULL (nothing happens).
 * Always returns NULL.
 */
AWS_S3_API
struct aws_s3_meta_request *aws_s3_meta_request_release(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_init_default_signing_config(
    struct aws_signing_config_aws *signing_config,
    const struct aws_byte_cursor region,
    struct aws_credentials_provider *credentials_provider);

AWS_EXTERN_C_END

#endif /* AWS_S3_CLIENT_H */
