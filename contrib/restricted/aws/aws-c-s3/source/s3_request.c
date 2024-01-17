#include "aws/s3/private/s3_request.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include <aws/auth/signable.h>
#include <aws/io/stream.h>

static void s_s3_request_destroy(void *user_data);

struct aws_s3_request *aws_s3_request_new(
    struct aws_s3_meta_request *meta_request,
    int request_tag,
    uint32_t part_number,
    uint32_t flags) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->allocator);

    struct aws_s3_request *request = aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_s3_request));

    aws_ref_count_init(&request->ref_count, request, (aws_simple_completion_callback *)s_s3_request_destroy);

    request->allocator = meta_request->allocator;
    request->meta_request = aws_s3_meta_request_acquire(meta_request);

    request->request_tag = request_tag;
    request->part_number = part_number;
    request->record_response_headers = (flags & AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS) != 0;
    request->part_size_response_body = (flags & AWS_S3_REQUEST_FLAG_PART_SIZE_RESPONSE_BODY) != 0;
    request->always_send = (flags & AWS_S3_REQUEST_FLAG_ALWAYS_SEND) != 0;

    return request;
}

void aws_s3_request_setup_send_data(struct aws_s3_request *request, struct aws_http_message *message) {
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(message);

    aws_s3_request_clean_up_send_data(request);

    request->send_data.message = message;
    aws_http_message_acquire(message);
}

static void s_s3_request_clean_up_send_data_message(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    struct aws_http_message *message = request->send_data.message;

    if (message == NULL) {
        return;
    }

    request->send_data.message = NULL;
    aws_http_message_release(message);
}

void aws_s3_request_clean_up_send_data(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    s_s3_request_clean_up_send_data_message(request);

    aws_signable_destroy(request->send_data.signable);
    request->send_data.signable = NULL;

    aws_http_headers_release(request->send_data.response_headers);
    request->send_data.response_headers = NULL;

    aws_byte_buf_clean_up(&request->send_data.response_body);

    AWS_ZERO_STRUCT(request->send_data);
}

void aws_s3_request_acquire(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    aws_ref_count_acquire(&request->ref_count);
}

void aws_s3_request_release(struct aws_s3_request *request) {
    if (request == NULL) {
        return;
    }

    aws_ref_count_release(&request->ref_count);
}

static void s_s3_request_destroy(void *user_data) {
    struct aws_s3_request *request = user_data;

    if (request == NULL) {
        return;
    }

    aws_s3_request_clean_up_send_data(request);
    aws_byte_buf_clean_up(&request->request_body);
    aws_s3_meta_request_release(request->meta_request);

    aws_mem_release(request->allocator, request);
}
