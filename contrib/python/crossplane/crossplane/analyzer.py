# -*- coding: utf-8 -*-
from .errors import (
    NgxParserDirectiveUnknownError,
    NgxParserDirectiveContextError,
    NgxParserDirectiveArgumentsError
)

# bit masks for different directive argument styles
NGX_CONF_NOARGS = 0x00000001  # 0 args
NGX_CONF_TAKE1  = 0x00000002  # 1 args
NGX_CONF_TAKE2  = 0x00000004  # 2 args
NGX_CONF_TAKE3  = 0x00000008  # 3 args
NGX_CONF_TAKE4  = 0x00000010  # 4 args
NGX_CONF_TAKE5  = 0x00000020  # 5 args
NGX_CONF_TAKE6  = 0x00000040  # 6 args
NGX_CONF_TAKE7  = 0x00000080  # 7 args
NGX_CONF_BLOCK  = 0x00000100  # followed by block
NGX_CONF_FLAG   = 0x00000200  # 'on' or 'off'
NGX_CONF_ANY    = 0x00000400  # >=0 args
NGX_CONF_1MORE  = 0x00000800  # >=1 args
NGX_CONF_2MORE  = 0x00001000  # >=2 args

# some helpful argument style aliases
NGX_CONF_TAKE12   = (NGX_CONF_TAKE1 | NGX_CONF_TAKE2)
NGX_CONF_TAKE13   = (NGX_CONF_TAKE1 | NGX_CONF_TAKE3)
NGX_CONF_TAKE23   = (NGX_CONF_TAKE2 | NGX_CONF_TAKE3)
NGX_CONF_TAKE34   = (NGX_CONF_TAKE3 | NGX_CONF_TAKE4)
NGX_CONF_TAKE123  = (NGX_CONF_TAKE12 | NGX_CONF_TAKE3)
NGX_CONF_TAKE1234 = (NGX_CONF_TAKE123 | NGX_CONF_TAKE4)

# bit masks for different directive locations
NGX_DIRECT_CONF      = 0x00010000  # main file (not used)
NGX_MAIN_CONF        = 0x00040000  # main context
NGX_EVENT_CONF       = 0x00080000  # events
NGX_MAIL_MAIN_CONF   = 0x00100000  # mail
NGX_MAIL_SRV_CONF    = 0x00200000  # mail > server
NGX_STREAM_MAIN_CONF = 0x00400000  # stream
NGX_STREAM_SRV_CONF  = 0x00800000  # stream > server
NGX_STREAM_UPS_CONF  = 0x01000000  # stream > upstream
NGX_HTTP_MAIN_CONF   = 0x02000000  # http
NGX_HTTP_SRV_CONF    = 0x04000000  # http > server
NGX_HTTP_LOC_CONF    = 0x08000000  # http > location
NGX_HTTP_UPS_CONF    = 0x10000000  # http > upstream
NGX_HTTP_SIF_CONF    = 0x20000000  # http > server > if
NGX_HTTP_LIF_CONF    = 0x40000000  # http > location > if
NGX_HTTP_LMT_CONF    = 0x80000000  # http > location > limit_except

# helpful directive location alias describing "any" context
# doesn't include NGX_HTTP_SIF_CONF, NGX_HTTP_LIF_CONF, or NGX_HTTP_LMT_CONF
NGX_ANY_CONF = (
    NGX_MAIN_CONF | NGX_EVENT_CONF | NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF |
    NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_STREAM_UPS_CONF |
    NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_UPS_CONF
)

"""
DIRECTIVES
~~~~~~~~~~
This dict maps directives to lists of bit masks that define their behavior.

Each bit mask describes these behaviors:
  - how many arguments the directive can take
  - whether or not it is a block directive
  - whether this is a flag (takes one argument that's either "on" or "off")
  - which contexts it's allowed to be in

Since some directives can have different behaviors in different contexts, we
  use lists of bit masks, each describing a valid way to use the directive.

Definitions for directives that're available in the open source version of 
  nginx were taken directively from the source code. In fact, the variable
  names for the bit masks defined above were taken from the nginx source code.

Definitions for directives that're only available for nginx+ were inferred
  from the documentation at http://nginx.org/en/docs/.
"""
DIRECTIVES = {
    'absolute_redirect': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'accept_mutex': [
        NGX_EVENT_CONF | NGX_CONF_FLAG
    ],
    'accept_mutex_delay': [
        NGX_EVENT_CONF | NGX_CONF_TAKE1
    ],
    'access_log': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_HTTP_LMT_CONF | NGX_CONF_1MORE,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_1MORE
    ],
    'add_after_body': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'add_before_body': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'add_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE23
    ],
    'add_trailer': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE23
    ],
    'addition_types': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'aio': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'aio_write': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'alias': [
        NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'allow': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LMT_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ancient_browser': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'ancient_browser_value': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'auth_basic': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LMT_CONF | NGX_CONF_TAKE1
    ],
    'auth_basic_user_file': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LMT_CONF | NGX_CONF_TAKE1
    ],
    'auth_http': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1
    ],
    'auth_http_header': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE2
    ],
    'auth_http_pass_client_cert': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_FLAG
    ],
    'auth_http_timeout': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1
    ],
    'auth_request': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'auth_request_set': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'autoindex': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'autoindex_exact_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'autoindex_format': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'autoindex_localtime': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'break': [
        NGX_HTTP_SRV_CONF | NGX_HTTP_SIF_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_NOARGS
    ],
    'charset': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1
    ],
    'charset_map': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE2
    ],
    'charset_types': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'chunked_transfer_encoding': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'client_body_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'client_body_in_file_only': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'client_body_in_single_buffer': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'client_body_temp_path': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1234
    ],
    'client_body_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'client_header_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'client_header_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'client_max_body_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'connection_pool_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'create_full_put_path': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'daemon': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_FLAG
    ],
    'dav_access': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE123
    ],
    'dav_methods': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'debug_connection': [
        NGX_EVENT_CONF | NGX_CONF_TAKE1
    ],
    'debug_points': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'default_type': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'deny': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LMT_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'directio': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'directio_alignment': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'disable_symlinks': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'empty_gif': [
        NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS
    ],
    'env': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'error_log': [
        NGX_MAIN_CONF | NGX_CONF_1MORE,
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_1MORE,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_1MORE
    ],
    'error_page': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_2MORE
    ],
    'etag': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'events': [
        NGX_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_NOARGS
    ],
    'expires': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE12
    ],
    'fastcgi_bind': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'fastcgi_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_buffering': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_buffers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'fastcgi_busy_buffers_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_cache': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_cache_background_update': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_cache_bypass': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'fastcgi_cache_key': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_cache_lock': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_cache_lock_age': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_cache_lock_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_cache_max_range_offset': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_cache_methods': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'fastcgi_cache_min_uses': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_cache_path': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_2MORE
    ],
    'fastcgi_cache_revalidate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_cache_use_stale': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'fastcgi_cache_valid': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'fastcgi_catch_stderr': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_connect_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_force_ranges': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_hide_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_ignore_client_abort': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_ignore_headers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'fastcgi_index': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_intercept_errors': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_keep_conn': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_limit_rate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_max_temp_file_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_next_upstream': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'fastcgi_next_upstream_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_next_upstream_tries': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_no_cache': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'fastcgi_param': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE23
    ],
    'fastcgi_pass': [
        NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_pass_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_pass_request_body': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_pass_request_headers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_read_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_request_buffering': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_send_lowat': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_send_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_socket_keepalive': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'fastcgi_split_path_info': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_store': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_store_access': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE123
    ],
    'fastcgi_temp_file_write_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_temp_path': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1234
    ],
    'flv': [
        NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS
    ],
    'geo': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE12,
        NGX_STREAM_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE12
    ],
    'geoip_city': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE12,
        NGX_STREAM_MAIN_CONF | NGX_CONF_TAKE12
    ],
    'geoip_country': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE12,
        NGX_STREAM_MAIN_CONF | NGX_CONF_TAKE12
    ],
    'geoip_org': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE12,
        NGX_STREAM_MAIN_CONF | NGX_CONF_TAKE12
    ],
    'geoip_proxy': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'geoip_proxy_recursive': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_FLAG
    ],
    'google_perftools_profiles': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'grpc_bind': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'grpc_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_connect_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_hide_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_ignore_headers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'grpc_intercept_errors': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'grpc_next_upstream': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'grpc_next_upstream_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_next_upstream_tries': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_pass': [
        NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1
    ],
    'grpc_pass_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_read_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_send_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_set_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'grpc_socket_keepalive': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'grpc_ssl_certificate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_ssl_certificate_key': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_ssl_ciphers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_ssl_crl': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_ssl_name': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_ssl_password_file': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_ssl_protocols': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'grpc_ssl_server_name': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'grpc_ssl_session_reuse': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'grpc_ssl_trusted_certificate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'grpc_ssl_verify': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'grpc_ssl_verify_depth': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'gunzip': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'gunzip_buffers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'gzip': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_FLAG
    ],
    'gzip_buffers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'gzip_comp_level': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'gzip_disable': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'gzip_http_version': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'gzip_min_length': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'gzip_proxied': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'gzip_static': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'gzip_types': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'gzip_vary': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'hash': [
        NGX_HTTP_UPS_CONF | NGX_CONF_TAKE12,
        NGX_STREAM_UPS_CONF | NGX_CONF_TAKE12
    ],
    'http': [
        NGX_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_NOARGS
    ],
    'http2_body_preread_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'http2_chunk_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'http2_idle_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'http2_max_concurrent_pushes': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'http2_max_concurrent_streams': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'http2_max_field_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'http2_max_header_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'http2_max_requests': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'http2_push': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'http2_push_preload': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'http2_recv_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'http2_recv_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'if': [
        NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_BLOCK | NGX_CONF_1MORE
    ],
    'if_modified_since': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'ignore_invalid_headers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_FLAG
    ],
    'image_filter': [
        NGX_HTTP_LOC_CONF | NGX_CONF_TAKE123
    ],
    'image_filter_buffer': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'image_filter_interlace': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'image_filter_jpeg_quality': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'image_filter_sharpen': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'image_filter_transparency': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'image_filter_webp_quality': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'imap_auth': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_1MORE
    ],
    'imap_capabilities': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_1MORE
    ],
    'imap_client_buffer': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1
    ],
    'include': [
        NGX_ANY_CONF | NGX_CONF_TAKE1
    ],
    'index': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'internal': [
        NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS
    ],
    'ip_hash': [
        NGX_HTTP_UPS_CONF | NGX_CONF_NOARGS
    ],
    'keepalive': [
        NGX_HTTP_UPS_CONF | NGX_CONF_TAKE1
    ],
    'keepalive_disable': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'keepalive_requests': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_HTTP_UPS_CONF | NGX_CONF_TAKE1
    ],
    'keepalive_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12,
        NGX_HTTP_UPS_CONF | NGX_CONF_TAKE1
    ],
    'large_client_header_buffers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE2
    ],
    'least_conn': [
        NGX_HTTP_UPS_CONF | NGX_CONF_NOARGS,
        NGX_STREAM_UPS_CONF | NGX_CONF_NOARGS
    ],
    'limit_conn': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE2
    ],
    'limit_conn_dry_run': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'limit_conn_log_level': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'limit_conn_status': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'limit_conn_zone': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE2,
        NGX_STREAM_MAIN_CONF | NGX_CONF_TAKE2
    ],
    'limit_except': [
        NGX_HTTP_LOC_CONF | NGX_CONF_BLOCK | NGX_CONF_1MORE
    ],
    'limit_rate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1
    ],
    'limit_rate_after': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1
    ],
    'limit_req': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE123
    ],
    'limit_req_dry_run': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'limit_req_log_level': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'limit_req_status': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'limit_req_zone': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE34
    ],
    'lingering_close': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'lingering_time': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'lingering_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'listen': [
        NGX_HTTP_SRV_CONF | NGX_CONF_1MORE,
        NGX_MAIL_SRV_CONF | NGX_CONF_1MORE,
        NGX_STREAM_SRV_CONF | NGX_CONF_1MORE
    ],
    'load_module': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'location': [
        NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE12
    ],
    'lock_file': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'log_format': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_2MORE,
        NGX_STREAM_MAIN_CONF | NGX_CONF_2MORE
    ],
    'log_not_found': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'log_subrequest': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'mail': [
        NGX_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_NOARGS
    ],
    'map': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE2,
        NGX_STREAM_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE2
    ],
    'map_hash_bucket_size': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'map_hash_max_size': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'master_process': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_FLAG
    ],
    'max_ranges': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'memcached_bind': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'memcached_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'memcached_connect_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'memcached_gzip_flag': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'memcached_next_upstream': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'memcached_next_upstream_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'memcached_next_upstream_tries': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'memcached_pass': [
        NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1
    ],
    'memcached_read_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'memcached_send_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'memcached_socket_keepalive': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'merge_slashes': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_FLAG
    ],
    'min_delete_depth': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'mirror': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'mirror_request_body': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'modern_browser': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'modern_browser_value': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'mp4': [
        NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS
    ],
    'mp4_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'mp4_max_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'msie_padding': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'msie_refresh': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'multi_accept': [
        NGX_EVENT_CONF | NGX_CONF_FLAG
    ],
    'open_file_cache': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'open_file_cache_errors': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'open_file_cache_min_uses': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'open_file_cache_valid': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'open_log_file_cache': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1234,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1234
    ],
    'output_buffers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'override_charset': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_FLAG
    ],
    'pcre_jit': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_FLAG
    ],
    'perl': [
        NGX_HTTP_LOC_CONF | NGX_HTTP_LMT_CONF | NGX_CONF_TAKE1
    ],
    'perl_modules': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'perl_require': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'perl_set': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE2
    ],
    'pid': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'pop3_auth': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_1MORE
    ],
    'pop3_capabilities': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_1MORE
    ],
    'port_in_redirect': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'postpone_output': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'preread_buffer_size': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'preread_timeout': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'protocol': [
        NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_bind': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE12
    ],
    'proxy_buffer': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_buffering': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'proxy_buffers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'proxy_busy_buffers_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_cache': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_cache_background_update': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'proxy_cache_bypass': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'proxy_cache_convert_head': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'proxy_cache_key': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_cache_lock': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'proxy_cache_lock_age': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_cache_lock_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_cache_max_range_offset': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_cache_methods': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'proxy_cache_min_uses': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_cache_path': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_2MORE
    ],
    'proxy_cache_revalidate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'proxy_cache_use_stale': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'proxy_cache_valid': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'proxy_connect_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_cookie_domain': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'proxy_cookie_path': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'proxy_download_rate': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_force_ranges': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'proxy_headers_hash_bucket_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_headers_hash_max_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_hide_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_http_version': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_ignore_client_abort': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'proxy_ignore_headers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'proxy_intercept_errors': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'proxy_limit_rate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_max_temp_file_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_method': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_next_upstream': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'proxy_next_upstream_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_next_upstream_tries': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_no_cache': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'proxy_pass': [
        NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_HTTP_LMT_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_pass_error_message': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_FLAG
    ],
    'proxy_pass_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_pass_request_body': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'proxy_pass_request_headers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'proxy_protocol': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'proxy_protocol_timeout': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_read_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_redirect': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'proxy_request_buffering': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'proxy_requests': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_responses': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_send_lowat': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_send_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_set_body': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_set_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'proxy_socket_keepalive': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'proxy_ssl': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'proxy_ssl_certificate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_ssl_certificate_key': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_ssl_ciphers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_ssl_crl': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_ssl_name': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_ssl_password_file': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_ssl_protocols': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_1MORE
    ],
    'proxy_ssl_server_name': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'proxy_ssl_session_reuse': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'proxy_ssl_trusted_certificate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_ssl_verify': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'proxy_ssl_verify_depth': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_store': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_store_access': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE123
    ],
    'proxy_temp_file_write_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'proxy_temp_path': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1234
    ],
    'proxy_timeout': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'proxy_upload_rate': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'random': [
        NGX_HTTP_UPS_CONF | NGX_CONF_NOARGS | NGX_CONF_TAKE12,
        NGX_STREAM_UPS_CONF | NGX_CONF_NOARGS | NGX_CONF_TAKE12
    ],
    'random_index': [
        NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'read_ahead': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'real_ip_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'real_ip_recursive': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'recursive_error_pages': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'referer_hash_bucket_size': [
        NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'referer_hash_max_size': [
        NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'request_pool_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'reset_timedout_connection': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'resolver': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE,
        NGX_HTTP_UPS_CONF | NGX_CONF_1MORE,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_1MORE,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_1MORE
    ],
    'resolver_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'return': [
        NGX_HTTP_SRV_CONF | NGX_HTTP_SIF_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE12,
        NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'rewrite': [
        NGX_HTTP_SRV_CONF | NGX_HTTP_SIF_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE23
    ],
    'rewrite_log': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_SIF_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_FLAG
    ],
    'root': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1
    ],
    'satisfy': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_bind': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'scgi_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_buffering': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'scgi_buffers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'scgi_busy_buffers_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_cache': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_cache_background_update': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'scgi_cache_bypass': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'scgi_cache_key': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_cache_lock': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'scgi_cache_lock_age': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_cache_lock_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_cache_max_range_offset': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_cache_methods': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'scgi_cache_min_uses': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_cache_path': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_2MORE
    ],
    'scgi_cache_revalidate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'scgi_cache_use_stale': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'scgi_cache_valid': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'scgi_connect_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_force_ranges': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'scgi_hide_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_ignore_client_abort': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'scgi_ignore_headers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'scgi_intercept_errors': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'scgi_limit_rate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_max_temp_file_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_next_upstream': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'scgi_next_upstream_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_next_upstream_tries': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_no_cache': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'scgi_param': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE23
    ],
    'scgi_pass': [
        NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1
    ],
    'scgi_pass_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_pass_request_body': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'scgi_pass_request_headers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'scgi_read_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_request_buffering': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'scgi_send_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_socket_keepalive': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'scgi_store': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_store_access': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE123
    ],
    'scgi_temp_file_write_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'scgi_temp_path': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1234
    ],
    'secure_link': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'secure_link_md5': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'secure_link_secret': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'send_lowat': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'send_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'sendfile': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_FLAG
    ],
    'sendfile_max_chunk': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'server': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_NOARGS,
        NGX_HTTP_UPS_CONF | NGX_CONF_1MORE,
        NGX_MAIL_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_NOARGS,
        NGX_STREAM_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_NOARGS,
        NGX_STREAM_UPS_CONF | NGX_CONF_1MORE
    ],
    'server_name': [
        NGX_HTTP_SRV_CONF | NGX_CONF_1MORE,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1
    ],
    'server_name_in_redirect': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'server_names_hash_bucket_size': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'server_names_hash_max_size': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'server_tokens': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'set': [
        NGX_HTTP_SRV_CONF | NGX_HTTP_SIF_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE2
    ],
    'set_real_ip_from': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'slice': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'smtp_auth': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_1MORE
    ],
    'smtp_capabilities': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_1MORE
    ],
    'smtp_client_buffer': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1
    ],
    'smtp_greeting_delay': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1
    ],
    'source_charset': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1
    ],
    'spdy_chunk_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'spdy_headers_comp': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'split_clients': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE2,
        NGX_STREAM_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE2
    ],
    'ssi': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_FLAG
    ],
    'ssi_last_modified': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'ssi_min_file_chunk': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'ssi_silent_errors': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'ssi_types': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'ssi_value_length': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'ssl': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_FLAG,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_FLAG
    ],
    'ssl_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_certificate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_certificate_key': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_ciphers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_client_certificate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_crl': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_dhparam': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_early_data': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_FLAG
    ],
    'ssl_ecdh_curve': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_engine': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'ssl_handshake_timeout': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_password_file': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_prefer_server_ciphers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_FLAG,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_FLAG,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'ssl_preread': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'ssl_protocols': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_1MORE,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_1MORE,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_1MORE
    ],
    'ssl_session_cache': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE12,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE12,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE12
    ],
    'ssl_session_ticket_key': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_session_tickets': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_FLAG,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_FLAG,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'ssl_session_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_stapling': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_FLAG
    ],
    'ssl_stapling_file': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_stapling_responder': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_stapling_verify': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_FLAG
    ],
    'ssl_trusted_certificate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_verify_client': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'ssl_verify_depth': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'starttls': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1
    ],
    'stream': [
        NGX_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_NOARGS
    ],
    'stub_status': [
        NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS | NGX_CONF_TAKE1
    ],
    'sub_filter': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'sub_filter_last_modified': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'sub_filter_once': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'sub_filter_types': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'subrequest_output_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'tcp_nodelay': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG,
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'tcp_nopush': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'thread_pool': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE23
    ],
    'timeout': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_TAKE1
    ],
    'timer_resolution': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'try_files': [
        NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_2MORE
    ],
    'types': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_BLOCK | NGX_CONF_NOARGS
    ],
    'types_hash_bucket_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'types_hash_max_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'underscores_in_headers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_CONF_FLAG
    ],
    'uninitialized_variable_warn': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_SIF_CONF | NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_FLAG
    ],
    'upstream': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE1
    ],
    'use': [
        NGX_EVENT_CONF | NGX_CONF_TAKE1
    ],
    'user': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE12
    ],
    'userid': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'userid_domain': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'userid_expires': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'userid_mark': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'userid_name': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'userid_p3p': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'userid_path': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'userid_service': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_bind': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'uwsgi_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_buffering': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_buffers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'uwsgi_busy_buffers_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_cache': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_cache_background_update': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_cache_bypass': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'uwsgi_cache_key': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_cache_lock': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_cache_lock_age': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_cache_lock_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_cache_max_range_offset': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_cache_methods': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'uwsgi_cache_min_uses': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_cache_path': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_2MORE
    ],
    'uwsgi_cache_revalidate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_cache_use_stale': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'uwsgi_cache_valid': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'uwsgi_connect_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_force_ranges': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_hide_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_ignore_client_abort': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_ignore_headers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'uwsgi_intercept_errors': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_limit_rate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_max_temp_file_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_modifier1': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_modifier2': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_next_upstream': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'uwsgi_next_upstream_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_next_upstream_tries': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_no_cache': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'uwsgi_param': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE23
    ],
    'uwsgi_pass': [
        NGX_HTTP_LOC_CONF | NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_pass_header': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_pass_request_body': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_pass_request_headers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_read_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_request_buffering': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_send_timeout': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_socket_keepalive': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_ssl_certificate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_ssl_certificate_key': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_ssl_ciphers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_ssl_crl': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_ssl_name': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_ssl_password_file': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_ssl_protocols': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'uwsgi_ssl_server_name': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_ssl_session_reuse': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_ssl_trusted_certificate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_ssl_verify': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'uwsgi_ssl_verify_depth': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_store': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_store_access': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE123
    ],
    'uwsgi_temp_file_write_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'uwsgi_temp_path': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1234
    ],
    'valid_referers': [
        NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'variables_hash_bucket_size': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'variables_hash_max_size': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'worker_aio_requests': [
        NGX_EVENT_CONF | NGX_CONF_TAKE1
    ],
    'worker_connections': [
        NGX_EVENT_CONF | NGX_CONF_TAKE1
    ],
    'worker_cpu_affinity': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_1MORE
    ],
    'worker_priority': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'worker_processes': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'worker_rlimit_core': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'worker_rlimit_nofile': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'worker_shutdown_timeout': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'working_directory': [
        NGX_MAIN_CONF | NGX_DIRECT_CONF | NGX_CONF_TAKE1
    ],
    'xclient': [
        NGX_MAIL_MAIN_CONF | NGX_MAIL_SRV_CONF | NGX_CONF_FLAG
    ],
    'xml_entities': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'xslt_last_modified': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'xslt_param': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'xslt_string_param': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'xslt_stylesheet': [
        NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'xslt_types': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'zone': [
        NGX_HTTP_UPS_CONF | NGX_CONF_TAKE12,
        NGX_STREAM_UPS_CONF | NGX_CONF_TAKE12
    ],

    # nginx+ directives [definitions inferred from docs]
    'api': [
        NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS | NGX_CONF_TAKE1
    ],
    'auth_jwt': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'auth_jwt_claim_set': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_2MORE
    ],
    'auth_jwt_header_set': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_2MORE
    ],
    'auth_jwt_key_file': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'auth_jwt_key_request': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'auth_jwt_leeway': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'f4f': [
        NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS
    ],
    'f4f_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'fastcgi_cache_purge': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'health_check': [
        NGX_HTTP_LOC_CONF | NGX_CONF_ANY,
        NGX_STREAM_SRV_CONF | NGX_CONF_ANY
    ],
    'health_check_timeout': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'hls': [
        NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS
    ],
    'hls_buffers': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE2
    ],
    'hls_forward_args': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'hls_fragment': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'hls_mp4_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'hls_mp4_max_buffer_size': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'js_access': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'js_content': [
        NGX_HTTP_LOC_CONF | NGX_HTTP_LMT_CONF | NGX_CONF_TAKE1
    ],
    'js_filter': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'js_include': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'js_path': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1
    ],
    'js_preread': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'js_set': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE2,
        NGX_STREAM_MAIN_CONF | NGX_CONF_TAKE2
    ],
    'keyval': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE3,
        NGX_STREAM_MAIN_CONF | NGX_CONF_TAKE3,
    ],
    'keyval_zone': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_1MORE,
        NGX_STREAM_MAIN_CONF | NGX_CONF_1MORE,
    ],
    'least_time': [
        NGX_HTTP_UPS_CONF | NGX_CONF_TAKE12,
        NGX_STREAM_UPS_CONF | NGX_CONF_TAKE12
    ],
    'limit_zone': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE3
    ],
    'match': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE1,
        NGX_STREAM_MAIN_CONF | NGX_CONF_BLOCK | NGX_CONF_TAKE1
    ],
    'memcached_force_ranges': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_FLAG
    ],
    'mp4_limit_rate': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'mp4_limit_rate_after': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'ntlm': [
        NGX_HTTP_UPS_CONF | NGX_CONF_NOARGS
    ],
    'proxy_cache_purge': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'queue': [
        NGX_HTTP_UPS_CONF | NGX_CONF_TAKE12
    ],
    'scgi_cache_purge': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'session_log': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1
    ],
    'session_log_format': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_2MORE
    ],
    'session_log_zone': [
        NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE23 | NGX_CONF_TAKE4 | NGX_CONF_TAKE5 | NGX_CONF_TAKE6
    ],
    'state': [
        NGX_HTTP_UPS_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_UPS_CONF | NGX_CONF_TAKE1
    ],
    'status': [
        NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS
    ],
    'status_format': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE12
    ],
    'status_zone': [
        NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
        NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1,
        NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
        NGX_HTTP_LIF_CONF | NGX_CONF_TAKE1
    ],
    'sticky': [
        NGX_HTTP_UPS_CONF | NGX_CONF_1MORE
    ],
    'sticky_cookie_insert': [
        NGX_HTTP_UPS_CONF | NGX_CONF_TAKE1234
    ],
    'upstream_conf': [
        NGX_HTTP_LOC_CONF | NGX_CONF_NOARGS
    ],
    'uwsgi_cache_purge': [
        NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_1MORE
    ],
    'zone_sync': [
        NGX_STREAM_SRV_CONF | NGX_CONF_NOARGS
    ],
    'zone_sync_buffers': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE2
    ],
    'zone_sync_connect_retry_interval': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_connect_timeout': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_interval': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_recv_buffer_size': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_server': [
        NGX_STREAM_SRV_CONF | NGX_CONF_TAKE12
    ],
    'zone_sync_ssl': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'zone_sync_ssl_certificate': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_ssl_certificate_key': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_ssl_ciphers': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_ssl_crl': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_ssl_name': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_ssl_password_file': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_ssl_protocols': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_1MORE
    ],
    'zone_sync_ssl_server_name': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'zone_sync_ssl_trusted_certificate': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_ssl_verify': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_FLAG
    ],
    'zone_sync_ssl_verify_depth': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ],
    'zone_sync_timeout': [
        NGX_STREAM_MAIN_CONF | NGX_STREAM_SRV_CONF | NGX_CONF_TAKE1
    ]
}

# map for getting bitmasks from certain context tuples
CONTEXTS = {
    (): NGX_MAIN_CONF,
    ('events',): NGX_EVENT_CONF,
    ('mail',): NGX_MAIL_MAIN_CONF,
    ('mail', 'server'): NGX_MAIL_SRV_CONF,
    ('stream',): NGX_STREAM_MAIN_CONF,
    ('stream', 'server'): NGX_STREAM_SRV_CONF,
    ('stream', 'upstream'): NGX_STREAM_UPS_CONF,
    ('http',): NGX_HTTP_MAIN_CONF,
    ('http', 'server'): NGX_HTTP_SRV_CONF,
    ('http', 'location'): NGX_HTTP_LOC_CONF,
    ('http', 'upstream'): NGX_HTTP_UPS_CONF,
    ('http', 'server', 'if'): NGX_HTTP_SIF_CONF,
    ('http', 'location', 'if'): NGX_HTTP_LIF_CONF,
    ('http', 'location', 'limit_except'): NGX_HTTP_LMT_CONF
}


def enter_block_ctx(stmt, ctx):
    # don't nest because NGX_HTTP_LOC_CONF just means "location block in http"
    if ctx and ctx[0] == 'http' and stmt['directive'] == 'location':
        return ('http', 'location')

    # no other block contexts can be nested like location so just append it
    return ctx + (stmt['directive'],)


def analyze(fname, stmt, term, ctx=(), strict=False, check_ctx=True,
        check_args=True):

    directive = stmt['directive']
    line = stmt['line']

    # if strict and directive isn't recognized then throw error
    if strict and directive not in DIRECTIVES:
        reason = 'unknown directive "%s"' % directive
        raise NgxParserDirectiveUnknownError(reason, fname, line)

    # if we don't know where this directive is allowed and how
    # many arguments it can take then don't bother analyzing it
    if ctx not in CONTEXTS or directive not in DIRECTIVES:
        return

    args = stmt.get('args') or []
    n_args = len(args)

    masks = DIRECTIVES[directive]

    # if this directive can't be used in this context then throw an error
    if check_ctx:
        masks = [mask for mask in masks if mask & CONTEXTS[ctx]]
        if not masks:
            reason = '"%s" directive is not allowed here' % directive
            raise NgxParserDirectiveContextError(reason, fname, line)

    if not check_args:
        return

    valid_flag = lambda x: x.lower() in ('on', 'off')

    # do this in reverse because we only throw errors at the end if no masks
    # are valid, and typically the first bit mask is what the parser expects
    for mask in reversed(masks):
        # if the directive isn't a block but should be according to the mask
        if mask & NGX_CONF_BLOCK and term != '{':
            reason = 'directive "%s" has no opening "{"'
            continue

        # if the directive is a block but shouldn't be according to the mask
        if not mask & NGX_CONF_BLOCK and term != ';':
            reason = 'directive "%s" is not terminated by ";"'
            continue

        # use mask to check the directive's arguments
        if ((mask >> n_args & 1 and n_args <= 7) or  # NOARGS to TAKE7
            (mask & NGX_CONF_FLAG and n_args == 1 and valid_flag(args[0])) or
            (mask & NGX_CONF_ANY and n_args >= 0) or
            (mask & NGX_CONF_1MORE and n_args >= 1) or
            (mask & NGX_CONF_2MORE and n_args >= 2)):
            return
        elif mask & NGX_CONF_FLAG and n_args == 1 and not valid_flag(args[0]):
            reason = 'invalid value "%s" in "%%s" directive, it must be "on" or "off"' % args[0]
        else:
            reason = 'invalid number of arguments in "%s" directive'

    raise NgxParserDirectiveArgumentsError(reason % directive, fname, line)


def register_external_directives(directives):
    for directive, bitmasks in directives.iteritems():
        if bitmasks:
            DIRECTIVES[directive] = bitmasks
