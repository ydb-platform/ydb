from .docs import docs
from .request import (
    request_schema,
    use_kwargs,  # for backward compatibility
    match_info_schema,  # request_schema with locations=["match_info"]
    querystring_schema,  # request_schema with locations=["querystring"]
    form_schema,  # request_schema with locations=["form"]
    json_schema,  # request_schema with locations=["json"]
    headers_schema,  # request_schema with locations=["headers"]
    cookies_schema,  # request_schema with locations=["cookies"]
)
from .response import (
    response_schema,
    marshal_with,  # for backward compatibility
)
