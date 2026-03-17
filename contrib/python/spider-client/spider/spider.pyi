from typing import Optional, Dict, Any
from spider_types import RequestParamsDict

class Spider:
    api_key: str

    def __init__(self, api_key: Optional[str] = None) -> None: ...
    def api_post(
        self,
        endpoint: str,
        data: dict,
        stream: bool,
        content_type: str = "application/json",
    ) -> Any: ...
    def api_get(
        self, endpoint: str, stream: bool, content_type: str = "application/json"
    ) -> Any: ...
    def api_delete(
        self, endpoint: str, stream: bool, content_type: str = "application/json"
    ) -> Any: ...
    def scrape_url(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> Any: ...
    def crawl_url(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> Any: ...
    def links(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> Any: ...
    def screenshot(
        self,
        url: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> Any: ...
    def search(
        self,
        q: str,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> Any: ...
    def transform(
        self,
        data: Any,
        params: Optional[RequestParamsDict] = None,
        stream: bool = False,
        content_type: str = "application/json",
    ) -> Any: ...
    def get_credits(self) -> Any: ...
    def data_post(
        self, table: str, data: Optional[RequestParamsDict] = None
    ) -> Any: ...
    def data_get(
        self,
        table: str,
        params: Optional[RequestParamsDict] = None,
    ) -> Any: ...
    def _prepare_headers(
        self, content_type: str = "application/json"
    ) -> Dict[str, str]: ...
    def _post_request(
        self, url: str, data: Any, headers: Dict[str, str], stream: bool = False
    ) -> Any: ...
    def _get_request(
        self, url: str, headers: Dict[str, str], stream: bool = False
    ) -> Any: ...
    def _delete_request(
        self, url: str, headers: Dict[str, str], stream: bool = False
    ) -> Any: ...
    def _handle_error(self, response: Any, action: str) -> None: ...
