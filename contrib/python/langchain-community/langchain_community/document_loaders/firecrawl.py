import dataclasses
import os
from typing import Any, Iterator, Literal, Optional

from langchain_core.document_loaders import BaseLoader
from langchain_core.documents import Document
from langchain_core.utils import get_from_env


class FireCrawlLoader(BaseLoader):
    """
    FireCrawlLoader document loader integration

    Setup:
        Install ``firecrawl-py``,``langchain_community`` and set environment variable ``FIRECRAWL_API_KEY``.

        .. code-block:: bash

            pip install -U firecrawl-py langchain_community
            export FIRECRAWL_API_KEY="your-api-key"

    Instantiate:
        .. code-block:: python

            from langchain_community.document_loaders import FireCrawlLoader

            loader = FireCrawlLoader(
                url = "https://firecrawl.dev",
                mode = "crawl"
                # other params = ...
            )

    Lazy load:
        .. code-block:: python

            docs = []
            docs_lazy = loader.lazy_load()

            # async variant:
            # docs_lazy = await loader.alazy_load()

            for doc in docs_lazy:
                docs.append(doc)
            print(docs[0].page_content[:100])
            print(docs[0].metadata)

        .. code-block:: python

            Introducing [Smart Crawl!](https://www.firecrawl.dev/smart-crawl)
             Join the waitlist to turn any web
            {'ogUrl': 'https://www.firecrawl.dev/', 'title': 'Home - Firecrawl', 'robots': 'follow, index', 'ogImage': 'https://www.firecrawl.dev/og.png?123', 'ogTitle': 'Firecrawl', 'sitemap': {'lastmod': '2024-08-12T00:28:16.681Z', 'changefreq': 'weekly'}, 'keywords': 'Firecrawl,Markdown,Data,Mendable,Langchain', 'sourceURL': 'https://www.firecrawl.dev/', 'ogSiteName': 'Firecrawl', 'description': 'Firecrawl crawls and converts any website into clean markdown.', 'ogDescription': 'Turn any website into LLM-ready data.', 'pageStatusCode': 200, 'ogLocaleAlternate': []}

    Async load:
        .. code-block:: python

            docs = await loader.aload()
            print(docs[0].page_content[:100])
            print(docs[0].metadata)

        .. code-block:: python

            Introducing [Smart Crawl!](https://www.firecrawl.dev/smart-crawl)
             Join the waitlist to turn any web
            {'ogUrl': 'https://www.firecrawl.dev/', 'title': 'Home - Firecrawl', 'robots': 'follow, index', 'ogImage': 'https://www.firecrawl.dev/og.png?123', 'ogTitle': 'Firecrawl', 'sitemap': {'lastmod': '2024-08-12T00:28:16.681Z', 'changefreq': 'weekly'}, 'keywords': 'Firecrawl,Markdown,Data,Mendable,Langchain', 'sourceURL': 'https://www.firecrawl.dev/', 'ogSiteName': 'Firecrawl', 'description': 'Firecrawl crawls and converts any website into clean markdown.', 'ogDescription': 'Turn any website into LLM-ready data.', 'pageStatusCode': 200, 'ogLocaleAlternate': []}

    """  # noqa: E501

    # No legacy support in v2-only implementation

    def __init__(
        self,
        url: Optional[str] = None,
        *,
        query: Optional[str] = None,
        api_key: Optional[str] = None,
        api_url: Optional[str] = None,
        mode: Literal["crawl", "scrape", "map", "extract", "search"] = "crawl",
        params: Optional[dict] = None,
    ):
        """Initialize with API key and url.

        Args:
            url: The url to be crawled.
            api_key: The Firecrawl API key. If not specified will be read from env var
                FIRECRAWL_API_KEY. Get an API key
            api_url: The Firecrawl API URL. If not specified will be read from env var
                FIRECRAWL_API_URL or defaults to https://api.firecrawl.dev.
            mode: The mode to run the loader in. Default is "crawl".
                 Options include "scrape" (single url),
                 "crawl" (all accessible sub pages),
                 "map" (returns list of links that are semantically related).
                 "extract" (extracts structured data from a page).
                 "search" (search for data across the web).
            params: The parameters to pass to the Firecrawl API.
                Examples include crawlerOptions.
                For more details, visit: https://github.com/mendableai/firecrawl-py
        """

        try:
            from firecrawl import FirecrawlApp
        except ImportError:
            raise ImportError(
                "`firecrawl` package not found, please run `pip install firecrawl-py`"
            )
        if mode not in ("crawl", "scrape", "search", "map", "extract", "search"):
            raise ValueError(
                f"""Invalid mode '{mode}'.
                Allowed: 'crawl', 'scrape', 'search', 'map', 'extract', 'search'."""
            )

        if mode in ("scrape", "crawl", "map", "extract") and not url:
            raise ValueError("Url must be provided for modes other than 'search'")
        if mode == "search" and not (query or (params and params.get("query"))):
            raise ValueError("Query must be provided for search mode")

        api_key = api_key or get_from_env("api_key", "FIRECRAWL_API_KEY")
        # Ensure we never pass None for api_url (v2 client validates as str).
        # Avoid get_from_env to prevent raising.
        resolved_api_url = (
            api_url or os.getenv("FIRECRAWL_API_URL") or "https://api.firecrawl.dev"
        )
        self.firecrawl = FirecrawlApp(api_key=api_key, api_url=resolved_api_url)
        self.url = url or ""
        self.mode = mode
        self.params = params or {}
        if query is not None:
            self.params["query"] = query

    def lazy_load(self) -> Iterator[Document]:
        # Prepare integration tag and filter params per method
        firecrawl_docs: list[Any] = []
        if self.mode == "scrape":
            allowed = {
                "formats",
                "headers",
                "include_tags",
                "exclude_tags",
                "only_main_content",
                "timeout",
                "wait_for",
                "mobile",
                "parsers",
                "actions",
                "location",
                "skip_tls_verification",
                "remove_base64_images",
                "fast_mode",
                "use_mock",
                "block_ads",
                "proxy",
                "max_age",
                "store_in_cache",
            }
            kwargs = {k: v for k, v in self.params.items() if k in allowed}
            kwargs["integration"] = "langchain"
            firecrawl_docs = [self.firecrawl.scrape(self.url, **kwargs)]
        elif self.mode == "crawl":
            if not self.url:
                raise ValueError("URL is required for crawl mode")
            allowed = {
                "prompt",
                "exclude_paths",
                "include_paths",
                "max_discovery_depth",
                "ignore_sitemap",
                "ignore_query_parameters",
                "limit",
                "crawl_entire_domain",
                "allow_external_links",
                "allow_subdomains",
                "delay",
                "max_concurrency",
                "webhook",
                "scrape_options",
                "zero_data_retention",
                "poll_interval",
                "timeout",
            }
            kwargs = {k: v for k, v in self.params.items() if k in allowed}
            kwargs["integration"] = "langchain"
            crawl_response = self.firecrawl.crawl(self.url, **kwargs)
            # Support dict or object with 'data'
            if isinstance(crawl_response, dict):
                data = crawl_response.get("data", [])
                firecrawl_docs = list(data) if isinstance(data, list) else []
            else:
                data = getattr(crawl_response, "data", [])
                firecrawl_docs = list(data) if isinstance(data, list) else []
        elif self.mode == "map":
            if not self.url:
                raise ValueError("URL is required for map mode")
            allowed = {
                "search",
                "include_subdomains",
                "limit",
                "sitemap",
                "timeout",
                "location",
            }
            kwargs = {k: v for k, v in self.params.items() if k in allowed}
            kwargs["integration"] = "langchain"
            map_response = self.firecrawl.map(self.url, **kwargs)
            # Firecrawl v2 (>=4.3.6) returns an object with a `links` array
            # Fallback to legacy list response if needed
            if isinstance(map_response, dict):
                links = map_response.get("links")
                firecrawl_docs = list(links) if isinstance(links, list) else []
            elif hasattr(map_response, "links"):
                links = getattr(map_response, "links")
                firecrawl_docs = list(links) if isinstance(links, list) else []
            else:
                is_list = isinstance(map_response, list)
                firecrawl_docs = list(map_response) if is_list else []
        elif self.mode == "extract":
            if not self.url:
                raise ValueError("URL is required for extract mode")
            allowed = {
                "prompt",
                "schema",
                "system_prompt",
                "allow_external_links",
                "enable_web_search",
                "show_sources",
                "scrape_options",
                "ignore_invalid_urls",
                "poll_interval",
                "timeout",
                "agent",
            }
            kwargs = {k: v for k, v in self.params.items() if k in allowed}
            kwargs["integration"] = "langchain"
            firecrawl_docs = [str(self.firecrawl.extract([self.url], **kwargs))]
        elif self.mode == "search":
            allowed = {
                "sources",
                "categories",
                "limit",
                "tbs",
                "location",
                "ignore_invalid_urls",
                "timeout",
                "scrape_options",
            }
            kwargs = {k: v for k, v in self.params.items() if k in allowed}
            kwargs["integration"] = "langchain"
            search_data = self.firecrawl.search(
                query=self.params.get("query"), **kwargs
            )
            # If SDK already returns a list[dict], use it directly
            if isinstance(search_data, list):
                firecrawl_docs = list(search_data)
            else:
                # Normalize typed SearchData into list of dicts with markdown + metadata
                results: list[dict[str, Any]] = []
                containers = []
                if isinstance(search_data, dict):
                    containers = [
                        search_data.get("web"),
                        search_data.get("news"),
                        search_data.get("images"),
                    ]
                else:
                    containers = [
                        getattr(search_data, "web", None),
                        getattr(search_data, "news", None),
                        getattr(search_data, "images", None),
                    ]
                for kind, items in (
                    ("web", containers[0]),
                    ("news", containers[1]),
                    ("images", containers[2]),
                ):
                    if not items:
                        continue
                    for item in items:
                        url_val = (
                            getattr(item, "url", None)
                            if not isinstance(item, dict)
                            else item.get("url")
                        )
                        title_val = (
                            getattr(item, "title", None)
                            if not isinstance(item, dict)
                            else item.get("title")
                        )
                        desc_val = (
                            getattr(item, "description", None)
                            if not isinstance(item, dict)
                            else item.get("description")
                        )
                        content_val = desc_val or title_val or url_val or ""
                        metadata_val = {
                            k: v
                            for k, v in {
                                "url": url_val,
                                "title": title_val,
                                "category": getattr(item, "category", None)
                                if not isinstance(item, dict)
                                else item.get("category"),
                                "type": kind,
                            }.items()
                            if v is not None
                        }
                        results.append(
                            {"markdown": content_val, "metadata": metadata_val}
                        )
                firecrawl_docs = results
        else:
            raise ValueError(
                f"""Invalid mode '{self.mode}'.
                Allowed: 'crawl', 'scrape', 'map', 'extract', 'search'."""
            )
        for doc in firecrawl_docs:
            if self.mode == "map":
                # Support both legacy string list and v2 link objects
                if isinstance(doc, str):
                    page_content: str = doc
                    meta: dict[str, Any] = {}
                elif isinstance(doc, dict):
                    page_content_value = doc.get("url") or doc.get("href") or ""
                    page_content = (
                        page_content_value
                        if isinstance(page_content_value, str)
                        else str(page_content_value or "")
                    )
                    meta = {
                        k: v
                        for k, v in {
                            "title": doc.get("title"),
                            "description": doc.get("description"),
                        }.items()
                        if v is not None
                    }
                elif hasattr(doc, "url") or hasattr(doc, "title"):
                    page_content_value = getattr(doc, "url", "") or getattr(
                        doc, "href", ""
                    )
                    page_content = (
                        page_content_value
                        if isinstance(page_content_value, str)
                        else str(page_content_value or "")
                    )
                    meta = {}
                    title = getattr(doc, "title", None)
                    description = getattr(doc, "description", None)
                    if title is not None:
                        meta["title"] = title
                    if description is not None:
                        meta["description"] = description
                else:
                    page_content = str(doc)
                    meta = {}
            elif self.mode == "extract":
                page_content = str(doc)
                meta = {}
            elif self.mode == "search":
                # Already normalized to dicts with markdown/metadata above
                if isinstance(doc, dict):
                    markdown_value = doc.get("markdown") or ""
                    page_content = (
                        markdown_value
                        if isinstance(markdown_value, str)
                        else str(markdown_value or "")
                    )
                    metadata_obj = doc.get("metadata", {})
                    meta = metadata_obj if isinstance(metadata_obj, dict) else {}
                else:
                    page_content = str(doc)
                    meta = {}
            else:
                if isinstance(doc, dict):
                    content_value = (
                        doc.get("markdown") or doc.get("html") or doc.get("rawHtml", "")
                    )
                    page_content = (
                        content_value
                        if isinstance(content_value, str)
                        else str(content_value or "")
                    )
                    meta = doc.get("metadata", {})
                else:
                    content_value = (
                        getattr(doc, "markdown", None)
                        or getattr(doc, "html", None)
                        or getattr(doc, "rawHtml", "")
                    )
                    page_content = (
                        content_value
                        if isinstance(content_value, str)
                        else str(content_value or "")
                    )
                    meta = getattr(doc, "metadata", {}) or {}

                # Normalize metadata to plain dict for LangChain Document
                if not isinstance(meta, dict):
                    if hasattr(meta, "model_dump") and callable(meta.model_dump):
                        meta = meta.model_dump()
                    elif dataclasses.is_dataclass(meta):
                        meta = dataclasses.asdict(meta)  # type: ignore[arg-type]
                    elif hasattr(meta, "__dict__"):
                        meta = dict(vars(meta))
                    else:
                        meta = {"value": str(meta)}
            if not page_content:
                continue
            yield Document(
                page_content=page_content,
                metadata=meta,
            )
