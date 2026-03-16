# Spider Cloud Python SDK

The Spider Cloud Python SDK offers a toolkit for straightforward website scraping, crawling at scale, and other utilities like extracting links and taking screenshots, enabling you to collect data formatted for compatibility with language models (LLMs). It features a user-friendly interface for seamless integration with the Spider Cloud API.

## Installation

To install the Spider Cloud Python SDK, you can use pip:

```bash
pip install spider_client
```

## Usage

1. Get an API key from [spider.cloud](https://spider.cloud)
2. Set the API key as an environment variable named `SPIDER_API_KEY` or pass it as a parameter to the `Spider` class.

Here's an example of how to use the SDK:

```python
from spider import Spider

# Initialize the Spider with your API key
app = Spider(api_key='your_api_key')

# Scrape a single URL
url = 'https://spider.cloud'
scraped_data = app.scrape_url(url)

# Crawl a website
crawler_params = {
    'limit': 1,
    'proxy_enabled': True,
    'metadata': False,
    'request': 'http'
}
crawl_result = app.crawl_url(url, params=crawler_params)
```

### Scraping a URL

To scrape data from a single URL:

```python
url = 'https://example.com'
scraped_data = app.scrape_url(url)
```

### Crawling a Website

To automate crawling a website:

```python
url = 'https://example.com'
crawl_params = {
    'limit': 200,
    'request': 'smart_mode'
}
crawl_result = app.crawl_url(url, params=crawl_params)
```

#### Crawl Streaming

Stream crawl the website in chunks to scale.

```python
    def handle_json(json_obj: dict) -> None:
        assert json_obj["url"] is not None

    url = 'https://example.com'
    crawl_params = {
        'limit': 200,
    }
    response = app.crawl_url(
        url,
        params=params,
        stream=True,
        callback=handle_json,
    )
```

### Search

Perform a search for websites to crawl or gather search results:

```python
query = 'a sports website'
crawl_params = {
    'request': 'smart_mode',
    'search_limit': 5,
    'limit': 5,
    'fetch_page_content': True
}
crawl_result = app.search(query, params=crawl_params)
```

### Retrieving Links from a URL(s)

Extract all links from a specified URL:

```python
url = 'https://example.com'
links = app.links(url)
```

### Transform

Transform HTML to markdown or text lightning fast:

```python
data = [ { 'html': '<html><body><h1>Hello world</h1></body></html>' } ]
params = {
    'readability': False,
    'return_format': 'markdown',
}
result = app.transform(data, params=params)
```

### Taking Screenshots of a URL(s)

Capture a screenshot of a given URL:

```python
url = 'https://example.com'
screenshot = app.screenshot(url)
```

### Checking Available Credits

You can check the remaining credits on your account:

```python
credits = app.get_credits()
```

## Streaming

If you need to stream the request use the third param:

```python
url = 'https://example.com'

crawler_params = {
    'limit': 1,
    'proxy_enabled': True,
    'metadata': False,
    'request': 'http'
}

links = app.links(url, crawler_params, True)
```

## Content-Type

The following Content-type headers are supported using the fourth param:

1. `application/json`
1. `text/csv`
1. `application/xml`
1. `application/jsonl`

```python
url = 'https://example.com'

crawler_params = {
    'limit': 1,
    'proxy_enabled': True,
    'metadata': False,
    'request': 'http'
}

# stream json lines back to the client
links = app.crawl(url, crawler_params, True, "application/jsonl")
```

## Error Handling

The SDK handles errors returned by the Spider Cloud API and raises appropriate exceptions. If an error occurs during a request, an exception will be raised with a descriptive error message.

## Contributing

Contributions to the Spider Cloud Python SDK are welcome! If you find any issues or have suggestions for improvements, please open an issue or submit a pull request on the GitHub repository.

## License

The Spider Cloud Python SDK is open-source and released under the [MIT License](https://opensource.org/licenses/MIT).
