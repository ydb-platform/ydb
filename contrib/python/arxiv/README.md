# arxiv.py
[![PyPI](https://img.shields.io/pypi/v/arxiv)](https://pypi.org/project/arxiv/) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/arxiv) [![GitHub Workflow Status (branch)](https://img.shields.io/github/actions/workflow/status/lukasschwab/arxiv.py/python-package.yml?branch=master)](https://github.com/lukasschwab/arxiv.py/actions?query=branch%3Amaster) [![Full package documentation](https://img.shields.io/badge/docs-hosted-brightgreen)](https://lukasschwab.me/arxiv.py/index.html)

Python wrapper for [the arXiv API](https://arxiv.org/help/api/index).

[arXiv](https://arxiv.org/) is a project by the Cornell University Library that provides open access to 1,000,000+ articles in Physics, Mathematics, Computer Science, Quantitative Biology, Quantitative Finance, and Statistics.

## Usage

Install the package:

```bash
$ pip install arxiv   # Or `uv add arxiv` or similar.
```

In your Python code, include the line:

```python
import arxiv
```

### Examples

> [!TIP]
> [`arxivql`](https://pypi.org/project/arxivql/) may simplify constructing complex query strings.

#### Fetching results

```python
import arxiv

# Construct the default API client.
client = arxiv.Client()

# Search for the 10 most recent articles matching the keyword "quantum."
search = arxiv.Search(
  query = "quantum",
  max_results = 10,
  sort_by = arxiv.SortCriterion.SubmittedDate
)

results = client.results(search)

# `results` is a generator; you can iterate over its elements one by one...
for r in client.results(search):
  print(r.title)
# ...or exhaust it into a list. Careful: this is slow for large results sets.
all_results = list(results)
print([r.title for r in all_results])

# For advanced query syntax documentation, see the arXiv API User Manual:
# https://arxiv.org/help/api/user-manual#query_details
search = arxiv.Search(query = "au:del_maestro AND ti:checkerboard")
first_result = next(client.results(search))
print(first_result)

# Search for the paper with ID "1605.08386v1"
search_by_id = arxiv.Search(id_list=["1605.08386v1"])
# Reuse client to fetch the paper, then print its title.
first_result = next(client.results(search_by_id))
print(first_result.title)
```

#### Fetching results with a custom client

```python
import arxiv

big_slow_client = arxiv.Client(
  page_size = 1000,
  delay_seconds = 10.0,
  num_retries = 5
)

# Prints 1000 titles before needing to make another request.
for result in big_slow_client.results(arxiv.Search(query="quantum")):
  print(result.title)
```

#### Logging

To inspect this package's network behavior and API logic, configure a `DEBUG`-level logger.

```pycon
>>> import logging, arxiv
>>> logging.basicConfig(level=logging.DEBUG)
>>> client = arxiv.Client()
>>> paper = next(client.results(arxiv.Search(id_list=["1605.08386v1"])))
INFO:arxiv.arxiv:Requesting 100 results at offset 0
INFO:arxiv.arxiv:Requesting page (first: False, try: 0): https://export.arxiv.org/api/query?search_query=&id_list=1605.08386v1&sortBy=relevance&sortOrder=descending&start=0&max_results=100
DEBUG:urllib3.connectionpool:Starting new HTTPS connection (1): export.arxiv.org:443
DEBUG:urllib3.connectionpool:https://export.arxiv.org:443 "GET /api/query?search_query=&id_list=1605.08386v1&sortBy=relevance&sortOrder=descending&start=0&max_results=100&user-agent=arxiv.py%2F1.4.8 HTTP/1.1" 200 979
```

## Types 

### Client

A `Client` specifies a reusable strategy for fetching results from arXiv's API. For most use cases the default client should suffice.

Clients configurations specify pagination and retry logic. *Reusing* a client allows successive API calls to use the same connection pool and ensures they abide by the rate limit you set.

### Search

A `Search` specifies a search of arXiv's database. Use `Client.results` to get a generator yielding `Result`s.

### Result

The `Result` objects yielded by `Client.results` include metadata about each paper and helper methods for downloading their content.

The meaning of the underlying raw data is documented in the [arXiv API User Manual: Details of Atom Results Returned](https://arxiv.org/help/api/user-manual#_details_of_atom_results_returned).

`Result` also exposes helper methods for downloading papers: `Result.download_pdf` and `Result.download_source`.

## Development

This project uses [UV](https://astral.sh/uv) for development, while maintaining compatibility with traditional pip installation for end users.

### Development Setup

1. **Install UV** (if you haven't already):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Clone and setup**:
   ```bash
   git clone https://github.com/lukasschwab/arxiv.py
   cd arxiv.py
   make dev-setup
   ```
