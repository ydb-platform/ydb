<div align="center">
  <img src="https://raw.githubusercontent.com/zarr-developers/community/main/logos/logo2.png"><br>
</div>

# Zarr

<table>
<tr>
  <td>Latest Release</td>
  <td>
    <a href="https://pypi.org/project/zarr/">
    <img src="https://badge.fury.io/py/zarr.svg" alt="latest release" />
    </a>
  </td>
</tr>
  <td></td>
  <td>
    <a href="https://anaconda.org/anaconda/zarr/">
    <img src="https://anaconda.org/conda-forge/zarr/badges/version.svg" alt="latest release" />
    </a>
</td>
</tr>
<tr>
  <td>Package Status</td>
  <td>
		<a href="https://pypi.org/project/zarr/">
		<img src="https://img.shields.io/pypi/status/zarr.svg" alt="status" />
		</a>
  </td>
</tr>
<tr>
  <td>License</td>
  <td>
    <a href="https://github.com/zarr-developers/zarr-python/blob/main/LICENSE.txt">
    <img src="https://img.shields.io/pypi/l/zarr.svg" alt="license" />
    </a>
</td>
</tr>
<tr>
  <td>Build Status</td>
  <td>
    <a href="https://github.com/zarr-developers/zarr-python/blob/main/.github/workflows/python-package.yml">
    <img src="https://github.com/zarr-developers/zarr-python/actions/workflows/python-package.yml/badge.svg" alt="build status" />
    </a>
  </td>
</tr>
<tr>
  <td>Pre-commit Status</td>
  <td>
    <a href=""https://github.com/zarr-developers/zarr-python/blob/main/.pre-commit-config.yaml">
    <img src="https://results.pre-commit.ci/badge/github/zarr-developers/zarr-python/main.svg" alt="pre-commit status" />
    </a>
  </td>
</tr>

<tr>
  <td>Coverage</td>
  <td>
    <a href="https://codecov.io/gh/zarr-developers/zarr-python">
    <img src="https://codecov.io/gh/zarr-developers/zarr-python/branch/main/graph/badge.svg"/ alt="coverage">
    </a>
  </td>
</tr>
<tr>
  <td>Downloads</td>
  <td>
    <a href="https://zarr.readthedocs.io">
    <img src="https://pepy.tech/badge/zarr" alt="pypi downloads" />
    </a>
  </td>
</tr>
<tr>
	<td>Developer Chat</td>
	<td>
		<a href="https://ossci.zulipchat.com/">
		<img src="https://img.shields.io/badge/zulip-join_chat-brightgreen.svg" />
		</a>
	</td>
</tr>
<tr>
	<td>Funding</td>
	<td>
		<a href="https://chanzuckerberg.com/eoss/">
			<img src="https://img.shields.io/badge/funded%20by-EOSS-FF414B.svg?logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSI0NSIgaGVpZ2h0PSI0NSI+PHBhdGggZD0iTTIyLjA5OCAyMS4wNzRjLTEuMjM1Ljg2LTIuNjggMS4zOTktNC42ODggMS40MzgtMi40MjYuMDUtNC4yMzgtMS41NTEtNC40MDYtMy45MThhNC40NjUgNC40NjUgMCAwIDEgMS4xODctMy4zOCA0LjQ4IDQuNDggMCAwIDEgMy4yODYtMS40NDQgNC42NzQgNC42NzQgMCAwIDEgMS44OTQuMzc1cy0uMTU2IDEuMzItLjIxIDEuOTE0bDEuOTg3LjAxNS4zNTYtMi45NzYtLjU2My0uMzU2YTYuNTQxIDYuNTQxIDAgMCAwLTMuNDg0LS45NjkgNi41OTMgNi41OTMgMCAwIDAtNC43NSAyLjA4NiA2LjQxNSA2LjQxNSAwIDAgMC0xLjcxNSA0Ljg3NWMuMTEzIDEuNjEuODA5IDMuMDc4IDEuOTUgNC4xNDEgMS4xNTYgMS4wNyAyLjcxNCAxLjY0NSA0LjM5OCAxLjYzMy4wMzkgMCAuMDc4IDAgLjEyNS0uMDA0YTkuOTE4IDkuOTE4IDAgMCAwIDMuNDMzLS43MjNsMS40MzQtMi43ODlzLS4wMjctLjA2Mi0uMjM0LjA3OCIgc3R5bGU9InN0cm9rZTpub25lO2ZpbGwtcnVsZTpub256ZXJvO2ZpbGw6I2ZmZjtmaWxsLW9wYWNpdHk6MSIvPjxwYXRoIGQ9Im0yOC44NjMgMjguMjE1LS4yNDIgMi43NDJ2LjAzMWMtLjEwMSAxLjAzNS0uNjUyIDEuOTE0LTEuNDg0IDIuMzUyLS42Ni4zNDctMS4zOC4zNDctMi4wMjgtLjAwOC0uNjc1LS4zNjMtLjk5Mi0uOTE4LTEuMTE3LTEuNjEzLS4xODctMS4xMDIuNDM4LTIuMjU0IDEuMjkzLTIuODMybDEzLjM1Mi04LjY4OGMuMDk3LjczOC4xNTIgMS40ODUuMTUyIDIuMjUgMCA4Ljk5Mi03LjMwOSAxNi4zMDUtMTYuMjkzIDE2LjMwNS04Ljk4OCAwLTE2LjI4OS03LjMxMy0xNi4yODktMTYuMzA1IDAtOC45ODggNy4zMDUtMTYuMyAxNi4yOTMtMTYuM3MxMi40OTYgNC4wOSAxNC45ODQgOS45MUwzOS4xMTQgMTVDMzYuMjYxIDguNjY0IDI5Ljg5MyA0LjIzNCAyMi41IDQuMjM0Yy03LjM5NSAwLTE4LjIxNSA4LjE3Mi0xOC4yMTUgMTguMjE1IDAgMTAuMDQ3IDguMTcyIDE4LjIxMSAxOC4yMTEgMTguMjExIDEwLjA0IDAgMTguMjE1LTguMTcyIDE4LjIxNS0xOC4yMSAwLTEwLjA0LS4xMS0yLjI5NC0uMzEzLTMuMzkxYTE5Ljk5NyAxOS45OTcgMCAwIDAtLjQ1My0xLjgzMmwtMy43OTMgMi4zNy01LjAyIDMuMThjLS4xNzUtLjY2OC0uNTgxLTEuMzQzLTEuNDQtMS43My0xLjAxMi0uNDY1LTIuNjYtLjExLTMuODY4LjU4MiAwIDAgMy4zMDUtNi40MDIgMy44Ni03LjQ1LjAzNS0uMDY2LS4wMTYtLjE0OC0uMDk4LS4xNDhoLTYuMThsLS4yNSAyLjA2N0gyNi41MDhsLTQuNzMgOS4wNjJjLS4wOTQuMTc2LjA5Ny4zNjMuMjc3LjI3NGwyLjUyMy0xLjM0YzEuMjE5LS42MjUgMy4yNS0xLjkxIDQuMjU0LTEuMTY4LjE0NS4xMS4zMzIuMzk4LjM0OC42OTVhLjM1Ny4zNTcgMCAwIDEtLjE0OS4yOTNsLTUuMDQzIDMuNDA2Yy0xLjYzNiAxLjE2OC0yLjI3NyAyLjkxLTIuMTIgNC41NTUuMTI0IDEuMzc1Ljk4NCAyLjU2NiAyLjI1NyAzLjI2MmE0LjE3IDQuMTcgMCAwIDAgMi4xMTMuNTE1IDQuMTY5IDQuMTY5IDAgMCAwIDEuODY0LS41YzEuNDM3LS43NTQgMi4zOTQtMi4yMzQgMi41NjItMy45NDVsLjQwMi00LjYxMy0yLjE5OSAxLjYzM1ptMCAwIiBzdHlsZT0ic3Ryb2tlOm5vbmU7ZmlsbC1ydWxlOm5vbnplcm87ZmlsbDojZmZmO2ZpbGwtb3BhY2l0eToxIi8+PC9zdmc+" alt="CZI's Essential Open Source Software for Science">
		</a>
	</td>
</tr>
	<td>Citation</td>
	<td>
		<a href="https://doi.org/10.5281/zenodo.3773450">
			<img src="https://zenodo.org/badge/DOI/10.5281/zenodo.3773450.svg" alt="DOI">
		</a>
	</td>
</tr>

</table>

## What is it?

Zarr is a Python package providing an implementation of compressed, chunked, N-dimensional arrays, designed for use in parallel computing. See the [documentation](https://zarr.readthedocs.io) for more information.

## Main Features

- [**Create**](https://zarr.readthedocs.io/en/stable/user-guide/arrays.html#creating-an-array) N-dimensional arrays with any NumPy `dtype`.
- [**Chunk arrays**](https://zarr.readthedocs.io/en/stable/user-guide/performance.html#chunk-optimizations) along any dimension.
- [**Compress**](https://zarr.readthedocs.io/en/stable/user-guide/arrays.html#compressors) and/or filter chunks using any NumCodecs codec.
- [**Store arrays**](https://zarr.readthedocs.io/en/stable/user-guide/storage.html) in memory, on disk, inside a zip file, on S3, etc...
- [**Read**](https://zarr.readthedocs.io/en/stable/user-guide/arrays.html#reading-and-writing-data) an array [**concurrently**](https://zarr.readthedocs.io/en/stable/user-guide/performance.html#parallel-computing-and-synchronization) from multiple threads or processes.
- [**Write**](https://zarr.readthedocs.io/en/stable/user-guide/arrays.html#reading-and-writing-data) to an array concurrently from multiple threads or processes.
- Organize arrays into hierarchies via [**groups**](https://zarr.readthedocs.io/en/stable/quickstart.html#hierarchical-groups).

## Where to get it

Zarr can be installed from PyPI using `pip`:

```bash
pip install zarr
```

or via `conda`:

```bash
conda install -c conda-forge zarr
```

For more details, including how to install from source, see the [installation documentation](https://zarr.readthedocs.io/en/stable/index.html#installation).
