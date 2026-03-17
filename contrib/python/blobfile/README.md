# blobfile

This is a library that provides a Python-like interface for reading local and remote files (only from blob storage), with an API similar to `open()` as well as some of the `os.path` and `shutil` functions.  `blobfile` supports local paths, Google Cloud Storage paths (`gs://<bucket>`), and Azure Blob Storage paths (`az://<account>/<container>` or `https://<account>.blob.core.windows.net/<container>/`).

The main function is `BlobFile`, which lets you open local and remote files that act more or less like local ones.  There are also a few additional functions such as `basename`, `dirname`, and `join`, which mostly do the same thing as their `os.path` namesakes, only they also support GCS paths and ABS paths.

This library is inspired by TensorFlow's [`gfile`](https://www.tensorflow.org/api_docs/python/tf/io/gfile/GFile) but does not have exactly the same interface.

## Installation

```sh
pip install blobfile
```

## Usage

```py
# write a file, then read it back

import blobfile as bf

with bf.BlobFile("gs://my-bucket-name/cats", "wb") as f:
    f.write(b"meow!")

print("exists:", bf.exists("gs://my-bucket-name/cats"))

with bf.BlobFile("gs://my-bucket-name/cats", "rb") as f:
    print("contents:", f.read())
```

There are also some [examples processing many blobs in parallel](docs/parallel_examples.md).

Here are the functions in `blobfile`:

* `BlobFile` - like `open()` but works with remote paths too, data can be streamed to/from the remote file.  It accepts the following arguments:
    * `streaming`:
        * The default for `streaming` is `True` when `mode` is in `"r", "rb"` and `False` when `mode` is in `"w", "wb", "a", "ab"`.
        * `streaming=True`:
            * Reading is done without downloading the entire remote file.
            * Writing is done to the remote file directly, but only in chunks of a few MB in size.  `flush()` will not cause an early write.
            * Appending is not implemented.
        * `streaming=False`:
            * Reading is done by downloading the remote file to a local file during the constructor.
            * Writing is done by uploading the file on `close()` or during destruction.
            * Appending is done by downloading the file during construction and uploading on `close()`.
    * `buffer_size`: number of bytes to buffer, this can potentially make reading more efficient.
    * `cache_dir`: a directory in which to cache files for reading, only valid if `streaming=False` and `mode` is in `"r", "rb"`.   You are responsible for cleaning up the cache directory.
    * `file_size`: size of the file being opened, can be specified directly to avoid checking the file size when opening the file.  While this will avoid a network request, it also means that you may get an error when first reading a file that does not exist rather than when opening it.  Only valid for modes "r" and "rb".  This valid will be ignored for local files.
    * `partial_writes_on_exc`: whether to write partially-written data to the file if an exception is thrown from within the `BlobFile` context. Only valid for writing modes. Currently only affects remote files. The default is True, matching Python's behaviour for local files.

Some are inspired by existing `os.path` and `shutil` functions:

* `copy` - copy a file from one path to another, this will do a remote copy between two remote paths on the same blob storage service
* `exists` - returns `True` if the file or directory exists
* `glob`/`scanglob` - return files matching a glob-style pattern as a generator.  Globs can have [surprising performance characteristics](https://cloud.google.com/storage/docs/gsutil/addlhelp/WildcardNames#efficiency-consideration:-using-wildcards-over-many-objects) when used with blob storage.  Character ranges are not supported in patterns.
* `isdir` - returns `True` if the path is a directory
* `listdir`/`scandir` - list contents of a directory as a generator
* `makedirs` - ensure that a directory and all parent directories exist
* `remove` - remove a file
* `rmdir` - remove an empty directory
* `rmtree` - remove a directory tree
* `stat` - get the size and modification time of a file
* `walk` - walk a directory tree with a generator that yields `(dirpath, dirnames, filenames)` tuples
* `basename` - get the final component of a path
* `dirname` - get the path except for the final component
* `join` - join 2 or more paths together, inserting directory separators between each component

There are a few bonus functions:

* `get_url` - returns a url for a path (usable by an HTTP client without any authentication) along with the expiration for that url (or None)
* `md5` - get the md5 hash for a path, for GCS this is often fast, but for other backends this may be slow.  On Azure, if the md5 of a file is calculated and is missing from the file, the file will be updated with the calculated md5.
* `set_mtime` - set the modified timestamp for a file
* `configure` - set global configuration options for blobfile
    * `log_callback=default_log_fn`: a log callback function `log(msg: string)` to use instead of printing to stdout.  If you use `parallel=True`, you probably want to use a log callback function that is pickleable.
    * `connection_pool_max_size=32`: the max size for each per-host connection pool
    * `max_connection_pool_count=10`: the maximum count of per-host connection pools
    * `azure_write_chunk_size=8 * 2 ** 20`: the size of blocks to write to Azure Storage blobs in bytes, can be set to a maximum of 100MB.  This determines both the unit of request retries as well as the maximum file size, which is `50,000 * azure_write_chunk_size`.
    * `google_write_chunk_size=8 * 2 ** 20`: the size of blocks to write to Google Cloud Storage blobs in bytes, this only determines the unit of request retries.
    * `retry_log_threshold=0`: set a retry count threshold above which to log failures to the log callback function
    * `retry_common_log_threshold=2`: set a retry count threshold above which to log very common failures to the log callback function
    * `connect_timeout=10`: the maximum amount of time (in seconds) to wait for a connection attempt to a server to succeed, set to None to wait forever
    * `read_timeout=30`: the maximum amount of time (in seconds) to wait between consecutive read operations for a response from the server, set to None to wait forever
    * `output_az_paths=True`: output `az://` paths instead of using the `https://` for azure
    * `use_azure_storage_account_key_fallback=False`: fallback to storage account keys for azure containers, having this enabled requires listing your subscriptions and may run into 429 errors if you hit the low azure quotas for subscription listing
    * `get_http_pool=None`: a function that returns a `urllib3.PoolManager` to be used for requests
    * `use_streaming_read=False`: if set to `True`, use a single read per file instead of reading a chunk at a time (not recommended for azure)
    * `use_blind_writes=False`: if set to `True`, skip certain read checks during Azure writes. Defaults to checking if `BLOBFILE_USE_BLIND_WRITES` is set to `1`.
    * `default_buffer_size=io.DEFAULT_BUFFER_SIZE`: the default buffer size to use for reading files (and writing local files)
    * `save_access_token_to_disk=True`: if set to `True` to save access tokens to disk so that other processes can read the access tokens to avoid the small amount of time it usually takes to get a token (if the token is still valid).
    * `multiprocessing_start_method="spawn"`: the start method to use when creating processes for parallel work
* `create_context` - (same arguments as `configure`), creates a new instance of `blobfile` with a custom configuration instead of modifying the global configuration

## Authentication

### Google Cloud Storage

The following methods will be tried in order:

1) Check the environment variable `GOOGLE_APPLICATION_CREDENTIALS` for a path to service account credentials in JSON format.
2) Check for "application default credentials".  To setup application default credentials, run `gcloud auth application-default login`.
3) Check for a GCE metadata server (if running on GCE) and get credentials from that service.

### Azure Blobs

The following methods will be tried in order:

1) If `AZURE_USE_IDENTITY=1` is set, use [DefaultAzureCredential](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python) from the `azure-identity` package to acquire tokens. Note: your application must install the `azure-identity` package; `blobfile` does not specify it as a required dependency.
2) Check the environment variable `AZURE_STORAGE_KEY` for an azure storage account key (these are per-storage account shared keys described in https://docs.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage)
3) Check the environment variable `AZURE_APPLICATION_CREDENTIALS` which should point to JSON credentials for a service principal output by the command `az ad sp create-for-rbac --name <name>`
4) Check the environment variables `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID` corresponding to a service principal described in the previous step but without the JSON file.
4) Check the environment variable `AZURE_STORAGE_CONNECTION_STRING` for an [Azure Storage connection string](https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string)
6) Use credentials from the `az` command line tool if they can be found.

If access using credentials fails, anonymous access will be tried.  `blobfile` supports public access for containers marked as public, but not individual blobs.

## Paths

For Google Cloud Storage and Azure Blobs, directories don't really exist.  These storage systems store the files in a single flat list.  The "/" separators are just part of the filenames and there is no need to call the equivalent of `os.mkdir` on one of these systems.

<!-- As a result, directories can be either "implicit" or "explicit".

* An "implicit" directory would be if the file "a/b" exists, then we would say that the directory "a" exists.  If you delete "a/b", then that directory no longer exists because no file exists with the prefix "a/".
* An "explicit" directory would be if the file "a/" exists.  All other files with the prefix "a/" could be deleted, and the directory "a" would still exist because of this dummy file. -->

To make local behavior consistent with the remote storage systems, missing local directories will be created automatically when opening a file in write mode.

### Local

These are just normal paths for the current machine, e.g. `/root/hello.txt`

### Google Cloud Storage

GCS paths have the format `gs://<bucket>/<blob>`, you cannot perform any operations on `gs://` itself.

### Azure Blobs

Azure Blobs URLs have the format `az://<account>/<container>` or `https://<account>.blob.core.windows.net/<container>/<blob>`.  The highest you can go up the hierarchy is `az://<account>/<container>/`, `blobfile` cannot perform any operations on `az://<account>/`.  The `https://` url is the output format by default, but the `az://` urls are accepted as inputs and you can set `output_az_paths=True` to get `az://` urls as output.

## Errors

* `Error` - base class for library-specific exceptions
* `RequestFailure(Error)` - a request has failed permanently, the status code can be found in the property `response_status:int` and an error code, if available, is in `error:Optional[str]`.
* `RestartableStreamingWriteFailure(RequestFailure)` - a streaming write has failed permanently, which requires restarting from the beginning of the stream.
* `ConcurrentWriteFailure(RequestFailure)` - a write failed because another process was writing to the same file at the same time.
* `VersionMismatch(RequestFailure)` - a write failed because the remote file did not match the version specified by the user.
* The following generic exceptions are raised from some functions to make the behavior similar to the original versions: `FileNotFoundError`, `FileExistsError`, `IsADirectoryError`, `NotADirectoryError`, `OSError`, `ValueError`, `io.UnsupportedOperation`

## Logging

`blobfile` will keep retrying transient errors until they succeed or a permanent error is encountered (which will raise an exception).  In order to make diagnosing stalls easier, `blobfile` will log when retrying requests.

To route those log lines, use `configure(log_callback=<fn>)` to set a callback function which will be called whenever a log line should be printed.  The default callback prints to stdout with the prefix `blobfile:`.

### Using the `logging` module

If you use the python `logging` module, you can have `blobfile` log there:

```py
bf.configure(log_callback=logging.getLogger("blobfile").warning)
```

While `blobfile` does not use the python `logging` module by default, it does use other libraries which use that module.  So if you configure the python `logging` module, you may need to change the settings to adjust logging behavior:

* `urllib3`: `logging.getLogger("urllib3").setLevel(logging.ERROR)`
* `filelock`: `logging.getLogger("filelock").setLevel(logging.ERROR)`

Also, as a tip, make sure to use a format that tells you the name of the logger:

```py
logging.basicConfig(format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", level=logging.WARNING)
```

This will let you see which package is producing log messages.

## Safety

The library should be thread safe and fork safe with the following exceptions:

* A `BlobFile` instance is not thread safe (only 1 thread should own a `BlobFile` instance at a time)
* Calls to `bf.configure()` are not thread-safe and should ideally happen before performing any operations

### Concurrent Writers

Google Cloud Storage supports multiple writers for the same blob and the last one to finish should win.  However, in the event of a large number of simultaneous writers, the service will return 429 or 503 errors and most writers will stall.  In this case, write to different blobs instead.

Azure Blobs doesn't support multiple writers for the same blob.  With the way `BlobFile` is currently configured, the last writer to start writing will win.  Other writers will get a `ConcurrentWriteFailure`.  In addition, all writers could fail if the file size is large and there are enough concurrent writers.  In this case, you can write to a temporary blob (with a random filename), copy it to the final location, and then delete the original.  The copy will be within a container so it should be fast.

## Changes

See [CHANGES](CHANGES.md)

## Contributing

Create [testing buckets](https://github.com/christopher-hesse/blobfile/blob/bb885e72e97ddade675d4493db235b43888a2191/blobfile/_ops_test.py#L30-L36) for each cloud provider with appropriate credentials.

To make a new release:
- Update `CHANGES.md`
- Update the version in `pyproject.toml`
- Update the version in `blobfile/__init__.py`
- `rm -rf build dist`
- `python -m build .`
- `twine upload dist/*`
- Tag the release on Github

## Testing

This will auto-format the code, check the types, and then run the tests:

```sh
python testing/run.py
```

Run a single test:

```sh
python testing/run.py -v -s -k test_windowed_file
```

Modify `testing/run.py` if you only want to do some of these things.  The tests are rather slow, ~7 minutes to run (even though large file tests are disabled) and require accounts with every cloud provider.
