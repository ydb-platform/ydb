## Welcome to the Bindings Contributors

Welcome to the community of bindings contributors! chDB offers a Stable C ABI, which facilitates the development of bindings in various languages. For a C language calling demo, please refer to the examples in the `/examples` directory, such as `chdbDlopen.c`, `chdbSimple.c`, and `chdbStub.c`.

### chDB Language Bindings Status

Here is a table outlining the current status of language bindings for chDB:

| Language | Repository | Status |
| --- | --- | --- |
| Python | [chdb](https://github.com/chdb-io/chdb) | Production Ready |
| Golang | [chdb-go](https://github.com/chdb-io/chdb-go) | General Available |
| Bun | [chdb-bun](https://github.com/chdb-io/chdb-bun) | General Available |
| Node | [chdb-node](https://github.com/chdb-io/chdb-node) | General Available |
| Rust | [chdb-rust](https://github.com/chdb-io/chdb-rust) | In Development (Contributors Needed) |
| Java | n/a | Contributors Needed |
| PHP | n/a | Contributors Needed |
| Ruby | n/a | Contributors Needed |
| R | n/a | Contributors Needed |



### chDB stable ABI

chDB provides several stable ABIs for other language bindings.



The following is the definition of the `local_result` and `local_result_v2` structure:

```c
struct local_result
{
    char * buf;
    size_t len;
    void * _vec; // std::vector<char> *, for freeing
    double elapsed;
    uint64_t rows_read;
    uint64_t bytes_read;
};

struct local_result_v2
{
    char * buf;
    size_t len;
    void * _vec; // std::vector<char> *, for freeing
    double elapsed;
    uint64_t rows_read;
    uint64_t bytes_read;
    char * error_message;
};
```

The following is the definition of the `query_stable`, `free_result` and `query_stable_v2`, `free_result_v2` functions.

```c
// v1 API
struct local_result * query_stable(int argc, char ** argv);
void free_result(struct local_result * result);

// v2 API added `char * error_message`.
struct local_result_v2 * query_stable_v2(int argc, char ** argv);
void free_result_v2(struct local_result_v2 * result);
```

#### Query
`query_stable` and `query_stable_v2` accept the same parameters just like the `clickhouse-local` command line tool. You can check `queryToBuffer` function in [LocalChdb.cpp](programs/local/LocalChdb.cpp) as an example.

The difference is that `query_stable_v2` adds the `char * error_message` field.
You can check if the `error_message` field is `NULL` to determine if an error occurred.

#### Free Result
`free_result` and `free_result_v2` are used to free the `local_result` and `local_result_v2` memory. For GC languages, you can call `free_result` or `free_result_v2` in the destructor of the object.

#### Known Issues

- By chDB v1.2.0, the `query_stable_v2``
 returns nil if the query(eg. CREATE TABLE) successes but returns no data. We will change this behavior in the future.


### Need Help?
If you have already developed bindings for a language not listed above, 

If you are interested in contributing to the development of bindings for a language not listed above, please contact us at:

- Discord: [bindings](https://discord.gg/uUk6AKf7yM)
- Email: auxten@clickhouse.com
- Twitter: [@chdb](https://twitter.com/chdb_io)
