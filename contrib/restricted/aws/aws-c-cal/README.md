## AWS C Cal

AWS Crypto Abstraction Layer: Cross-Platform, C99 wrapper for cryptography primitives.

## License

This library is licensed under the Apache 2.0 License.

## Supported Platforms
* Windows (Vista and Later)
* Apple
* Unix (via OpenSSL compatible libcrypto)

## Build Instructions

CMake 3.0+ is required to build.

`<install-path>` must be an absolute path in the following instructions.

#### Linux-Only Dependencies

If you are building on Linux, there are several options for crypto libraries.
Preferred choice is aws-lc, that can be build as follows.

```
git clone git@github.com:awslabs/aws-lc.git
cmake -S aws-lc -B aws-lc/build -DCMAKE_INSTALL_PREFIX=<install-path>
cmake --build aws-lc/build --target install
```

Alternatively, OpenSSL versions 1.0.2 or 1.1.1 or BoringSSL at commit 9939e14
(other commits are not tested and not guaranteed to work) can be used. To build
against OpenSSL or BoringSSL specify -DUSE_OPENSSL=ON. Typical OpenSSL flags can
be used to help project locate artifacts (-DLibCrypto_INCLUDE_DIR and -DLibCrypto_STATIC_LIBRARY)

#### Building aws-c-cal and Remaining Dependencies

```
git clone git@github.com:awslabs/aws-c-common.git
cmake -S aws-c-common -B aws-c-common/build -DCMAKE_INSTALL_PREFIX=<install-path>
cmake --build aws-c-common/build --target install

git clone git@github.com:awslabs/aws-c-cal.git
cmake -S aws-c-cal -B aws-c-cal/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-cal/build --target install
```

## Currently provided algorithms

### Hashes
#### MD5
##### Streaming
````
struct aws_hash *hash = aws_md5_new(allocator);
aws_hash_update(hash, &your_buffer);
aws_hash_finalize(hash, &output_buffer, 0);
aws_hash_destroy(hash);
````

##### One-Shot
````
aws_md5_compute(allocator, &your_buffer, &output_buffer, 0);
````

#### SHA256
##### Streaming
````
struct aws_hash *hash = aws_sha256_new(allocator);
aws_hash_update(hash, &your_buffer);
aws_hash_finalize(hash, &output_buffer, 0);
aws_hash_destroy(hash);
````

##### One-Shot
````
aws_sha256_compute(allocator, &your_buffer, &output_buffer, 0);
````

### HMAC
#### SHA256 HMAC
##### Streaming
````
struct aws_hmac *hmac = aws_sha256_hmac_new(allocator, &secret_buf);
aws_hmac_update(hmac, &your_buffer);
aws_hmac_finalize(hmac, &output_buffer, 0);
aws_hmac_destroy(hmac);
````

##### One-Shot
````
aws_sha256_hmac_compute(allocator, &secret_buf, &your_buffer, &output_buffer, 0);
````

## FAQ
### I want more algorithms, what do I do?
Great! So do we! At a minimum, file an issue letting us know. If you want to file a Pull Request, we'd be happy to review and merge it when it's ready.
### Who should consume this package directly?
Are you writing C directly? Then you should.
Are you using any other programming language? This functionality will be exposed via that language specific crt packages.
### I found a security vulnerability in this package. What do I do?
Due to the fact that this package is specifically performing cryptographic operations, please don't file a public issue. Instead, email aws-sdk-common-runtime@amazon.com, and we'll work with you directly.

