## AWS C Cal

AWS Crypto Abstraction Layer: Cross-Platform, C99 wrapper for cryptography primitives.

## License

This library is licensed under the Apache 2.0 License.

## Supported Platforms
* Windows (Vista and Later)
* Apple
* Unix (via OpenSSL compatible libcrypto)

## Build Instructions
Since this project builds with CMake, you can build with whichever tool you prefer. Here, we show make for simplicity. You can
use Visual Studio, XCode, or whatever you'd like via the -G option.

```
git clone git@github.com:awslabs/aws-c-common
mkdir aws-c-common-build
cd aws-c-common-build
cmake -DCMAKE_PREFIX_PATH=<install path> -DCMAKE_INSTALL_PREFIX=<install path> ../aws-c-common
make
make test
make install

cd ..
git clone git@github.com:awslabs/aws-c-cal
mkdir aws-c-cal-build
cd aws-c-cal-build
cmake -DCMAKE_PREFIX_PATH=<install path> -DCMAKE_INSTALL_PREFIX=<install path> ../aws-c-cal
make
make test
make install
````

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

