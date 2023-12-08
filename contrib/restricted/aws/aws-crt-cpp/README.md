## AWS Crt Cpp

C++ wrapper around the aws-c-* libraries. Provides Cross-Platform Transport Protocols and SSL/TLS implementations for C++.

### Documentation

https://awslabs.github.io/aws-crt-cpp/

### Currently Included:

* aws-c-common: Cross-platform primitives and data structures.
* aws-c-io: Cross-platform event-loops, non-blocking I/O, and TLS implementations.
* aws-c-mqtt: MQTT client.
* aws-c-auth: Auth signers such as Aws-auth sigv4
* aws-c-http: HTTP 1.1 client, and websockets (H2 coming soon)
* aws-checksums: Cross-Platform HW accelerated CRC32c and CRC32 with fallback to efficient SW implementations.
* aws-c-event-stream: C99 implementation of the vnd.amazon.event-stream content-type.

More protocols and utilities are coming soon, so stay tuned.

## Building

The C99 libraries are already included for your convenience as submodules.
You should perform a recursive clone `git clone --recursive` or initialize the submodules via
`git submodule update --init`. These dependencies are compiled by CMake as part of the build process.

If you want to manage these dependencies manually (e.g. you're using them in other projects), configure CMake with
`-DBUILD_DEPS=OFF` and `-DCMAKE_PREFIX_PATH=<install>` pointing to the absolute path where you have them installed.

### MSVC
If you want to use a statically linked MSVCRT (/MT, /MTd), you can add `-DSTATIC_CRT=ON` to your cmake configuration.

### Apple Silicon (aka M1) and Universal Binaries

aws-crt-cpp supports both `arm64` and `x86_64` architectures.
Configure cmake with `-DCMAKE_OSX_ARCHITECTURES=arm64` to target Apple silicon,
or `-DCMAKE_OSX_ARCHITECTURES=x86_64` to target Intel.
If you wish to create a [universal binary](https://developer.apple.com/documentation/apple-silicon/building-a-universal-macos-binary),
you should use `lipo` to combine the `x86_64` and `arm64` binaries.
For example: `lipo -create -output universal_app x86_app arm_app`

You SHOULD NOT build for both architectures simultaneously via `-DCMAKE_OSX_ARCHITECTURES="arm64;x86_64"`.
aws-crt-cpp has not been tested in this configuration.
aws-crt-cpp's cmake configuration scripts are known to get confused by this,
and will not enable optimizations that would benefit an independent `arm64` or `x86_64` build.

### OpenSSL and LibCrypto (Unix only)

If your application uses OpenSSL, configure with `-DUSE_OPENSSL=ON`.

aws-crt-cpp does not use OpenSSL for TLS.
On Apple and Windows devices, the OS's default TLS library is used.
On Unix devices, [s2n-tls](https://github.com/aws/s2n-tls) is used.
But s2n-tls uses libcrypto, the cryptography math library bundled with OpenSSL.
To simplify the build process, the source code for s2n-tls and libcrypto are
included as git submodules and built along with aws-crt-cpp.
But if your application is also loading the system installation of OpenSSL
(i.e. your application uses libcurl which uses libssl which uses libcrypto)
there may be crashes as the application tries to use two different versions of libcrypto at once.

Setting `-DUSE_OPENSSL=ON` will cause aws-crt-cpp to link against your system's existing `libcrypto`,
instead of building its own copy.

You can ignore all this on Windows and Apple platforms, where aws-crt-cpp uses the OS's default libraries for TLS and cryptography math.

## Dependencies?

There are no non-OS dependencies that AWS does not own, maintain, and ship.

## Common Usage

To do anything with IO, you'll need to create a few objects that will be used by the rest of the library.

For example:

````
    Aws::Crt::LoadErrorStrings();
````

Will load error strings for debugging purposes. Since the C libraries use error codes, this will allow you to print the corresponding
error string for each error code.

````
    Aws::Crt::ApiHandle apiHandle;
````
This performs one-time static initialization of the library. You'll need it to do anything, so don't forget to create one.

````
    Aws::Crt::Io::EventLoopGroup eventLoopGroup(<number of threads you want>);
````
To use any of our APIs that perform IO you'll need at least one event-loop. An event-loop group is a collection of event-loops that
protocol implementations will load balance across. If you won't have very many connections (say, more than 100 or so), then you
most likely only want 1 thread. In this case, you want to pass a single instance of this to every client or server implementation of a protocol
you use in your application. In some advanced use cases, you may want to reserve a thread for different types of IO tasks. In that case, you can have an
instance of this class for each reservation.

````
     Aws::Crt::Io::TlsContextOptions tlsCtxOptions =
        Aws::Crt::Io::TlsContextOptions::InitClientWithMtls(certificatePath.c_str(), keyPath.c_str());
    /*
     * If we have a custom CA, set that up here.
     */
    if (!caFile.empty())
    {
        tlsCtxOptions.OverrideDefaultTrustStore(nullptr, caFile.c_str());
    }

    uint16_t port = 8883;
    if (Io::TlsContextOptions::IsAlpnSupported())
    {
        /*
        * Use ALPN to negotiate the mqtt protocol on a normal
        * TLS port if possible.
        */
        tlsCtxOptions.SetAlpnList("x-amzn-mqtt-ca");
        port = 443;
    }

    Aws::Crt::Io::TlsContext tlsCtx(tlsCtxOptions, Io::TlsMode::CLIENT);
````

If you plan on using TLS, you will need a TlsContext. These are NOT CHEAP, so use as few as possible to perform your task.
If you're in client mode and not doing anything fancy (e.g. mutual TLS), then you can likely get away with using a single
instance for the entire application.

````
Aws::Crt::Io::ClientBootstrap bootstrap(eventLoopGroup);
````

Lastly, you will need a client or server bootstrap to use a client or server protocol implementation. Since everything is
non-blocking and event driven, this handles most of the "callback hell" inherent in the design. Assuming you aren't partitioning
threads for particular use-cases, you can have a single instance of this that you pass to multiple clients.

## Mac-Only TLS Behavior

Please note that on Mac, once a private key is used with a certificate, that certificate-key pair is imported into the Mac Keychain.  All subsequent uses of that certificate will use the stored private key and ignore anything passed in programmatically.  Beginning in v0.8.10, when a stored private key from the Keychain is used, the following will be logged at the "info" log level:

```
static: certificate has an existing certificate-key pair that was previously imported into the Keychain.  Using key from Keychain instead of the one provided.
```

## License

This library is licensed under the Apache 2.0 License.
