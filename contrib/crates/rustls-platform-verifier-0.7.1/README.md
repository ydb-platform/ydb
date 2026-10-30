# rustls-platform-verifier

[![crates.io version](https://img.shields.io/crates/v/rustls-platform-verifier.svg)](https://crates.io/crates/rustls-platform-verifier)
[![crate documentation](https://docs.rs/rustls-platform-verifier/badge.svg)](https://docs.rs/rustls-platform-verifier)
![MSRV](https://img.shields.io/badge/rustc-1.85+-blue.svg)
[![crates.io downloads](https://img.shields.io/crates/d/rustls-platform-verifier.svg)](https://crates.io/crates/rustls-platform-verifier)
![CI](https://github.com/1Password/rustls-platform-verifier/workflows/CI/badge.svg)

A Rust library to verify the validity of TLS certificates based on the operating system's certificate facilities.
On operating systems that don't have these, `webpki` and/or `rustls-native-certs` is used instead.

This crate is advantageous over `rustls-native-certs` on its own for a few reasons:
- Improved correctness and security, as the OSes [CA constraints](https://support.apple.com/en-us/HT212865) will be taken into account.
- Better integration with OS certificate stores and enterprise CA deployments.
- Revocation support via verifying validity via OCSP and CRLs.
- Less I/O and memory overhead because all the platform CAs don't need to be loaded and parsed.

This library supports the following platforms and flows:

| OS             | Certificate Store                             | Verification Method                  | Revocation Support |
|----------------|-----------------------------------------------|--------------------------------------|--------------------|
| Windows        | Windows platform certificate store            | Windows API certificate verification | Yes                |
| macOS (10.14+) | macOS platform roots and keychain certificate | macOS `Security.framework`           | Yes                |
| iOS            | iOS platform roots and keychain certificates  | iOS `Security.framework`             | Yes                |
| Android        | Android System Trust Store                    | Android Trust Manager                | Sometimes[^1]      |
| Linux          | System CA bundle, or user-provided certs[^3]  | webpki                               | No[^2]             |
| WASM           | webpki roots                                  | webpki                               | No[^2]             |

[^1]: On Android, revocation checking requires API version >= 24 (e.g. at least Android 7.0, August 2016).
When available, revocation checking is only performed for the end-entity certificate. If a stapled OCSP
response for the end-entity cert isn't provided, and the certificate omits both a OCSP responder URL and
CRL distribution point to fetch revocation information from, revocation checking may fail.

[^2]: The fall-back webpki verifier configured for Linux/WASM does not support providing CRLs for revocation
checking. If you require revocation checking on these platforms, prefer constructing your own
`WebPkiServerVerifier`, providing necessary CRLs. See the Rustls [`ServerCertVerifierBuilder`] docs for more
information.

[^3]: On Linux the [rustls-native-certs] and [openssl-probe] crates are used to try and discover the system CA bundle.
Users may wish to augment these certificates with [webpki-roots] using [`Verifier::new_with_extra_roots`] in case
a system CA bundle is unavailable.

[`ServerCertVerifierBuilder`]: https://docs.rs/rustls/latest/rustls/client/struct.ServerCertVerifierBuilder.html
[`Verifier::new_with_extra_roots`]: https://docs.rs/rustls-platform-verifier/latest/rustls_platform_verifier/struct.Verifier.html#method.new_with_extra_roots
[rustls-native-certs]: https://github.com/rustls/rustls-native-certs
[openssl-probe]: https://github.com/alexcrichton/openssl-probe
[webpki-roots]: https://github.com/rustls/webpki-roots

## Deployment Considerations

When choosing to use `rustls-platform-verifier` or another trust store option, these differences are important to consider. They
are primarily about root certificate availability:

| Backend                                         | Updates                         | Roots used                                                                                            | Supports system-local roots  |
|-------------------------------------------------|---------------------------------|-------------------------------------------------------------------------------------------------------|------------------------------|
| `rustls-platform-verifier` (non-Linux/BSD)      | Updated by OS                   | System store, with full (dis)trust decisions from every source available.                             | Yes                          |
| `rustls-native-certs` + `webpki`                | Updated by OS                   | System store, with no (dis)trust decisions. All roots are treated equally regardless of their status. | Yes, with exceptions         |
| `webpki-roots` + `webpki`                       | Static, manual updates required | Hardcoded Mozilla CA roots, limited support for constrained roots.                                    | No                           |

**In general**: It is the opinion of the `rustls` and `rustls-platform-verifier` teams that this is the best default available for client-side libraries and applications
making connections to TLS servers when running on common operating systems. This is because it gets both live trust information (new roots, explicit markers, and auto-managed CRLs)
and better matches the common expectation of apps running on that platform (to use proxies, for example). Otherwise, it becomes your maintenance burden to
ship updates right away in order to handle increasing numbers of positive and negative trust events in the WebPKI/certificate ecosystem, or risk availability and security concerns.

#### Linux/BSD
As of the time of writing, `rustls-platform-verifier` on these OSes only loads the trust stores from the OS once upon startup. This is the same behavior as `rustls-native-certs`, but the
abstraction allows better behavior on the other platforms without extra work for downstreams.

#### Other

Alternatively, there is a clear answer to use static `webpki-roots` in your application instead if you are deploying containerized applications frequently, where root store changes
will make it to production faster and any possibly used trust root is static by definition.

Even though platform verifiers are sometimes implemented in memory-unsafe languages, it is very unlikely that Rust apps using this library will become a point of weakness.
This is due to either using a smaller set of servers or just being less exposed then other critical functions of the operating system, default web browser, etc.
But if your activity is identical or close to one of the following examples that process large amounts of untrusted input, a 100% Rust option like `webpki` is a more secure option:
- Seeing how many TLS servers `rustls` with a specific configuration can connect to.
- Harvesting data from various untrusted TLS endpoints exposed on the internet.
- Extracting info from a known-evil endpoint.
- Scanning all TLS certificates on the open internet.

`rustls-platform-verifier` is widely deployed by several applications that use the `rustls` stack, such as 1Password, Bitwarden, Signal, and `rustup`, on a wide set of OSes.
This means that it has received lots of exposure to edge cases and has real-world experience/expertise invested into it to ensure optimal compatibility and security.

## Installation and setup
On most platforms, no setup should be required beyond adding the dependency via `cargo`:
```toml
rustls-platform-verifier = "0.5"
```

To get a rustls `ClientConfig` configured to use the platform verifier use:

```rust
use rustls::ClientConfig;
use rustls_platform_verifier::ConfigVerifierExt;
let config = ClientConfig::with_platform_verifier();
```

This crate will use the [rustls process-default crypto provider](https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html#using-the-per-process-default-cryptoprovider). To construct a `ClientConfig` with a different `CryptoProvider`, use:

```rust
use rustls::ClientConfig;
use rustls_platform_verifier::BuilderVerifierExt;
let arc_crypto_provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
let config = ClientConfig::builder_with_provider(arc_crypto_provider)
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_platform_verifier()
    .unwrap()
    .with_no_client_auth();
```

### Android
Some manual setup is required, outside of `cargo`, to use this crate on Android. In order to
use Android's certificate verifier, the crate needs to call into the JVM. A small Kotlin
component must be included in your app's build to support `rustls-platform-verifier`.

#### Gradle Setup

`rustls-platform-verifier` distributes the required native components in a Maven-compatible format via GitHub, but the project must be setup to locate them
automatically and correctly. These steps assume you are using `.gradle` Groovy files because they're the most common, but if you are using
Kotlin scripts (`.gradle.kts`) for configuration instead, an example snippet is included towards the end of this section.

Each snippet includes a [`ValueSource`](https://docs.gradle.org/current/javadoc/org/gradle/api/provider/ValueSource.html) implementation that obtains a
Cargo-synchronized dependency version performantly, and is also friendly to Gradle's configuration cache. The version can be be selected manually instead, 
but runtime crashes may occur if a SemVer incompatible version is used.
 
Inside of your project's `build.gradle` file, add the following code and Maven repository definition:

`$PATH_TO_LOCK_FILE` is the relative path to the Cargo lockfile of your crate or workspace (`Cargo.lock`).

```groovy

repositories {
    maven {
        url = "https://github.com/rustls/rustls-platform-verifier/raw/maven-archive/android-release-support/maven/"
    }
}

abstract class RustlsVersion implements ValueSource<String, RustlsVersion.Params> {
    interface Params extends ValueSourceParameters {
        RegularFileProperty getLockFile()
    }

    static final String CRATE_NAME = "rustls-platform-verifier-android"

    @Override
    String obtain() {
        def lockFile = parameters.lockFile.get().asFile
        def lines = lockFile.readLines()
        def idx = lines.findIndexOf { it.trim() == "name = \"$CRATE_NAME\"" }
        def version = idx < 0 ? null : lines.drop(idx + 1)
            .find { it.stripLeading().startsWith("version = ") }
            ?.find(/"([^"]*)"/) { match, v -> v }
        if (!version) throw new GradleException("$CRATE_NAME not found in $lockFile")
        return version
    }
}

def rustlsPlatformVerifierVersion = providers.of(RustlsVersion) { spec ->
    spec.parameters.lockFile.set(layout.projectDirectory.file($PATH_TO_LOCK_FILE))
}

configurations.configureEach { configuration ->
    configuration.resolutionStrategy.eachDependency { details ->
        if (details.requested.group == "org.rustls" && details.requested.name == "rustls-platform-verifier") {
            details.useVersion(rustlsPlatformVerifierVersion.get())
            details.because("native component version must be identical to version of ${RustlsVersion.CRATE_NAME}")
        }
    }
}
```

Then, wherever you declare your dependencies, add the following:
```groovy
implementation "rustls:rustls-platform-verifier"
```

The dependency intentionally has no static version, it is only resolved dynamically at configuration time by the build script.

Cargo automatically handles finding the downloaded crate in the correct location for your project. It also handles updating the version when
new releases of `rustls-platform-verifier` are published. If you only use published releases, no extra maintenance should be required.

These script snippets can be tweaked as best suits your project, but the `cargo metadata` invocation must be included so that the Android
implementation part can be located on-disk.

##### Kotlin and Gradle

`build.gradle.kts`:
```kotlin

repositories {
    maven {
        url = uri("https://github.com/rustls/rustls-platform-verifier/raw/maven-archive/android-release-support/maven/")
    }
}

abstract class RustlsVersion : ValueSource<String, RustlsVersion.Params> {
    interface Params : ValueSourceParameters {
        val lockFile: RegularFileProperty
    }

    companion object {
        const val CRATE_NAME = "rustls-platform-verifier-android"
    }

    override fun obtain(): String {
        val version = parameters.lockFile.get().asFile.readLines().let { lines ->
            val nameIdx = lines.indexOfFirst { it.trim() == "name = \"$CRATE_NAME\"" }
            if (nameIdx < 0) {
                null
            } else {
                lines.drop(nameIdx + 1)
                    .firstOrNull { it.trimStart().startsWith("version = ") }
                    ?.substringAfter('"', "")
                    ?.substringBefore('"', "")
                    ?.takeIf { it.isNotEmpty() }
            }
        }
        return version?: error("$CRATE_NAME not found in Cargo.lock")
    }
}

val rustlsPlatformVerifierVersion = providers.of(RustlsVersion::class.java) {
    parameters.lockFile.set(layout.projectDirectory.file($PATH_TO_LOCK_FILE))
}

configurations.configureEach {
    resolutionStrategy.eachDependency {
        if (requested.group == "org.rustls" && requested.name == "rustls-platform-verifier") {
            useVersion(rustlsPlatformVerifierVersion.get())
            because("native component version must be identical to version of ${RustlsVersion.CRATE_NAME}")
        }
    }
}

dependencies {
    // `rustls-platform-verifier` is a Rust crate, but it also has a Kotlin component.
    implementation(libs.rustls.platform.verifier)
}
```

`libs.version.toml`:
```toml
# We keep the dependency unversioned because its version is selected dynamically during configuration.
rustls-platform-verifier = { group = "rustls", name = "rustls-platform-verifier" }
```

#### Proguard

If your Android application makes use of Proguard for optimizations, its important to make sure that the Android verifier component isn't optimized
out because it looks like dead code. Proguard is unable to see any JNI usage, so your rules must manually opt into keeping it. The following rule
can do this for you:
```text
-keep, includedescriptorclasses class org.rustls.platformverifier.** { *; }
```

#### Crate initialization

In order for the crate to call into the JVM, it needs handles from Android. These
are provided by one of the `init_with_env`, `init_with_refs` or `init_with_runtime` functions. These give `rustls-platform-verifier`
the resources it needs to make calls into the Android certificate verifier.

As an example, if your Rust Android component which the "native" Android
part of your app calls at startup has an initialization, like this:

```rust ,ignore
use jni::jni_mangle;
use jni::objects::{JClass, JObject};
use jni::EnvUnowned;

#[jni_mangle("com.orgname.application.Application")]
pub fn init<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    context: JObject<'caller>,
) {
    // ... initialize your app's other parts here.
}
```

In the simplest case, you should to insert a call to `rustls_platform_verifier::android::init_with_env()` here,
before any networking has a chance to run. This only needs to be called once and
the verifier will be valid for the lifetime of your app's process.

```rust ,ignore
use jni::errors::ThrowRuntimeExAndDefault;
use jni::jni_mangle;
use jni::objects::{JClass, JObject};
use jni::EnvUnowned;

#[jni_mangle("com.orgname.application.Application")]
pub fn init<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    context: JObject<'caller>,
) {
    // ... initialize your app's other parts here.

    // Then, initialize the certificate verifier for future use.
    unowned_env
        .with_env(|env| rustls_platform_verifier::android::init_with_env(env, context))
        .resolve::<ThrowRuntimeExAndDefault>();
}
```

In more advanced cases, such as where your code already stores long-lived handles into
the Android environment, you can alternatively use `init_with_runtime`. This function takes
a `&'static` reference to something that implements the `android::Runtime` trait, which the
crate then uses to obtain the access when required to the JVM.

## Credits
Made with ❤️ by the [1Password](https://1password.com/) and `rustls` teams. Portions of the Android and Windows verifier
implementations were adapted and referenced from Chromium's previous verifier implementations as well.

#### License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
</sub>
