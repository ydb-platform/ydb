# Browser acceptance resources and tests

This `PY3TEST` launches the real C++ Skipper and mock SDK endpoints. Production
has no Node process, npm package or frontend build. Test-only Node is the existing
repository resource at `build/platform/nodejs/24.22.1`.

This target is excluded from test recursion and runs only when named explicitly.
The independent offline `browser/provenance` target remains in acceptance test
recursion. From the checkout root, run browser acceptance manually with a prepared
bundle for the host platform (the example uses macOS arm64):

```sh
$HOME/repos/my-env/bin/yamake --build=release -DCFLAGS=-fno-omit-frame-pointer \
  -tt --test-size-timeout=medium=100 \
  --test-env=SKIPPER_PREPARED_BROWSER_BUNDLE=/private/tmp/skipper-browser-bundles/output/darwin-arm64 \
  ydb/apps/bridge_skipper/acceptance/browser
```

Required browser bundle, pinned by this task:

- `playwright-core` **1.64.1**, whose `browsers.json` must pin
  `chromium-headless-shell` revision **1248**;
- Chromium headless shell **r1248** for Linux x86_64 and macOS arm64;
- explicit TTF/OTF fonts, their licenses/provenance and fontconfig configuration;
- Linux shared-library dependency closure, including fontconfig and its
  dependencies, excluding host glibc/ELF loader. macOS uses system frameworks.

**Resource registration is intentionally deferred by the owner.** The prepared bundles remain local; no uploads are authorized for this task. `resources/registration.json` records
prepared archive names, sizes and SHA256, with bundle resource IDs awaiting
repository-owner approval. The test fails explicitly when the corresponding
resource is missing; it never installs npm packages, downloads a browser or
falls back to a locally installed browser. The local Codex runtime's
playwright-core 1.62.1 / Chromium r1234 is intentionally unsuitable. Missing
resources are a validation blocker, not a skipped passing browser suite.

## Prepare and approve

`resources/upstream.json` pins the exact official Playwright/browser archives
and repository fonts by SHA256, and `resources/linux-native-lock.json` pins all
82 Debian-bullseye dependency packages by version, URL and SHA256.
`fetch_inputs.py` downloads and verifies those preparation inputs without
installing packages or uploading anything. Tests never call it. The staging root
retains `playwright-core-1.64.1.tgz` and its unpacked `package/` directory for the
offline preparation step. Use a clean staging directory; leftover package files
or directories fail the exact archive-inventory check.

```text
python3 fetch_inputs.py --staging /private/tmp/skipper-browser-bundles --platform all
```

`prepare_bundle.py` is the offline verifier/packer. Its required
`--playwright-archive` is the original staged Playwright tarball and
`--browser-archive` is the original staged Chromium zip. Both archives must match
their SHA256 pins in `resources/upstream.json`. The complete unpacked package and
browser inventories, including directories, and every file must match the
corresponding archive. Archive paths must be canonical, and symlinks or other
nonregular entries are rejected in archives and staged trees. Playwright
name/version and browser revision labels alone are insufficient.

Every supplied font and its license must have a locked basename and SHA256 in
`resources/upstream.json`; unknown or duplicate names and nonregular files are
rejected. Preparation also checks versions and the native closure. The supplied
`PROVENANCE.json` and native license notices remain records for owner review;
claims in these records do not establish the archive or font pins. Final runtime
trust still comes from the owner-reviewed bundle-manifest pin in registration.
On Linux run it on Linux x86_64 with the native library closure staged flat in
one directory; it verifies all `ldd` dependencies recursively resolve to the
bundle or the explicitly allowed host glibc/loader.

```text
python3 prepare_bundle.py --platform linux-x86_64 \
  --playwright /staging/package \
  --playwright-archive /staging/playwright-core-1.64.1.tgz \
  --browser /staging/linux-x86_64 \
  --browser-archive /staging/chrome-headless-shell-linux64.zip \
  --native /staging/native-libs \
  --font /staging/fonts/DejaVuSans.ttf --font /staging/fonts/DejaVuSansMono.ttf \
  --font-license /staging/fonts/LICENSE_DEJAVU --native-licenses /staging/native-licenses \
  --provenance /staging/PROVENANCE-linux-x86_64.json --output /staging/output
```

For the exact prepared artifacts, use the staged inputs above and these argument
sets (the same two `--font` and `--font-license` paths from `fonts/` apply to both):

```text
python3 prepare_bundle.py --platform darwin-arm64 --playwright /private/tmp/skipper-browser-bundles/package --playwright-archive /private/tmp/skipper-browser-bundles/playwright-core-1.64.1.tgz --browser /private/tmp/skipper-browser-bundles/darwin-arm64 --browser-archive /private/tmp/skipper-browser-bundles/chrome-headless-shell-mac-arm64.zip --font /private/tmp/skipper-browser-bundles/fonts/DejaVuSans.ttf --font /private/tmp/skipper-browser-bundles/fonts/DejaVuSansMono.ttf --font-license /private/tmp/skipper-browser-bundles/fonts/LICENSE_DEJAVU --provenance /private/tmp/skipper-browser-bundles/PROVENANCE-darwin-arm64.json --output /private/tmp/skipper-browser-bundles/output
python3 prepare_bundle.py --platform linux-x86_64 --playwright /private/tmp/skipper-browser-bundles/package --playwright-archive /private/tmp/skipper-browser-bundles/playwright-core-1.64.1.tgz --browser /private/tmp/skipper-browser-bundles/linux-x86_64 --browser-archive /private/tmp/skipper-browser-bundles/chrome-headless-shell-linux64.zip --native /private/tmp/skipper-browser-bundles/native-libs --native-licenses /private/tmp/skipper-browser-bundles/native-licenses --readelf /path/to/pinned/llvm-readelf --font /private/tmp/skipper-browser-bundles/fonts/DejaVuSans.ttf --font /private/tmp/skipper-browser-bundles/fonts/DejaVuSansMono.ttf --font-license /private/tmp/skipper-browser-bundles/fonts/LICENSE_DEJAVU --provenance /private/tmp/skipper-browser-bundles/PROVENANCE-linux-x86_64.json --output /private/tmp/skipper-browser-bundles/output
```

`prepare_bundle.py` refuses to overwrite an existing extracted output directory;
use a fresh output root for reproduction. For macOS omit `--native` and provide
an unpacked browser directory containing
`chrome-headless-shell-mac-arm64/chrome-headless-shell`. Linux's directory must
contain `chrome-headless-shell-linux64/chrome-headless-shell`. Cross preparation
on macOS may use an explicit pinned `--readelf` for static ELF dependency closure
verification; that does not establish a successful Linux runtime launch. The
output includes a normalized archive, file manifest and archive SHA256. It verifies both Playwright version and matching browser
revision before producing artifacts, after checking the archive and font pins.
The bundle contains no Node binary;
`node_uri` in the registration manifest references the existing resource.

Obtain repository-owner approval on those concrete artifacts **before any
upload**. Then register the immutable platform resource IDs/checksums, add the
resource declarations in `resources/ya.make`, exposing the extracted bundle as
`SKIPPER_BROWSER_RESOURCE_GLOBAL`. For a pre-upload verification run only,
`SKIPPER_PREPARED_BROWSER_BUNDLE` may point to that prepared bundle. Both paths
first compare the exact embedded `bundle.json` bytes with the platform's trusted
`bundle_manifest_sha256` in `resources/registration.json`, then verify every
listed file. Extra/unlisted files or directories, symlinks, nonregular nodes and
traversing/noncanonical paths are rejected. Replacing a browser and rewriting
its own manifest does not satisfy the trusted pin. Node must always resolve
from the repository resource.

No bundles have been uploaded by this implementation. Bundle registration,
Linux runtime execution and registered-resource browser acceptance remain
unverified. The prepared Linux closure requires host glibc 2.31 or newer.

## Coverage

`test_browser.py` drives `browser.cjs` through the pinned Node resource. Tests
assert real-server Manual review/execute and exact mock mutation count, plus
Auto activation and Off acknowledgement followed by a Ready incident with zero
new mutations. Presentation cases use the real page with fixture state responses
for the 640/960/1280/1600 viewport and 2/3/4/9/20 pile matrix, exact columns,
vertical reachability, no document horizontal overflow, failure borders, themes,
long names and drawer focus restoration. Time cases use Europe/Belgrade,
America/New_York and UTC around the New York DST overlap, check expiry wording
and preserve canonical UTC attributes. Screenshots are diagnostic artifacts,
not pixel goldens. Fonts are injected from the pinned bundle for deterministic
font metrics and also configured through fontconfig on Linux.

Run this target only through the repository's authorized `yamake` wrapper and
build/test skill. Passing mock/browser checks does not qualify production Auto;
the [real-cluster release checklist](../../README.md#required-builds-and-release-checklist)
remains separate.


## Prepared artifacts awaiting owner approval

Prepared under `/private/tmp/skipper-browser-bundles/output` (binary archives are
outside Git):

| Platform | Archive | Bytes | SHA256 |
| --- | --- | ---: | --- |
| macOS arm64 | `skipper-browser-darwin-arm64-pw1.64.1-r1248.tar.gz` | 100904577 | `8b2d0732d75ce7aff9326370bbb4dcab55e7802f1088ef009738eaafdfcd1e68` |
| Linux x86_64 | `skipper-browser-linux-x86_64-pw1.64.1-r1248.tar.gz` | 147411201 | `c2e9eb654a355e99165590872ce96b1ef162635e65431047ad8e0844b7884286` |

Each archive has a matching `.manifest.json` report with file hashes and an
extracted platform directory for pre-upload verification. Runtime trust pins the
embedded `bundle.json`, not that external archive report. Its digest is recorded
per platform in `resources/registration.json`; the original prepared manifests
and archives remain unchanged by the provenance checks. Linux's 175 library
aliases passed static ELF `DT_NEEDED` closure verification, excluding host
glibc/loader. The browser itself requires at most GLIBC2.25; bundled libraries
require at most GLIBC2.30, with host glibc2.31 as the supported baseline. A
successful Linux runtime launch is still unverified. No uploads
have been performed.


The separate `provenance` PY3TEST target (`provenance/test_bundle_provenance.py`)
independently covers valid local/registered bundles,
self-consistent forged manifests, extra files, symlinks, nonregular nodes,
traversal, staged archive/directory mismatches, and pinned Playwright/font
substitutions with small synthetic fixtures. Rejections assert the specific
reason so an unrelated inventory failure cannot mask a missing validation check.
These checks run offline with synthetic resource paths, without a Node resource
or C++ executable dependency. They never launch a browser or download dependencies.
Their two-way sharding is separate from the real browser suite's 32-way sharding,
so fast provenance cases cannot regroup the longer browser scenarios.
