pkgs: attrs: with pkgs;

let
  ngtcp2 = pkgs.ngtcp2.overrideAttrs (finalAttrs: previousAttrs: rec {
    version = "1.8.1";
    src = fetchurl {
      url = "https://github.com/ngtcp2/ngtcp2/releases/download/v${version}/ngtcp2-${version}.tar.xz";
      hash = "sha256-rIRKees/FT5Mzc/szt9CxXqzUruKuS7IrF00F6ec+xE=";
    };
  });
in rec {
  version = "8.5.0";
  versionWithUnderscores = "${lib.replaceStrings ["."] ["_"] version}";

  src = fetchurl {
    url = "https://github.com/curl/curl/releases/download/curl-${versionWithUnderscores}/curl-${version}.tar.bz2";
    hash = "sha256-zktqZlVDEUdiSq9YJjKjb+Gt4mLV+rOFxg94lC3Y2Hs=";
  };

  patches = [];

  buildInputs = [
    c-ares
    zlib
    zstd
    quictls
    nghttp3
    ngtcp2
    libssh2
  ];

  configureFlags = [
    "--build=x86_64-pc-linux-gnu"
    "--disable-manual"
    "--disable-ldap"
    "--disable-ldaps"
    "--enable-ares"
    "--with-openssl"
    "--with-ca-fallback"
    "--with-zstd=${zstd.dev}"
    "--with-brotli=${brotli.dev}"
    "--with-nghttp3"
    "--with-ngtcp2"
    "--with-libssh2=${libssh2.dev}"
    "--without-gnutls"
    "--without-libidn2"
    "--without-libpsl"
    "--without-librtmp"
    "--without-wolfssl"
  ];

  # WARN:
  # _GNU_SOURCE is required in order to detect strerror_r flavor properly
  # Removing this setting will remain curl in compilable yet non-functional state.
  NIX_CFLAGS_COMPILE = [ "-D_GNU_SOURCE" ];
}
