pkgs: attrs: with pkgs; rec {
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
  ];

  configureFlags = [
    "--disable-manual"
    "--disable-ldap"
    "--disable-ldaps"
    "--enable-ares"
    "--with-openssl=${openssl.dev}"
    "--with-ca-fallback"
    "--with-zstd=${zstd.dev}"
    "--with-brotli=${brotli.dev}"
    "--without-gnutls"
    "--without-libidn2"
    "--without-librtmp"
    "--without-nghttp3"
    "--without-ngtcp2"
    "--without-wolfssl"
  ];

  # WARN:
  # _GNU_SOURCE is required in order to detect strerror_r flavor properly
  # Removing this setting will remain curl in compilable yet non-functional state.
  NIX_CFLAGS_COMPILE = [ "-D_GNU_SOURCE" ];
}
