pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.15.0";

  src = fetchFromGitHub {
    owner = "confluentinc";
    repo = "librdkafka";
    rev = "v${version}";
    hash = "sha256-WW64fwh0xR4lEVwmrv00tP9mo6b49aCNgLLH/P0YS8k=";
  };

  patches = [];

  buildInputs = [
    cyrus_sasl
    lz4
    openssl
    perl
    python3
    zlib
    zstd
  ];

  configureFlags = [
    "--disable-c11threads"
    "--enable-gssapi"
    "--enable-lz4"
    "--enable-sasl"
    "--enable-ssl"
    "--enable-zlib"
    "--enable-zstd"
  ];

  makeFlags = "libs";
}
