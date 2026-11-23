pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.15.1";

  src = fetchFromGitHub {
    owner = "confluentinc";
    repo = "librdkafka";
    rev = "v${version}";
    hash = "sha256-74D1sMSA6advmirxeJiYzoqI0jjBqvbDtUqW8B8PieU=";
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
