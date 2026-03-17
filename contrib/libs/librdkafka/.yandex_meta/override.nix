pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.6.1";

  src = fetchFromGitHub {
    owner = "confluentinc";
    repo = "librdkafka";
    rev = "v${version}";
    hash = "sha256-qgy5VVB7H0FECtQR6HkTJ58vrHIU9TAFurDNuZGGgvw=";
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
