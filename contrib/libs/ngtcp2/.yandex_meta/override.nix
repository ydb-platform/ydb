pkgs: attrs: with pkgs; rec {
  version = "1.18.0";

  src = fetchurl {
    url = "https://github.com/ngtcp2/ngtcp2/releases/download/v${version}/ngtcp2-${version}.tar.xz";
    hash = "sha256:aac91fbcb8af77216862cc1bf6e9ddcabfe42b4c373a438b7b1d36b763a4ac5f";
  };

  patches = [];

  buildInputs = [
    libev
    nghttp3
    quictls
  ];

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DENABLE_BORINGSSL=YES"
    "-DENABLE_STATIC_LIB=YES"
    "-DBORINGSSL_INCLUDE_DIR=${boringssl.dev}/include"
    "-DBORINGSSL_LIBRARIES=${boringssl}/lib/libssl.a;${boringssl}/lib/libcrypto.a"
  ];
}
