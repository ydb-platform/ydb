pkgs: attrs: with pkgs; rec {
  version = "1.22.1";

  src = fetchurl {
    url = "https://github.com/ngtcp2/ngtcp2/releases/download/v${version}/ngtcp2-${version}.tar.xz";
    hash = "sha256-39LGi9ZLiYR8YRQluUhxBcRuhEe1wh5q6wBkLI++LKg=";
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
