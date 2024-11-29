pkgs: attrs: with pkgs; rec {
    name = "ngtcp2";
    version = "1.8.1";

    nativeBuildInputs = [
      autoreconfHook cmake pkg-config autoconf libtool automake openssl
    ];

    # without this nix tries to fetch and build quictls
    buildInputs = [ ];

    src = fetchurl {
      url = "https://github.com/ngtcp2/ngtcp2/releases/download/v${version}/ngtcp2-${version}.tar.xz";
      hash = "sha256-rIRKees/FT5Mzc/szt9CxXqzUruKuS7IrF00F6ec+xE=";
    };

    patches = [
        ./001_crypto_includes.patch
    ];
}
