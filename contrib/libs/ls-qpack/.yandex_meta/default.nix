pkgs: attrs: with pkgs; {
  ls-qpack = stdenv.mkDerivation rec {
    pname = "ls-qpack";
    version = "2.6.2";

    src = fetchurl {
      url = "https://github.com/litespeedtech/ls-qpack/archive/refs/tags/v${version}.tar.gz";
      hash = "sha256-2xqDECu30tlGqRDLEanWnBxbn9mkAknCLEEfucQp/hY=";
    };

    nativeBuildInputs = [
      cmake
    ];

    buildInputs = [
        xxHash
    ];

    cmakeFlags = [
      "-DLSQPACK_BIN=OFF"
      "-DLSQPACK_XXH=OFF"
    ];
  };
}
