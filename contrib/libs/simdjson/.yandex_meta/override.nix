pkgs: attrs: with pkgs; rec {
  version = "4.6.9";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-0Yowwkq8H+6wwqI+IZZ95r/YIAv/yphl7ctan0Nwt64=";
  };

  cmakeFlags = [
    "-DBUILD_SHARED_LIBS=OFF"
    "-DCMAKE_DISABLE_PRECOMPILE_HEADERS=ON"
    "-DSIMDJSON_ENABLE_THREADS=OFF"
    "-DSIMDJSON_DEVELOPER_MODE=OFF"
  ];
}
