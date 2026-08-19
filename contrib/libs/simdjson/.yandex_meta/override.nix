pkgs: attrs: with pkgs; rec {
  version = "4.6.6";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-hi4uXGpD/cJ2HJi9WyTP0ruqbi/JwpqjbFFWemdoiys=";
  };

  cmakeFlags = [
    "-DBUILD_SHARED_LIBS=OFF"
    "-DCMAKE_DISABLE_PRECOMPILE_HEADERS=ON"
    "-DSIMDJSON_ENABLE_THREADS=OFF"
    "-DSIMDJSON_DEVELOPER_MODE=OFF"
  ];
}
