pkgs: attrs: with pkgs; rec {
  version = "4.6.11";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-JjcvJldWQTTFq9PU1F0IafL7GVoMfVUDz6hVb5Po/A4=";
  };

  cmakeFlags = [
    "-DBUILD_SHARED_LIBS=OFF"
    "-DCMAKE_DISABLE_PRECOMPILE_HEADERS=ON"
    "-DSIMDJSON_ENABLE_THREADS=OFF"
    "-DSIMDJSON_DEVELOPER_MODE=OFF"
  ];
}
