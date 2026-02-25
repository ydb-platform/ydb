pkgs: attrs: with pkgs; rec {
  version = "4.3.1";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-SSNbVBGul8NogDfFHR2gZ80d1CNRrBjZBC+7kxItnFo=";
  };

  cmakeFlags = [
    "-DBUILD_SHARED_LIBS=OFF"
    "-DCMAKE_DISABLE_PRECOMPILE_HEADERS=ON"
    "-DSIMDJSON_ENABLE_THREADS=OFF"
    "-DSIMDJSON_DEVELOPER_MODE=OFF"
  ];
}
