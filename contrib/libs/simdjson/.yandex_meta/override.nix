pkgs: attrs: with pkgs; rec {
  version = "4.4.0";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-70R4PfgCCokAQeS4ygPyalp6IE/Zvqm2WPPXoNV5maU=";
  };

  cmakeFlags = [
    "-DBUILD_SHARED_LIBS=OFF"
    "-DCMAKE_DISABLE_PRECOMPILE_HEADERS=ON"
    "-DSIMDJSON_ENABLE_THREADS=OFF"
    "-DSIMDJSON_DEVELOPER_MODE=OFF"
  ];
}
