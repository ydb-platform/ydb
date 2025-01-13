pkgs: attrs: with pkgs; rec {
  version = "3.11.5";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-p7xRrdYZoWXVsuSF45GvB3pw5Ndxpyq/Hi2Uka3mUdQ=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
