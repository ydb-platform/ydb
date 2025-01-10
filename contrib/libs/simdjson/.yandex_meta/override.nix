pkgs: attrs: with pkgs; rec {
  version = "3.11.4";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-mcsMp9P9+3ACHkykJitHADoZ35kBeUza2LN+EPnq8RU=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
