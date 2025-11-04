pkgs: attrs: with pkgs; rec {
  version = "4.2.1";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-EuineRyNSwWoMRuev6ZFQNeFgSHjuoHhoGhYD0ls6GQ=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
