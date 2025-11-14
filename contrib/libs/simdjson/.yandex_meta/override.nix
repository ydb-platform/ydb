pkgs: attrs: with pkgs; rec {
  version = "4.2.2";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-ydMFiBfToJQn74rIHwR7cA/qILP7AoRh3pBvjBbpIIY=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
