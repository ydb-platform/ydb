pkgs: attrs: with pkgs; rec {
  version = "4.2.4";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-TTZcdnD7XT5n39n7rSlA81P3pid+5ek0noxjXAGbb64=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
