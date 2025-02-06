pkgs: attrs: with pkgs; rec {
  version = "3.12.0";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-F5yqhDBDoWgB4YkFOYUFEczdu24aBdbsTly4LcFZqDQ=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
