pkgs: attrs: with pkgs; rec {
  version = "3.13.0";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-Vzw1FpFjg3Tun1Sfk7H4h4tY7lfnjE1Wk+W82K7dcW0=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
