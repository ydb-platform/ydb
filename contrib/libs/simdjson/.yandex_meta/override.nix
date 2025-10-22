pkgs: attrs: with pkgs; rec {
  version = "4.0.1";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-CiNkaFBKPU+9SSs1tq3683SC1iBJvEiBh/83W/B928Y=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
