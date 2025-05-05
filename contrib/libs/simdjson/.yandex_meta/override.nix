pkgs: attrs: with pkgs; rec {
  version = "3.12.3";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-/FfaM5BTWKrt2m70+VcUXz//RiZuzxnUOaHOjPJWsGw=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
