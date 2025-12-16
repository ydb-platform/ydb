pkgs: attrs: with pkgs; rec {
  version = "4.2.3";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-JnyF2PMbzxwGhjnvkgITuPPJZvnMVPMPkbczmjiBb+0=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}
