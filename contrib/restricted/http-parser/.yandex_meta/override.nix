pkgs: attrs: with pkgs; with attrs; rec {
  version = "2.9.4";
  src = fetchFromGitHub {
    owner = "nodejs";
    repo = "http-parser";
    rev = "v${version}";
    sha256 = "1vda4dp75pjf5fcph73sy0ifm3xrssrmf927qd1x8g3q46z0cv6c";
  };
}
