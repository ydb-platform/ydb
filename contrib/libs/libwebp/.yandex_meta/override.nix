pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.2.2";

  src = fetchFromGitHub {
    owner = "webmproject";
    repo = "libwebp";
    rev = "v${version}";
    hash = "sha256-WF2HZPS7mbotk+d1oLM/JC5l/FWfkrk+T3Z6EW9oYEI=";
  };

  patches = [];
}
