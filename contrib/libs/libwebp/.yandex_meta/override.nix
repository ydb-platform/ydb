pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.5.0";

  src = fetchFromGitHub {
    owner = "webmproject";
    repo = "libwebp";
    rev = "v${version}";
    hash = "sha256-DMHP7DVWXrTsqU0m9tc783E6dNO0EQoSXZTn5kZOtTg=";
  };

  patches = [];
}
