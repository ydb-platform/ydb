pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.12.12";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-7FUs8+cCpEtC39qIWpVvEkEl2cqNrxQlov4IwY4acT8=";
  };

  patches = [];
  postPatch = "";
}
