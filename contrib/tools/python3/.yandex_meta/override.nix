pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.12.10";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-cz3pDerJJRmaUKvYDrW7SuPiYEEUqXP3FSRVDOcAxlY=";
  };

  patches = [];
  postPatch = "";
}
