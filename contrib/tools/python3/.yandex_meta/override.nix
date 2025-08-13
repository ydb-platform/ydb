pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.12.11";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-vDczdMOTglDf5F+8PPkixvxScDCpedJCo0eL0VJJ/8g=";
  };

  patches = [];
  postPatch = "";
}
