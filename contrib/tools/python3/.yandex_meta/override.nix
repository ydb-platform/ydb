pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.12.7";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-do8N5njMyYapDRyjDWYsIifRlhJKVumSAE1HrWWHvdM=";
  };

  patches = [];
  postPatch = "";
}
