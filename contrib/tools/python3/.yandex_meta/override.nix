pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.13.11";

  src = fetchFromGitHub {
    owner = "python";
    repo = "cpython";
    rev = "v${version}";
    hash = "sha256-SS0ZDQSwIRJPulUad5uG5vWLg6eGZNAYlvNdYx6mQPA=";
  };

  patches = [];
  postPatch = "";
}
