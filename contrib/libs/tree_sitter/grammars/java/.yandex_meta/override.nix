self: super: with self; rec {
  pname = "tree-sitter-java";
  version = "0.21.0";

  src = fetchFromGitHub {
    owner = "tree-sitter";
    repo = "${pname}";
    rev = "v${version}";
    hash = "sha256-COrEPsdTI6MJeb5iIZtyNHHe6nMsD/EnHDRVDTSKFTg=";
  };
}
