self: super: with self; rec {
  pname = "tree-sitter-kotlin";
  version = "0.3.6";

  src = fetchFromGitHub {
    owner = "fwcd";
    repo = pname;
    rev = "${version}";
    hash = "sha256-Jma3nMadaP8kA/71WT4qu8r2UU0MJAdhMMV3dM6THFM=";
  };
}
