self: super: with self; rec {
  pname = "tree-sitter-typescript";
  version = "0.20.6";

  src = fetchFromGitHub {
    owner = "tree-sitter";
    repo = "${pname}";
    rev = "v${version}";
    hash = "sha256-uGuwE1eTVEkuosMfTeY2akHB+bJ5npWEwUv+23nhY9M=";
  };
}
