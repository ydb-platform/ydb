self: super: with self; rec {
  pname = "tree-sitter-rust";
  version = "0.20.4";

  src = fetchFromGitHub {
    owner = "tree-sitter";
    repo = "${pname}";
    rev = "v${version}";
    hash = "sha256-ywjox/LG+5R2F49DxnJ+x9ndIYxIdUgxmF0Y+MaqZN4=";
  };
}
