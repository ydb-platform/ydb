self: super: with self; rec {
  pname = "tree-sitter-c";
  version = "0.21.0";

  src = fetchFromGitHub {
    owner = "tree-sitter";
    repo = "${pname}";
    rev = "v${version}";
    hash = "sha256-/3q25Fqo928yXEU6/8FF/PhYivFGuMjTYfkRATY0ggc=";
  };
}
