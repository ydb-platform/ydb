self: super: with self; rec {
  pname = "tree-sitter";
  version = "0.20.8";

  src = fetchFromGitHub {
    owner = pname;
    repo = pname;
    rev = "v${version}";
    hash = "sha256-278zU5CLNOwphGBUa4cGwjBqRJ87dhHMzFirZB09gYM=";
  };
}
