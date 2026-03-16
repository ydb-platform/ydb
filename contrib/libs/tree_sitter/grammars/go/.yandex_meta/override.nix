self: super: with self; rec {
  version = "0.21.0";

  src = fetchFromGitHub {
    owner = "tree-sitter";
    repo = "tree-sitter-go";
    rev = "v${version}";
    hash = "sha256-7dWVcetmJ//j0L56FDZylna3sY0qjUhilno69QFgrxc=";
  };
}
