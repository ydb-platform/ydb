self: super: with self; rec {
  pname = "tree-sitter-scala";
  version = "0.21.0";

  src = fetchFromGitHub {
    owner = "tree-sitter";
    repo = pname;
    rev = "v${version}";
    hash = "sha256-ovq84DCjMqEIdZTLkJh02TG8jgXAOZZJWa2wIGrbUcg=";
  };
}
