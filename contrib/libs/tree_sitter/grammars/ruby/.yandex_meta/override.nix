self: super: with self; rec {
  pname = "tree-sitter-ruby";
  version = "0.23.1";

  src = fetchFromGitHub {
    owner = "tree-sitter";
    repo = pname;
    rev = "v${version}";
    hash = "sha256-iu3MVJl0Qr/Ba+aOttmEzMiVY6EouGi5wGOx5ofROzA=";
  };
}
