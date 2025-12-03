self: super: with self; rec {
  version = "2.11.1";

  src = fetchFromGitHub {
    owner = "JuliaStrings";
    repo = "utf8proc";
    rev = "v${version}";
    hash = "sha256-fFeevzek6Oql+wMmkZXVzKlDh3wZ6AjGCKJFsXBaqzg=";
  };
}
