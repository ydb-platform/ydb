self: super: with self; rec {
  name = "fast_float";
  version = "8.0.1";

  src = fetchFromGitHub {
    owner = "fastfloat";
    repo = "fast_float";
    rev = "v${version}";
    hash = "sha256-Y13JdBk8pZyg748fEOj+O/6gMAaqNXIE2fLY5tsMGB0=";
  };
}
