self: super: with self; rec {
  version = "3.3";

  src = fetchFromGitHub {
    owner = "libffi";
    repo = "libffi";
    rev = "v${version}";
    hash = "sha256-1lqbL/C+WtnOa5DxgT81CGiywzPzBf4pBCWjdI+5oQA=";
  };

  nativeBuildInputs = [ autoreconfHook ];
}
