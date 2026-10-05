self: super: with self; rec {
  version = "3.6.0";

  src = fetchFromGitHub {
    owner = "libffi";
    repo = "libffi";
    rev = "v${version}";
    hash = "sha256-bu7O+O/8POQaFpBc+fSJ/RXqjmTsE5weucF7TY9PO9w=";
  };

  nativeBuildInputs = [ autoreconfHook ];
}
