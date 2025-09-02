self: super: with self; rec {
  version = "3.5.2";

  src = fetchFromGitHub {
    owner = "libffi";
    repo = "libffi";
    rev = "v${version}";
    hash = "sha256-tvNdhpUnOvWoC5bpezUJv+EScnowhURI7XEtYF/EnQw=";
  };

  nativeBuildInputs = [ autoreconfHook ];
}
