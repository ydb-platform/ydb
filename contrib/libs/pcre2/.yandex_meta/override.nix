pkgs: attrs: with pkgs; rec {
  version = "10.42";

  src = fetchFromGitHub {
    owner = "PCRE2Project";
    repo = "pcre2";
    rev = "pcre2-${version}";
    hash = "sha256-6/dbo20+JeXxJb26UKIhFI56TuPpa2TC7p4D27aKU7Q=";
  };

  patches = [];
  postPatch = "";

  postConfigure = ''
    cp src/pcre2_chartables.c.dist src/pcre2_chartables.c
  '';

  nativeBuildInputs = [ autoreconfHook ];
}
