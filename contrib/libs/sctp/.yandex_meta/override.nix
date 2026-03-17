pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.0.21";

  src = fetchFromGitHub {
    owner = "sctp";
    repo = "lksctp-tools";
    rev = "lksctp-tools-${version}";
    sha256 = "sha256-+vbdNvHuJLYp901QgtBzMejlbzMyr9Z1eXxR3Zy7eAE=";
  };

  patches = [];

  nativeBuildInputs = [ autoreconfHook ];
}
