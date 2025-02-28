pkgs: attrs: with pkgs; with attrs; rec {
  version = "5.6.4";

  src = fetchFromGitHub {
    owner = "tukaani-project";
    repo = "xz";
    rev = "v${version}";
    hash = "sha256-Xp1uLtQIoOG/qVLpq5D/KFmTOJ0+mNkNclyuJsvlUbE=";
  };

  nativeBuildInputs = [ autoreconfHook ];
}
