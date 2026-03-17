pkgs: attrs: with pkgs; rec {
  pname = "pixman";
  version = "0.46.4";

  src = fetchFromGitLab {
    domain = "gitlab.freedesktop.org";
    owner = "pixman";
    repo = "pixman";
    rev = "pixman-${version}";
    hash = "sha256-SiXzRtCuAkbg4LBFc3USTRwj9qsAtLyfzaDMed8h7Cc=";
  };

  nativeBuildInputs = [
    meson
    ninja
    pkg-config
  ];

  mesonFlags = [];
}
