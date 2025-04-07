pkgs: attrs: with pkgs; rec {
  version = "2025-02-25";
  revision = "a6f71cbc3a1e1b8b9df241e081fa0ffdcde96249";

  src = fetchFromGitHub {
    owner = "libcxxrt";
    repo = "libcxxrt";
    rev = "${revision}";
    hash = "sha256-+oTjU/DgOEIwJebSVkSEt22mJSdeONozB8FfzEiESHU=";
  };

  nativeBuildInputs = [ cmake ];
}
