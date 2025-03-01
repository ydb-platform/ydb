pkgs: attrs: with pkgs; rec {
  version = "2024-10-22";
  revision = "5bf955548df364bc6efe4add80947b8689c74e2a";

  src = fetchFromGitHub {
    owner = "libcxxrt";
    repo = "libcxxrt";
    rev = "${revision}";
    hash = "sha256-YxMYouW/swdDP/YtNGFlFpBFIc5Pl08mCIl07V3OsCE=";
  };

  nativeBuildInputs = [ cmake ];
}
